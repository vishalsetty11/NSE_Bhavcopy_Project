import datetime
import duckdb
import time

def download_to_cloud(db_path, days_back=365):
    # PRECISE FIX: Move import inside the function
    import requests
    
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS ingestedCSVData;")
    # ... (rest of the code remains exactly the same)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestedCSVData.raw_files (
            filename VARCHAR PRIMARY KEY, 
            content VARCHAR, 
            loaded_at TIMESTAMP DEFAULT now()
        );
    """)

    # Get list of already downloaded files to avoid duplicates
    existing_files = set()
    check_table = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'ingestedcsvdata' AND table_name = 'raw_files'").fetchone()[0]
    if check_table > 0:
        existing_files = {row[0] for row in con.execute("SELECT filename FROM ingestedCSVData.raw_files").fetchall()}

    print(f"🔍 Checking for missing data over the last {days_back} days...")
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    base_url = "https://nsearchives.nseindia.com/products/content/"
    
    for i in range(days_back):
        target_date = datetime.date.today() - datetime.timedelta(days=i)
        if target_date.weekday() >= 5: continue
            
        date_str = target_date.strftime('%d%m%Y')
        filename = f"sec_bhavdata_full_{date_str}.csv"
        
        if filename in existing_files: continue

        url = f"{base_url}{filename}"
        try:
            print(f"🌐 Fetching: {filename}")
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                con.execute("INSERT INTO ingestedCSVData.raw_files (filename, content) VALUES (?, ?) ON CONFLICT DO NOTHING", [filename, response.text])
                print(f"✅ Landed: {filename}")
                time.sleep(1) 
            elif response.status_code == 404:
                print(f"⚠️ Not found: {filename}")
        except Exception as e:
            print(f"❌ Error: {e}")

    con.close()
    return True