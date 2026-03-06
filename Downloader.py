import requests
import duckdb

def download_to_cloud(target_date, db_path):
    """
    Fetches NSE CSV and lands it as semi-structured data in MotherDuck.
    Schema: ingestedCSVData | Table: raw_files
    """
    date_str = target_date.strftime('%d%m%Y')
    filename = f"sec_bhavdata_full_{date_str}.csv"
    url = f"https://nsearchives.nseindia.com/products/content/{filename}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }

    print(f"🌐 Fetching from NSE: {filename}")
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            csv_content = response.text
            
            con = duckdb.connect(db_path)
            # 1. Create dedicated schema for raw landings
            con.execute("CREATE SCHEMA IF NOT EXISTS ingestedCSVData;")
            
            # 2. Store as semi-structured data (Filename + Raw Content String)
            con.execute("""
                CREATE TABLE IF NOT EXISTS ingestedCSVData.raw_files (
                    filename VARCHAR PRIMARY KEY, 
                    content VARCHAR, 
                    loaded_at TIMESTAMP DEFAULT now()
                );
            """)
            
            con.execute("""
                INSERT INTO ingestedCSVData.raw_files (filename, content)
                VALUES (?, ?) 
                ON CONFLICT (filename) DO NOTHING
            """, [filename, csv_content])
            
            con.close()
            print(f"☁️ Semi-structured CSV landed in 'ingestedCSVData.raw_files': {filename}")
            return filename
        else:
            print(f"❌ NSE Error {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Download Failed: {e}")
        return None