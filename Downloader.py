import datetime
import time
import json
import os

def download_to_cloud(db_path, days_back=365):
    """
    Fetches missing Bhavcopy files from NSE.
    Uses a Dictionary (persisted via JSON) to track per-file strikes.
    """
    import requests
    import duckdb
    
    # PRECISE CONFIG: Local JSON file to store the strike dictionary
    PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
    TRACKER_FILE = os.path.join(PROJECT_DIR, "strike_tracker.json")
    
    # 1. Load Strike Tracker (Dictionary) - AUTOMATIC CHECKER
    strike_tracker = {}
    if os.path.exists(TRACKER_FILE):
        try:
            with open(TRACKER_FILE, 'r') as f:
                strike_tracker = json.load(f)
        except Exception:
            strike_tracker = {}

    print(f"🔗 Establishing MotherDuck Connection...")
    con = duckdb.connect(db_path)
    
    con.execute("CREATE SCHEMA IF NOT EXISTS ingestedCSVData;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestedCSVData.raw_files (
            filename VARCHAR PRIMARY KEY, 
            content VARCHAR, 
            loaded_at TIMESTAMP DEFAULT now()
        );
    """)

    res_files = con.execute("SELECT filename FROM ingestedCSVData.raw_files").fetchall()
    existing_files = {row[0] for row in res_files}

    headers = {'User-Agent': 'Mozilla/5.0'}
    base_url = "https://nsearchives.nseindia.com/products/content/"
    
    new_downloads = 0
    consecutive_empty_days = 0 
    
    for i in range(days_back):
        if consecutive_empty_days >= 5: 
            break
            
        target_date = datetime.date.today() - datetime.timedelta(days=i)
        if target_date.weekday() >= 5: continue # Skip weekends
            
        filename = f"sec_bhavdata_full_{target_date.strftime('%d%m%Y')}.csv"
        
        # PRECISE SKIP: If the dictionary shows 3 or more strikes, skip this file
        if strike_tracker.get(filename, 0) >= 2:
            continue

        if filename in existing_files:
            consecutive_empty_days = 0
            continue

        try:
            print(f"🌐 Fetching: {filename}")
            response = requests.get(f"{base_url}{filename}", headers=headers, timeout=15)
            
            if response.status_code == 200:
                con.execute("INSERT INTO ingestedCSVData.raw_files (filename, content) VALUES (?, ?)", [filename, response.text])
                print(f"✅ Landed: {filename}")
                new_downloads += 1
                consecutive_empty_days = 0
                
                # PRECISE RESET: Remove from dictionary if successfully downloaded
                if filename in strike_tracker:
                    del strike_tracker[filename]
                time.sleep(1) 
            elif response.status_code == 404:
                print(f"⚠️ Not found: {filename}")
                consecutive_empty_days += 1
                
                # PRECISE UPDATE: Increment strikes in the dictionary
                strike_tracker[filename] = strike_tracker.get(filename, 0) + 1
            else:
                consecutive_empty_days += 1
                
        except Exception as e:
            print(f"❌ Error: {e}")
            consecutive_empty_days += 1

    # 3. Persist the Dictionary back to JSON for the next run
    with open(TRACKER_FILE, 'w') as f:
        json.dump(strike_tracker, f)

    con.close()
    return True