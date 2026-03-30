import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import datetime
import time

def fetch_chartink_wl(dashboard_id="171603"):
    """
    Fetches 'WL Volume % gain' data from Chartink.
    Ensures 0.1 precision for all numerical values.
    """
    client = requests.Session()
    dash_url = f"https://chartink.com/dashboard/{dashboard_id}"
    
    # PRECISE FIX: Mimic real browser headers exactly
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://chartink.com",
        "Referer": dash_url,
        "X-Requested-With": "XMLHttpRequest"
    }

    try:
        # 1. Initialize session and extract CSRF Token
        print(f"📡 Accessing Dashboard: {dashboard_id}...")
        res = client.get(dash_url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        csrf_meta = soup.select_one('meta[name="csrf-token"]')
        if not csrf_meta:
            print("❌ CSRF token missing. Page load failed.")
            return None
        
        csrf = csrf_meta['content']
        headers["X-CSRF-TOKEN"] = csrf
        
        # 2. Try Primary Endpoint: dashboard-analytics
        api_url = "https://chartink.com/dashboard-analytics"
        payload = {"dashboard_id": dashboard_id}
        
        print("🥈 Fetching widget data...")
        time.sleep(1) # Human-like delay to prevent blocking
        resp = client.post(api_url, data=payload, headers=headers, timeout=15)
        
        # 3. Fallback to process-dashboard if 404 occurs
        if resp.status_code == 404:
            print("⚠️ Primary route failed, trying fallback...")
            api_url = "https://chartink.com/process-dashboard"
            resp = client.post(api_url, data=payload, headers=headers, timeout=15)

        if resp.status_code == 200:
            data = resp.json()
            widgets = data.get('data', [])
            
            for widget in widgets:
                if "WL Volume % gain" in widget.get('name', ''):
                    # Extract raw values into a DataFrame
                    df = pd.DataFrame(widget.get('values', []))
                    
                    if df.empty:
                        print("⚠️ Widget found but contains no data.")
                        return None
                    
                    # PRECISE ROUNDING: 0.1 decimal precision as per project standards
                    num_cols = df.select_dtypes('number').columns
                    df[num_cols] = df[num_cols].astype(float).round(1)
                    
                    return df
            print("❌ 'WL Volume % gain' widget not found in dashboard.")
        else:
            print(f"❌ Error {resp.status_code}: {resp.text[:100]}")
            
    except Exception as e:
        print(f"❌ Scraper Error: {e}")
        
    return None

if __name__ == "__main__":
    # PRECISE CONFIGURATION: Unique daily filename and specific path
    DOWNLOAD_DIR = r"D:\Setty\Market Project\ChartLink"
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        
    # Generate unique filename based on today's date
    today_str = datetime.date.today().strftime('%d%m%Y')
    FILE_NAME = f"wl_volume_gain_{today_str}.csv"
    full_path = os.path.join(DOWNLOAD_DIR, FILE_NAME)

    df = fetch_chartink_wl()
    
    if df is not None:
        # Save as unique CSV with 0.1 precision
        df.to_csv(full_path, index=False)
        print(f"✅ Successfully saved: {full_path}")
        print(f"📊 Rows captured: {len(df)}")