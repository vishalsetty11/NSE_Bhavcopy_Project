import datetime
import os
from config import DB_PATH
from Downloader import download_to_cloud
from ETL.bronze import load_bronze
from ETL.silver import load_silver
from ETL.gold import load_gold

def get_previous_business_day(today):
    """Calculates the target date for the NSE Bhavcopy."""
    day_of_week = today.weekday()
    if day_of_week == 0:    # Monday -> Friday
        return today - datetime.timedelta(days=3)
    elif day_of_week == 6:  # Sunday -> Friday
        return today - datetime.timedelta(days=2)
    else:                   # Tuesday-Saturday -> Previous day
        return today - datetime.timedelta(days=1)

def run_nse_pipeline():
    """
    Orchestrates the 100% Cloud-Native Pipeline.
    Download (to memory) -> ingestedCSVData -> Bronze -> Silver -> Gold
    """
    target_date = get_previous_business_day(datetime.date.today())
    
    print(f"\n{'='*60}")
    print(f"🚀 STARTING CLOUD-NATIVE PIPELINE")
    print(f"📍 TARGET DB : {DB_PATH.split('?')[0]}") 
    print(f"📅 DATA DATE : {target_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}\n")

    # 1. DOWNLOAD STAGE: Direct to MotherDuck memory buffer
    # No local .csv files are created in this step.
    success_filename = download_to_cloud(target_date, DB_PATH)

    if success_filename:
        # 2. BRONZE STAGE: Parse CSV string from 'ingestedCSVData' table
        load_bronze(DB_PATH)

        # 3. SILVER STAGE: Cleaning & Filtering
        load_silver(DB_PATH)

        # 4. GOLD STAGE: Signal Generation
        load_gold(DB_PATH)
        
        print(f"\n✅ Pipeline Complete. Data live in MotherDuck.")
    else:
        print("\n⚠️ Pipeline Halted: File not available in NSE Archives yet.")

if __name__ == "__main__":
    run_nse_pipeline()