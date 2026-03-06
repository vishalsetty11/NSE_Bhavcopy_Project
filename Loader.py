import pandas as pd
# 🌟 UPDATED IMPORT 🌟
from sqlalchemy import create_engine, text, inspect 
import os

# Assuming DATE_COLUMN is correctly set to 'DATE1' or 'DATE' based on your previous fixes
DATE_COLUMN = 'DATE1' # Ensure this matches your CSV header exactly

def load_csv_to_database(file_path, db_url, table_name):
    """
    Reads the NSE CSV file and loads it into the local SQLite database file,
    only if the data for that day has not already been loaded.
    """
    if not file_path or not os.path.exists(file_path):
        print(f"❌ Cannot load. File not found at path: {file_path}")
        return

    print(f"\nStarting database load for: **{os.path.basename(file_path)}**")
    
    try:
        # ... (Steps 1. READ CSV & Transformation are unchanged) ...
        df = pd.read_csv(file_path, low_memory=False)
        df.columns = df.columns.str.strip() 
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x) 
        if 'SERIES' in df.columns:
            df = df[df['SERIES'] == 'EQ']

        if DATE_COLUMN not in df.columns:
            print(f"❌ Error: Date column '{DATE_COLUMN}' not found in CSV. Please correct the variable DATE_COLUMN.")
            return

        df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors='coerce') 
        unique_dates = df[DATE_COLUMN].dt.date.unique()
        
        if len(unique_dates) != 1:
            print("Warning: CSV contains data for multiple dates. Using the first date for idempotency check.")
        
        load_date = unique_dates[0]
        load_date_str = load_date.strftime('%Y-%m-%d')
        
        # 2. CREATE DATABASE ENGINE
        engine = create_engine(db_url)

        # 🌟 CORRECTED IDEMPOTENCY CHECK 🌟
        
        # Use inspect to safely check if the table exists
        inspector = inspect(engine)
        table_exists = inspector.has_table(table_name)
        
        if table_exists:
            # Table exists, proceed with the data idempotency check
            with engine.connect() as connection:
                check_query = text(
                    f"SELECT COUNT(*) FROM {table_name} WHERE date({DATE_COLUMN}) = :date_to_check LIMIT 1"
                )
                # Use connection.execute() on the connection object
                result = connection.execute(check_query, {'date_to_check': load_date_str}).scalar()

            if result and result > 0:
                print(f"\n✅ **SKIPPING LOAD:** Data for **{load_date_str}** already exists in table **{table_name}**.")
                return 
        else:
            print(f"ℹ️ **INFO:** Table '{table_name}' does not exist. It will be created on the first load.")
        
        # 3. LOAD TO DATABASE 
        df.to_sql(
            table_name, 
            con=engine, 
            if_exists='append', 
            index=False,
        )

        print(f"\n✅ **SUCCESS:** {len(df)} rows loaded into table **{table_name}** for date {load_date_str}.")
        
    except Exception as e:
        print(f"\n❌ **ERROR** during SQLite load: {e}")