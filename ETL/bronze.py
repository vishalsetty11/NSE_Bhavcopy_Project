import duckdb
from datetime import datetime

def load_bronze(db_path):
    """
    Bronze Stage: Parses semi-structured data from 'ingestedCSVData.raw_files' 
    into the relational 'bronze.bhavcopy_raw' table.
    """
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    
    # 1. Create the Bronze table (Raw landing with all types as VARCHAR)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bhavcopy_raw (
            SYMBOL          VARCHAR,
            SERIES          VARCHAR,
            DATE1           VARCHAR,
            PREV_CLOSE      VARCHAR,
            OPEN_PRICE      VARCHAR,
            HIGH_PRICE      VARCHAR,
            LOW_PRICE       VARCHAR,
            LAST_PRICE      VARCHAR,
            CLOSE_PRICE     VARCHAR,
            AVG_PRICE       VARCHAR,
            TTL_TRD_QNTY    VARCHAR,
            TURNOVER_LACS   VARCHAR,
            NO_OF_TRADES    VARCHAR,
            DELIV_QTY       VARCHAR,
            DELIV_PER       VARCHAR,
            _source_file    VARCHAR,
            _loaded_at      TIMESTAMP,
            _file_date      DATE
        );
    """)

    # 2. Identify files in Landing Schema
    all_landed = con.execute("SELECT filename FROM ingestedCSVData.raw_files").fetchall()
    all_landed_list = [f[0] for f in all_landed]

    # Identify new files not yet in Bronze
    pending = con.execute("""
        SELECT filename, content 
        FROM ingestedCSVData.raw_files 
        WHERE filename NOT IN (SELECT DISTINCT _source_file FROM bronze.bhavcopy_raw)
    """).fetchall()

    if not pending:
        # PRECISE LOGGING: Inform the user that the data is already safe in Bronze
        processed_count = con.execute("SELECT count(DISTINCT _source_file) FROM bronze.bhavcopy_raw").fetchone()[0]
        print(f"⏩ All {len(all_landed_list)} files in 'ingestedCSVData' are already present in Bronze.")
        print(f"📍 Current Bronze Catalog: {processed_count} files tracked.")
        con.close()
        return

    print(f"🥉 Parsing {len(pending)} semi-structured files to Bronze...")

    for filename, content in pending:
        try:
            # Extract date from filename: sec_bhavdata_full_DDMMYYYY.csv
            date_part = filename.split('_')[-1].replace('.csv', '')
            file_date = datetime.strptime(date_part, '%d%m%Y').date()
            
            # 3. Parse the raw string 'content' using DuckDB's native read_csv
            con.execute(f"""
                INSERT INTO bronze.bhavcopy_raw
                SELECT 
                    *, 
                    '{filename}' as _source_file, 
                    now() as _loaded_at,
                    '{file_date}' as _file_date
                FROM read_csv(?, header=True, all_varchar=True)
            """, [content])
            
            print(f"✅ Parsed to Bronze: {filename}")
        except Exception as e:
            print(f"❌ Error parsing {filename}: {e}")

    con.close()