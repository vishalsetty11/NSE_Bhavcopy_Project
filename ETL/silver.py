import duckdb

def load_silver(db_path):
    """
    Silver Stage: Cleaning, Casting, and Filtering EQ series.
    Forces schema recreation to ensure Primary Key exists for MotherDuck.
    """
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    
    # PRECISE FIX: Drop the table once to apply the new PRIMARY KEY constraint
    # After this runs successfully once, you can comment out the DROP line.
    con.execute("DROP TABLE IF EXISTS silver.bhavcopy_clean;")

    con.execute("""
        CREATE TABLE silver.bhavcopy_clean (
            symbol          VARCHAR NOT NULL,
            trade_date      DATE NOT NULL,
            prev_close      DECIMAL(12,2),
            open_price      DECIMAL(12,2),
            high_price      DECIMAL(12,2),
            low_price       DECIMAL(12,2),
            last_price      DECIMAL(12,2),
            close_price     DECIMAL(12,2),
            avg_price       DECIMAL(12,2),
            total_traded_qty BIGINT,
            turnover_lacs   DECIMAL(15,2),
            num_trades      INTEGER,
            delivery_qty    BIGINT,
            delivery_pct    DECIMAL(6,2),
            _loaded_at      TIMESTAMP,
            PRIMARY KEY (symbol, trade_date)
        );
    """)

    print("🥈 Refining to Silver (Cleaning & Filtering)...")

    # Explicit conflict target for MotherDuck
    con.execute("""
        INSERT INTO silver.bhavcopy_clean
        SELECT 
            TRIM(SYMBOL) as symbol,
            CAST(_file_date AS DATE) as trade_date,
            CAST(NULLIF(TRIM(PREV_CLOSE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(OPEN_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(HIGH_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(LOW_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(LAST_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(CLOSE_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(AVG_PRICE), '') AS DECIMAL(12,2)),
            CAST(NULLIF(TRIM(TTL_TRD_QNTY), '') AS BIGINT),
            CAST(NULLIF(TRIM(TURNOVER_LACS), '') AS DECIMAL(15,2)),
            CAST(NULLIF(TRIM(NO_OF_TRADES), '') AS INTEGER),
            CAST(NULLIF(TRIM(DELIV_QTY), '') AS BIGINT),
            CAST(NULLIF(TRIM(DELIV_PER), '') AS DECIMAL(6,2)),
            now() as _loaded_at
        FROM bronze.bhavcopy_raw
        WHERE TRIM(SERIES) = 'EQ'
          AND _file_date IS NOT NULL
        ON CONFLICT (symbol, trade_date) DO NOTHING;
    """)

    count = con.execute("SELECT count(*) FROM silver.bhavcopy_clean").fetchone()[0]
    print(f"✅ Silver layer complete. Records: {count}")
    con.close()