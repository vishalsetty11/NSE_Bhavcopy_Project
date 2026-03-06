import duckdb

def load_gold(db_path):
    """
    Gold Stage: Indicators and Signals.
    Forces schema recreation to ensure Primary Key exists for MotherDuck conflict resolution.
    """
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    
    # PRECISE FIX: Drop the table to apply the new PRIMARY KEY constraint
    # This is required because MotherDuck cannot 'target' conflict columns without a physical PK.
    con.execute("DROP TABLE IF EXISTS gold.daily_signals;")

    con.execute("""
        CREATE TABLE gold.daily_signals (
            symbol          VARCHAR NOT NULL,
            signal_date     DATE NOT NULL,
            close_price     DECIMAL(12,2),
            ema_20          DECIMAL(12,2),
            vol_multiplier  DECIMAL(6,2),
            is_marubozu     BOOLEAN,
            _computed_at    TIMESTAMP,
            PRIMARY KEY (symbol, signal_date)
        );
    """)

    print("🥇 Generating Gold Signals...")

    # Explicit conflict target for MotherDuck
    con.execute("""
        INSERT INTO gold.daily_signals
        WITH metrics AS (
            SELECT 
                symbol,
                trade_date,
                close_price,
                open_price,
                high_price,
                low_price,
                total_traded_qty,
                AVG(close_price) OVER(PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ema_20,
                AVG(total_traded_qty) OVER(PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING) as avg_vol_30
            FROM silver.bhavcopy_clean
        )
        SELECT 
            symbol,
            trade_date as signal_date,
            close_price,
            ema_20,
            CAST(total_traded_qty / NULLIF(avg_vol_30, 0) AS DECIMAL(6,2)) as vol_multiplier,
            (close_price > open_price AND (high_price - close_price) < (close_price - open_price) * 0.1) as is_marubozu,
            now() as _computed_at
        FROM metrics
        WHERE close_price > ema_20 
          AND total_traded_qty > (avg_vol_30 * 1.5)
          AND trade_date = (SELECT MAX(trade_date) FROM silver.bhavcopy_clean)
        ON CONFLICT (symbol, signal_date) DO NOTHING;
    """)

    res = con.execute("SELECT count(*) FROM gold.daily_signals WHERE signal_date = (SELECT MAX(signal_date) FROM gold.daily_signals)").fetchone()[0]
    print(f"✅ Gold layer complete. New signals found: {res}")
    con.close()