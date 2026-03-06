import pandas as pd
from sqlalchemy import create_engine
import os
import datetime

# --- Pine Script Inputs (Constants for the analysis) ---
EMA_LENGTH = 20
VOLUME_AVG_LENGTH = 30
VOLUME_MULTIPLIER = 3.0
MARUBOZU_TOLERANCE = 0.08 # 8%

# --- Database/Dataframe Column Names (CRITICAL: Adjust these if necessary) ---
SYMBOL_COLUMN = 'SYMBOL'
CLOSE_COLUMN = 'PREV_CLOSE'
OPEN_COLUMN = 'OPEN_PRICE'
HIGH_COLUMN = 'HIGH_PRICE'
LOW_COLUMN = 'LOW_PRICE'
VOLUME_COLUMN = 'DELIV_QTY' 
DATE_COLUMN = 'DATE1' # Ensure this matches the column name in your SQLite table

def find_weekly_long_signals(db_path, table_name):
    """
    Reads all historical data from SQLite, calculates the Pine Script conditions, 
    and returns a DataFrame containing the latest buy signals.
    """
    if not os.path.exists(db_path):
        print(f"❌ Database file not found: {db_path}")
        return pd.DataFrame() # Return empty DataFrame on failure

    print("\n--- Starting Analysis Stage (Finding Buy Signals) ---")
    
    # 1. Connect to DB and Read All Data
    engine = create_engine(f"sqlite:///{db_path}")
    columns_to_select = [SYMBOL_COLUMN, DATE_COLUMN, OPEN_COLUMN, HIGH_COLUMN, LOW_COLUMN, CLOSE_COLUMN, VOLUME_COLUMN]
    
    try:
        query = f"SELECT {', '.join(columns_to_select)} FROM {table_name} WHERE {CLOSE_COLUMN} IS NOT NULL ORDER BY {DATE_COLUMN}"
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"❌ Error reading data from DB: {e}")
        return pd.DataFrame()

    # Data Type Preparation
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
    df[CLOSE_COLUMN] = pd.to_numeric(df[CLOSE_COLUMN], errors='coerce')
    df[VOLUME_COLUMN] = pd.to_numeric(df[VOLUME_COLUMN], errors='coerce')
    df = df.dropna(subset=[CLOSE_COLUMN, VOLUME_COLUMN]).copy()
    
    signals = []

    # 2. Group by Symbol (Stock) and Apply Indicator Logic
    grouped = df.groupby(SYMBOL_COLUMN)
    
    for symbol, group_df in grouped:
        group_df = group_df.sort_values(DATE_COLUMN).reset_index(drop=True)

        # --- Indicator Calculations ---
        ema20 = group_df[CLOSE_COLUMN].ewm(span=EMA_LENGTH, adjust=False).mean()
        avgVol = group_df[VOLUME_COLUMN].rolling(window=VOLUME_AVG_LENGTH).mean()

        # --- Condition Checks ---
        group_df['volCondition'] = group_df[VOLUME_COLUMN] > VOLUME_MULTIPLIER * avgVol
        group_df['priceCondition'] = group_df[CLOSE_COLUMN] > ema20
        
        group_df['isGreenCandle'] = group_df[CLOSE_COLUMN] > group_df[OPEN_COLUMN]
        upperLimit = group_df[CLOSE_COLUMN] * (1 + MARUBOZU_TOLERANCE)
        lowerLimit = group_df[OPEN_COLUMN] * (1 - MARUBOZU_TOLERANCE)
        
        group_df['isGreenMarubozu'] = group_df['isGreenCandle'] & \
                                     (group_df[HIGH_COLUMN] <= upperLimit) & \
                                     (group_df[LOW_COLUMN] >= lowerLimit)

        # 3. Entry Condition
        group_df['entryCondition'] = group_df['volCondition'] & \
                                     group_df['priceCondition'] & \
                                     group_df['isGreenMarubozu']

        # Filter the group to only include days with a Buy Signal
        buy_signals_df = group_df[group_df['entryCondition']].copy() 
        
        if not buy_signals_df.empty:
            # If signals exist, get the last one (the most recent signal)
            latest_signal = buy_signals_df.iloc[[-1]]
            signals.append(latest_signal[[SYMBOL_COLUMN, DATE_COLUMN, CLOSE_COLUMN, VOLUME_COLUMN]])

    # 4. Output Results
    if signals:
        final_df = pd.concat(signals).sort_values(DATE_COLUMN, ascending=False)
        print("\n✅ **IDENTIFIED BUY SIGNALS (WeeklyLong Logic):**")
        print("--------------------------------------------------")
        print(final_df.to_string(index=False))
        return final_df
    else:
        print("\n No Buy Signals found based on the defined criteria.")
        return pd.DataFrame()