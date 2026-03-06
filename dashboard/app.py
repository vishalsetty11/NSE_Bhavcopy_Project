from flask import Flask, render_template, request, jsonify
import duckdb
import os
import sys

# --- PATH FIX: Allow importing from the parent directory ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import DB_PATH
except ImportError:
    # Manual fallback if config.py is missing
    DB_PATH = r"D:\Setty\Market Project\Bhavcopy\nse_market.duckdb"

app = Flask(__name__)

# PRECISE HIGHLIGHT: Visual confirmation of the connection source
is_cloud = DB_PATH.startswith("md:")
connection_label = "☁️ MOTHERDUCK (CLOUD)" if is_cloud else "📁 LOCAL DUCKDB"

print(f"\n{'='*60}")
print(f"BOOTING DASHBOARD")
print(f"MODE: {connection_label}")
print(f"PATH: {DB_PATH.split('?')[0]}") # Hide token in logs
print(f"{'='*60}\n")

def get_db_connection():
    """
    Connects to the database. 
    MotherDuck handles its own concurrency, so no read_only needed for 'md:' paths.
    """
    try:
        if is_cloud:
            # MotherDuck connection
            return duckdb.connect(DB_PATH)
        else:
            # Local file connection
            return duckdb.connect(DB_PATH, read_only=True)
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        raise

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    """Fetch record counts and data source info."""
    try:
        con = get_db_connection()
        
        stats = {
            "bronze": con.execute("SELECT count(*) FROM bronze.bhavcopy_raw").fetchone()[0],
            "silver": con.execute("SELECT count(*) FROM silver.bhavcopy_clean").fetchone()[0],
            "gold": con.execute("SELECT count(*) FROM gold.daily_signals").fetchone()[0],
            "last_update": con.execute("SELECT max(_loaded_at) FROM silver.bhavcopy_clean").fetchone()[0].strftime("%Y-%m-%d %H:%M"),
            "source": "MotherDuck" if is_cloud else "Local"
        }
        con.close()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/catalog')
def get_catalog():
    """Lists available tables in the Medallion schemas."""
    try:
        con = get_db_connection()
        tables = con.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema IN ('bronze', 'silver', 'gold')
        """).df().to_dict(orient='records')
        con.close()
        return jsonify(tables)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/query', methods=['POST'])
def run_query():
    """Executes custom SQL from the dashboard workspace."""
    data = request.get_json()
    sql = data.get('sql')
    try:
        con = get_db_connection()
        df = con.execute(sql).df()
        
        # Ensure dates are stringified for JSON serialization
        for col in df.select_dtypes(include=['datetime64', 'date']).columns:
            df[col] = df[col].astype(str)
            
        res = {
            "results": df.to_dict(orient='records'), 
            "columns": list(df.columns),
            "source": "Cloud" if is_cloud else "Local"
        }
        con.close()
        return jsonify(res)
    except Exception as e:
        print(f"❌ SQL Error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Using threaded=True to handle concurrent requests better in local dev
    app.run(debug=True, port=5000, threaded=True)