import duckdb
from config import MD_TOKEN

if not MD_TOKEN:
    print("❌ Error: MOTHERDUCK_TOKEN not found in .env")
    exit()

LOCAL_DB = r"D:\Setty\Market Project\Bhavcopy\nse_market.duckdb"
# FIX: Connect to MotherDuck root first (remove 'nse_market' from string)
CLOUD_CON_STR = f"md:?token={MD_TOKEN}"

print(f"🔄 Connecting to MotherDuck...")
con = duckdb.connect(CLOUD_CON_STR)

# FIX: Create the database in the cloud explicitly
print("🛠️ Creating cloud database 'nse_market'...")
con.execute("CREATE DATABASE IF NOT EXISTS nse_market;")

# Use the cloud database context
con.execute("USE nse_market;")

print(f"🔄 Attaching local database...")
con.execute(f"ATTACH '{LOCAL_DB}' AS local_db;")

for schema in ['bronze', 'silver', 'gold']:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    tables = con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'").fetchall()
    for (t_name,) in tables:
        print(f"📦 Syncing {schema}.{t_name}...")
        con.execute(f"CREATE OR REPLACE TABLE {schema}.{t_name} AS SELECT * FROM local_db.{schema}.{t_name};")

print("✅ Data Migration Complete!")
con.close()