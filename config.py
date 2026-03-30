import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

# Configuration Logic
ENV = os.getenv('APP_ENV', 'DEV')
MD_TOKEN = os.getenv('MOTHERDUCK_TOKEN')

if ENV == 'PROD' and MD_TOKEN:
    # Production: Cloud MotherDuck connection
    # Set token via env var (recommended by MotherDuck)
    os.environ['motherduck_token'] = MD_TOKEN
    DB_PATH = "md:nse_market"
else:
    # Development: Local DuckDB file path
    DB_PATH = r"D:\Setty\Market Project\Bhavcopy\nse_market.duckdb"