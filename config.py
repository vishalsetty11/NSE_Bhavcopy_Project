import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

# Configuration Logic
ENV = os.getenv('APP_ENV', 'DEV')
MD_TOKEN = os.getenv('MOTHERDUCK_TOKEN')

if ENV == 'PROD' and MD_TOKEN:
    # Production: Cloud MotherDuck connection
    # Path format: md:database_name?token=your_token
    DB_PATH = f"md:nse_market?token={MD_TOKEN}"
else:
    # Development: Local DuckDB file path
    DB_PATH = r"D:\Setty\Market Project\Bhavcopy\nse_market.duckdb"