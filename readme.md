NSE Medallion Intelligence 📈

A cloud-native, end-to-end ELT pipeline that ingests daily National Stock Exchange (NSE) Bhavcopy data into a MotherDuck data lake. It uses the Medallion Architecture to transform raw data into actionable "WeeklyLong" trading signals.

🚀 Key Features

100% Cloud-Native: Downloads data directly to memory and dumps it into MotherDuck—zero local disk footprint.

Medallion Architecture: Structured layers for data integrity (Bronze 🥉, Silver 🥈, Gold 🥇).

Automated Pipeline: Scheduled via GitHub Actions to run daily after market close.

Interactive Dashboard: Flask-based UI with a live SQL workspace and signal monitoring.

🛠️ Tech Stack

Database: MotherDuck (Cloud DuckDB)

Engine: DuckDB

Backend: Python 3.11+

Dashboard: Flask, Tailwind CSS, JavaScript

Automation: GitHub Actions

📂 Project Structure

.
├── pipeline_main.py          # Orchestrator (Main Entry Point)
├── Downloader.py             # NSE Fetcher (Direct-to-Cloud)
├── config.py                 # Environment & Path Configuration
├── .env                      # Secrets (Excluded from Git)
├── etl/                      
│   ├── bronze.py             # Raw Semi-structured Ingestion
│   ├── silver.py             # Data Cleaning & Casting (EQ Series)
│   └── gold.py               # Signal Generation (EMA 20 + Vol)
├── dashboard/                
│   ├── app.py                # Flask Server
│   └── templates/index.html  # UI Frontend
└── .github/workflows/        # Automation Logic


⚙️ Setup & Installation

Clone the repository:

git clone [https://github.com/vishalsetty11/NSE_Bhavcopy_Project.git](https://github.com/vishalsetty11/NSE_Bhavcopy_Project.git)
cd NSE_Bhavcopy_Project


Create a Virtual Environment:

python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate


Install Dependencies:

pip install -r requirements.txt


Configure Environment Variables:
Create a .env file in the root directory:

APP_ENV=PROD
MOTHERDUCK_TOKEN=your_motherduck_token_here


🏃 How to Run

1. Run the Pipeline

Fetches the latest Bhavcopy and updates the MotherDuck schemas:

python pipeline_main.py


2. Launch the Dashboard

View signals and run custom SQL queries locally:

python dashboard/app.py


Visit: http://127.0.0.1:5000

🤖 CI/CD (GitHub Actions)

The project includes a daily_pipeline.yml. To enable it:

Go to your GitHub Repo Settings > Secrets and variables > Actions.

Add a New Repository Secret: MOTHERDUCK_TOKEN.

The pipeline will now run automatically at 12:30 PM UTC daily.

Disclaimer: This project is for educational purposes only. Always perform your own research before making financial decisions.