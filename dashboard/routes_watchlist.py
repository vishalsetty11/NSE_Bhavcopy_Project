import duckdb
from flask import Blueprint, request, jsonify
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import os

# Import your existing config to get the correct DB_PATH (either local or MotherDuck)
from config import DB_PATH 

watchlist_bp = Blueprint('watchlist', __name__)

def get_calendar_service():
    """Initializes and returns the Google Calendar API service."""
    # Ensure token.json is in your project root
    if not os.path.exists('token.json'):
        raise FileNotFoundError("token.json not found. Please authenticate first.")
        
    creds = Credentials.from_authorized_user_file('token.json', ['https://www.googleapis.com/auth/calendar'])
    return build('calendar', 'v3', credentials=creds)

@watchlist_bp.route('/api/watchlist', methods=['GET', 'POST', 'DELETE'])
def manage_watchlist():
    # Use context manager for DuckDB to ensure connection closes automatically
    try:
        with duckdb.connect(DB_PATH) as conn:
            
            if request.method == 'POST':
                data = request.json
                # 1. Save to MotherDuck/Local DB
                conn.execute("""
                    INSERT INTO watchlist.watchlisted_stocks (symbol, note, file_name, reminder_time)
                    VALUES (?, ?, ?, ?)
                """, (data['symbol'], data['note'], data['file_name'], data['reminder_time']))
                
                # 2. Add to Google Calendar
                try:
                    service = get_calendar_service()
                    start_time = datetime.fromisoformat(data['reminder_time'])
                    end_time = start_time + timedelta(hours=1)
                    
                    event = {
                        'summary': f"Watchlist Reminder: {data['symbol']}",
                        'description': data['note'],
                        'start': {'dateTime': start_time.isoformat(), 'timeZone': 'Asia/Kolkata'},
                        'end': {'dateTime': end_time.isoformat(), 'timeZone': 'Asia/Kolkata'},
                    }
                    service.events().insert(calendarId='primary', body=event).execute()
                except Exception as e:
                    print(f"Calendar Error: {e}")
                    return jsonify({"status": "error", "message": "Database saved, but Calendar event failed"}), 500

                return jsonify({"status": "success"})

            if request.method == 'GET':
                # Convert results to list of dicts for JSON serialization
                results = conn.execute("SELECT * FROM watchlist.watchlisted_stocks").fetchdf()
                return jsonify(results.to_dict(orient='records'))

            if request.method == 'DELETE':
                stock_id = request.args.get('id')
                if not stock_id:
                    return jsonify({"status": "error", "message": "ID is required"}), 400
                conn.execute("DELETE FROM watchlist.watchlisted_stocks WHERE id = ?", (stock_id,))
                return jsonify({"status": "deleted"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    return jsonify({"status": "method not allowed"}), 405