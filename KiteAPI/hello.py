import logging
import os
import json
from tabulate import tabulate 
import datetime
from kiteconnect import KiteConnect

# --- CONFIGURATION ---
API_KEY = "19ekt4zstsb21aes"
API_SECRET = "svyokmbpmaran6shxgm49qt6ihtkzjav"
TOKEN_FILE = "access_token.txt"
LOGOUT_URL = "https://kite.zerodha.com/connect/login?api_key=19ekt4zstsb21aes" 

logging.basicConfig(level=logging.INFO)
kite = KiteConnect(api_key=API_KEY)

# --- TOKEN PERSISTENCE LOGIC ---
if os.path.exists(TOKEN_FILE) and os.path.getmtime(TOKEN_FILE) > (datetime.datetime.now() - datetime.timedelta(hours=23)).timestamp():
    # 1. Reuse existing token if the file exists and is less than ~24 hours old
    with open(TOKEN_FILE, 'r') as f:
        access_token = f.read().strip()
    
    kite.set_access_token(access_token)
    logging.info("Reusing saved Access Token.")
    
else:
    # 2. Generate a new session (Requires manual intervention once daily)
    logging.warning("Access Token missing or expired. Manual login required.")
    
    #Step 1: Print the URL and manually log in to get the REQUEST_TOKEN
    login_url = kite.login_url()
    print(f"Please log in manually: {login_url}")
    
    #Step 2: You must manually enter the request token from the redirect URL here
    REQUEST_TOKEN = input("Enter the request_token from the URL here: ").strip()
    
    try:
        # Step 3: Exchange request token for access token
        data = kite.generate_session(REQUEST_TOKEN, api_secret=API_SECRET)
        access_token = data["access_token"]
        kite.set_access_token(access_token)
        
        # Step 4: Save the new token
        with open(TOKEN_FILE, 'w') as f:
            f.write(access_token)
            
        logging.info("New Access Token generated and saved.")
        
    except Exception as e:
        logging.error(f"Failed to generate session: {e}")

# --- READY TO USE KITE ---
print("Kite Session is Active!")
print("\n--- Interactive Menu ---")
print("1: Get Filtered Instruments")
print("2: Get All Order History")
print("3: Get Holdings (Delivery)") # New
print("4: Get Active Positions (Intraday/F&O)") # New
print("q: Quit \n")

def getPrintOrders():
    print("--- Fetching Orders... ---")
    try:
        orders = kite.orders()
        print(f"Total orders found: {len(orders)}")
        if orders:
            # Print details of the most recent order
            print("Most Recent Order:")
            print(f"  ID: {orders[0].get('order_id')}, Status: {orders[0].get('status')}, Symbol: {orders[0].get('tradingsymbol')}")
            # print(orders) # Uncomment to print all order details
    except Exception as e:
        logging.error(f"Error getting orders: {e}")

def getInstruments():
    try:
        # Get instruments
        instruments = kite.instruments()

        # 1. Define the list of instruments you want (e.g., by trading symbol)
        targetSymbols=["NIFTY 50","RELIANCE"]

        # Using a list comprehension for brevity and efficiency
        filteredInstruments = [
            inst for inst in instruments
            if inst["tradingsymbol"] in targetSymbols
        ]
        print(f"Found {len(filteredInstruments)} instruments:")
        for inst in filteredInstruments:
                print(f"Symbol: {inst.get('tradingsymbol')}, Exchange: {inst.get('exchange')}, LTP: {inst.get('last_price')}, expiry: {inst.get('expiry')}, strike: {inst.get('strike')}, tick_size: {inst.get('tick_size')}, lot_size: {inst.get('lot_size')}")
    except Exception as e:
        logging.error(f"Error getting instruments: {e}")

def getHoldings():
    """Fetches and prints user's stock holdings."""
    print("--- Fetching Holdings... ---")
    try:
        holdings = kite.holdings()
        print(f"Total holdings found: {len(holdings)}")
            
        if not holdings:
            print("No holdings found.")
            return

        # 1. Define the headers for the table
        headers = ["Symbol", "Quantity", "Avg. Price", "LTP", "P&L", "Exchange"]
            
        # 2. Extract specific data points into a list of lists (rows)
        table_data = []
        for h in holdings:
            symbol = h.get('tradingsymbol', 'N/A')
            quantity = h.get('quantity', 0)
            avg_price = h.get('average_price', 0.0)
            ltp = h.get('last_price', 0.0)
            pnl = h.get('pnl', 0.0)
            exchange = h.get('exchange', 'N/A')

            table_data.append([
                symbol,
                quantity,
                f"{avg_price:,.2f}", # Format price to 2 decimal places
                f"{ltp:,.2f}",       # Format price to 2 decimal places
                f"{pnl:,.2f}",       # Format P&L to 2 decimal places
                exchange
                ])

        # 3. Print the data using the tabulate library
        print(tabulate(table_data, headers=headers, tablefmt="fancy_grid", numalign="right"))
            
    except Exception as e:
        logging.error(f"Error getting holdings: {e}")

def getActivePositions():
    """Fetches and prints user's active intraday/F&O positions."""
    print("--- Fetching Active Positions... ---")
    try:
        positions_data = kite.positions()
        
        # Positions are categorized as 'net' and 'day'
        net_positions = positions_data.get('net', [])
        
        print(f"Total Active (Net) Positions found: {len(net_positions)}")
        
        if net_positions:
            print("First Active Position Details:")
            # Print with formatting
            print(json.dumps(net_positions[0], indent=4))

    except Exception as e:
        logging.error(f"Error getting active positions: {e}")
# -------------------------------------------------------------------
# --- MENU DISPATCHER ---
# -------------------------------------------------------------------

def run_selection(choice):
    """Executes the function based on the user's choice."""
    # Updated switcher to include new functions
    switcher = {
        "1": getInstruments,
        "2": getPrintOrders,
        "3": getHoldings,       # New
        "4": getActivePositions # New
    }
    
    func = switcher.get(choice, lambda: print("-> Invalid choice. Please enter a valid menu number."))
    func()

# -------------------------------------------------------------------
# --- MAIN EXECUTION LOOP ---
# -------------------------------------------------------------------

def main_menu():
    """Displays menu and handles user input."""
    while True:        
        user_choice = input("\nEnter your choice (1, 2, 3, 4, or q): ").strip().lower()
        
        if user_choice == 'q':
            print("👋 Exiting script.")
            break
        
        if user_choice in ("1", "2", "3", "4"):
            run_selection(user_choice)
        else:
            print("-> Invalid input. Please try again.")

if __name__ == "__main__":
    # NOTE: You must ensure your token generation/persistence logic runs successfully here
    main_menu()