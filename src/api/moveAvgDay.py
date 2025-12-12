import os
import sys
import requests
import pandas as pd
import time
import math
from datetime import datetime, timedelta

# **--- 1. Add Project Root Path (For Relative Import Resolution) ---**
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import necessary modules using relative paths
# NOTE: Assuming the function name in country_loader is get_target_currencies
from src.api.api_loader import load_api_key, SERVICE_CODE, BASE_URL
from src.api.country_loader import get_target_currencies 

# --- 1. Settings and Constants Definition ---
DAYS_TO_FETCH = 50
DB_DIR = os.path.join(os.path.dirname(__file__), 'database') 
DB_FILE_PREFIX = 'exchange_data_'
MIN_PERIODS = 5 

# --- 2. DB and Data Management Functions (Restored Previous Functions) ---

def setup_database(currency_code):
    """Creates the DB folder and returns the file path."""
    os.makedirs(DB_DIR, exist_ok=True)
    return os.path.join(DB_DIR, f"{DB_FILE_PREFIX}{currency_code}.csv")

def load_db_data(file_path):
    """Loads existing DB data. Returns an empty DataFrame if file is missing or on error."""
    if os.path.exists(file_path):
        try:
            # Column names are kept in Korean for consistency with data structure
            df = pd.read_csv(file_path, index_col=0, parse_dates=['Date'])
            df['Date'] = df['Date'].dt.strftime('%Y%m%d') 
            df = df.sort_values(by='Date', ascending=False)
            return df
        except Exception as e:
            print(f"⚠️ Error loading {os.path.basename(file_path)}: {e}. Creating new DataFrame.")
            return pd.DataFrame()
    return pd.DataFrame()

def save_db_data(df, file_path):
    """Saves the DataFrame to a CSV file."""
    if not df.empty:
        df = df.drop_duplicates(subset=['Date'], keep='first')
        df = df.sort_values(by='Date', ascending=True)
        # Convert 'Date' to string before saving (for consistency with load_db_data)
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y%m%d')
        df.to_csv(file_path, index=True, encoding='utf-8')
        print(f"✅ DB save complete: {os.path.basename(file_path)}, total {len(df)} days of data.")
    else:
        print(f"❌ No data to save, skipping file {os.path.basename(file_path)}.")

# --- 3. Optimized Data Collection Function (Restored Previous Function) ---
# NOTE: BASE_URL, SERVICE_CODE constants are imported from api_loader and available globally
def fetch_optimized_data(api_key, currency_code, existing_dates, days_needed):
    """Fetches only the required dates' data from the API that are missing from the existing DB."""
    new_data = []
    fetched_count = 0
    MAX_ITERATIONS = 100
    
    # BASE_URL and SERVICE_CODE are global constants imported from the parent module.

    print(f"🔍 [{currency_code}] Starting new data acquisition (Required business days: {days_needed})")

    for i in range(1, MAX_ITERATIONS + 1):
        search_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        
        if fetched_count >= days_needed: break
        if search_date in existing_dates: continue

        params = {"authkey": api_key, "searchdate": search_date, "data": SERVICE_CODE}
        
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status() 
            day_data = response.json()

            if day_data and day_data[0].get('result') == 4:
                print("❌ API rate limit reached or no data available.")
                break 

            if day_data and not day_data[0].get('result') == 4:
                for item in day_data:
                    cur_unit = item.get('cur_unit')
                    deal_bas_r_str = item.get('deal_bas_r')

                    if cur_unit == currency_code and deal_bas_r_str:
                        # Convert rate string (e.g., "1,363.50") to float
                        numeric_rate = float(deal_bas_r_str.replace(',', ''))
                        
                        # Use Korean column names for consistency with DB CSVs
                        new_data.append({'Date': search_date, 'Currency Code': cur_unit, 'Currency': numeric_rate})
                        fetched_count += 1
                        print(f"  > [{search_date}] New data collected. (Acquired: {fetched_count}/{days_needed} days)")
                        break 
        
        except requests.exceptions.RequestException as e:
            print(f"❌ [{search_date}] API request error occurred: {e}. Stopping iteration.")
            break 
            
        time.sleep(0.1) 

    return pd.DataFrame(new_data)


# --- 4. Main Analysis Function (For External Reference) ---

def get_50day_ma_data(api_key):
    """
    Collects and updates exchange rate data, calculates the Moving Average (MA), and returns the DataFrame.
    Does NOT include R-value calculation logic.
    """
    
    TARGET_CURRENCIES = get_target_currencies() # Load currency code list
    all_ma_results = []
    
    for currency_code in TARGET_CURRENCIES:
        file_path = setup_database(currency_code)
        existing_df = load_db_data(file_path) # Load previous data (string 'Date')
        
        existing_dates = set(existing_df['Date'].unique()) if not existing_df.empty else set()
        current_data_count = len(existing_df)
        needed_days = DAYS_TO_FETCH - current_data_count
        
        updated_df = existing_df.copy()

        # 1. Data Collection and Update (Optimization)
        if needed_days > 0:
            new_df = fetch_optimized_data(api_key, currency_code, existing_dates, needed_days)
            updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            print(f"✅ [{currency_code}] Sufficient data ({current_data_count} days) exists in DB. Skipping API call.")

        
        # 2. Moving Average Calculation
        if len(updated_df) >= MIN_PERIODS:
            
            # Convert 'Date' to datetime objects and sort ascending for MA calculation
            updated_df['Date'] = pd.to_datetime(updated_df['Date'])
            updated_df = updated_df.sort_values(by='Date', ascending=True).reset_index(drop=True)
            
            # Calculate 50-day MA (Note: DAYS_TO_FETCH is currently 5)
            updated_df['50-day_MA'] = updated_df['Currency'].rolling(window=DAYS_TO_FETCH, min_periods=MIN_PERIODS).mean()
            
            # 3. Database Save (Update for the next run)
            # (The save_db_data function converts the date back to string before saving)
            save_db_data(updated_df, file_path)

            # 4. Prepare data for return (Final data needed for R-value calculation)
            latest_ma_data = updated_df.iloc[-1]
            
            # Extract necessary columns and add to the list (Convert date to string)
            all_ma_results.append({
                'Currency Code': currency_code,
                'Date': latest_ma_data['Date'].strftime('%Y%m%d'),
                'Currency': latest_ma_data['Currency'],
                '50-day_MA': latest_ma_data['50-day_MA']
            })

        else:
            print(f"⚠️ [{currency_code}] Data is less than the minimum {MIN_PERIODS} days. Cannot calculate MA. ({len(updated_df)} days)")

    return pd.DataFrame(all_ma_results)

if __name__ == "__main__":
    # Assuming load_api_key returns the key, service code, and base URL
    API_KEY, _, _ = load_api_key() 
    
    print("Starting 50-day Moving Average data collection and update...")
    result_df = get_50day_ma_data(API_KEY)
    
    if not result_df.empty:
        print("\n[Final 50-day Moving Average Data (For R-Value Calculation)]")
        # Print necessary data without R-value calculation
        print(result_df[['Currency Code', 'Date', 'Currency', '50-day_MA']].to_markdown(index=False, floatfmt=".2f"))
    else:
        print("No data was collected.")