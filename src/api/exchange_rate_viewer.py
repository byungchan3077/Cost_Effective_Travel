import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime

# 1. Configuration and Constants Definition
load_dotenv()
API_KEY = os.getenv("EXIM_API_KEY")

BASE_URL = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON"
SERVICE_CODE = "AP01" 
TIMEOUT_SECONDS = 10 

if not API_KEY:
    print(" ERROR: EXIM_API_KEY is not configured in the .env file.")
    exit()

def fetch_and_display_currency_data(api_key, base_url, service_code, currency_code):
    """
    Calls the API to retrieve and display the latest exchange rate data for a specific currency code.
    """
    # 1. Set search date (Attempt to retrieve the latest data)
    # search_date = datetime.now().strftime("%Y%m%d") # Using current date
    search_date = "20251209" # Using a fixed date for testing
    
    params = {
        "authkey": api_key,
        "searchdate": search_date,
        "data": service_code
    }
    
    print(f"\n--- 🔍 Currency Data Lookup [{currency_code}] ({search_date}) ---")
    
    try:
        response = requests.get(base_url, params=params, timeout=TIMEOUT_SECONDS)
        response.raise_for_status() 

        raw_json = response.json()
        
        # Check for API error code
        if raw_json and raw_json[0].get('result') == 4:
            print(" API Error: No data for the requested date, or daily request limit reached.")
            return

        # 2. Data Filtering
        target_data = None
        
        for item in raw_json:
            # Search for the user's desired currency code based on the 'cur_unit' column
            if item.get('cur_unit') == currency_code:
                target_data = item
                break
        
        # 3. Output Results
        if target_data:
            print(f" Data Found! Full data for currency code [{currency_code}]:")
            
            # Print the JSON data cleanly
            print(json.dumps(target_data, indent=4, ensure_ascii=False))
            
            # Additional output for key exchange rate information
            print("\n--- Key Information ---")
            print(f"  > Currency Name (cur_nm): {target_data.get('cur_nm')}")
            print(f"  > Basic Dealing Rate (deal_bas_r): {target_data.get('deal_bas_r')} KRW")
        else:
            print(f" ERROR: Currency code [{currency_code}] not found in the API response.")
            print("   (Ensure the currency code is exact, e.g., 'JPY(100)'.)")
            
    except requests.exceptions.RequestException as e:
        print(f" API Request Failed: {e}.")


if __name__ == "__main__":
    
    print("--- EXIM Bank Exchange Rate Viewer ---")
    
    # 4. Receive user input
    print("\nReference Currency Codes:")
    print("USD (USA), JPY(100) (Japan), EUR (Euro), GBP (UK), SGD (Singapore)...")
    
    # User inputs the exact currency code (cur_unit) to look up
    user_input_code = input("Enter the exact currency code (cur_unit) to look up: ").upper()
    
    if user_input_code:
        fetch_and_display_currency_data(API_KEY, BASE_URL, SERVICE_CODE, user_input_code)
    else:
        print("No currency code was entered.")
        
        