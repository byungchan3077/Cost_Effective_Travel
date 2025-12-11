import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime

# --- 1. Configuration and Constants Definition ---
load_dotenv()
API_KEY = os.getenv("EXIM_API_KEY")

# API Base URL and Service Code (Constants)
BASE_URL = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON"
SERVICE_CODE = "AP01" 
TIMEOUT_SECONDS = 10 

if not API_KEY:
    raise ValueError(" API Key is not configured in the .env file. Please check the EXIM_API_KEY variable.")

def load_api_key():
    """Returns the API Key, Base URL, and Service Code."""
    return API_KEY, BASE_URL, SERVICE_CODE

def print_data_format(api_key, base_url, service_code):
    """
    Retrieves and prints data from a recent business day to verify the API data format.
    """
    # Use a fixed recent business day (20251210) to check the data format.
    date = "20251210" 
    
    params = {
        "authkey": api_key,
        "searchdate": date,
        "data": service_code
    }
    
    print(f"\n--- Verifying API Data Format: [{date}] ---")
    
    try:
        response = requests.get(base_url, params=params, timeout=TIMEOUT_SECONDS)
        response.raise_for_status() 

        raw_json = response.json()
        
        if raw_json and raw_json[0].get('result') == 4:
            print(" API Error: Daily limit reached or data does not exist for the specified date.")
            return

        print(f" API Response Status: {response.status_code} (Success)")
        
        # Data format check (Output the first item)
        print("\n### Data Format (Mandatory Columns Check) ###")
        first_item = raw_json[0] if raw_json else {}
        print(json.dumps(first_item, indent=4, ensure_ascii=False))
        
    except requests.exceptions.RequestException as e:
        print(f" API Request Failed: {e}.")

if __name__ == "__main__":
    API_KEY, BASE_URL, SERVICE_CODE = load_api_key()
    print_data_format(API_KEY, BASE_URL, SERVICE_CODE)