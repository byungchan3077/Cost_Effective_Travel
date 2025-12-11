import os
import sys


try:
    # Append project root path to allow importing modules from the 'data' directory.
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from data.export_json import country_map 
except ImportError:
    # Define a default country_map if the actual file cannot be imported.
    country_map = {
        "United States": "USD", 
        "Japan": "JPY(100)", 
        "European Union": "EUR", 
        "China": "CNY",
        "Australia": "AUD"
    }
    
    print(" Cannot find country_map... Using temporary currency codes instead.")

def get_target_currencies():
    """
    Returns a list of unique currency codes (values) from the country_map.
    Repetitions are removed.
    """

    target_currencies = list(set(country_map.values()))
    
    print(f" Country_loader: Total {len(target_currencies)} currency codes loaded.")
    
    return target_currencies

if __name__ == "__main__":
    get_target_currencies()