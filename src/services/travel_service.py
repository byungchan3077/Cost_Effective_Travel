from typing import Dict, Any, List, Tuple
import pandas as pd
import json

# --- Internal Module Imports ---
from api import country_loader, api_loader, moveAvgDay
from data import export_json  # [Refactor] Replaced merge/preprocess
from logic import calculator, basket

def run_analysis_pipeline(total_budget: float, days: int) -> Tuple[List[Dict[str, Any]], str]:
    """
    Runs full pipeline: Fetch -> Merge -> Calculate -> Export.
    """
    print("\n[Service Log] Starting Full PPI Analysis Pipeline...")
    
    # 1. Get Target Currencies
    print("  - 1. Fetching target currency codes...")
    target_currencies = country_loader.get_target_currencies() 
    if not target_currencies:
        return [], "Error: No target currencies loaded."
        
    # 2. Fetch Exchange Rate & MA Data
    print("  - 2. Fetching MA data...")
    try:
        api_key, _, _ = api_loader.load_api_key()
        ma_data_df = moveAvgDay.get_50day_ma_data(api_key)
    except Exception as e:
        return [], f"Error: API/DB failed: {e}"
        
    if ma_data_df.empty:
        return [], "Error: No exchange rate data retrieved."
        
    # 3. Load Cost Data (from export_json)
    print("  - 3. Loading cost data...")
    try:
        # [Refactor] Load data via export_json.main()
        cost_dict = export_json.main()
        if not cost_dict:
            return [], "Error: Cost data is empty."
    except Exception as e:
        return [], f"Error: Cost data load failed: {e}"

    # 4. Calculate Scores (Placeholder)
    print("  - 4. Calculating final scores...")
    final_results = []
    
    return final_results, "Success"