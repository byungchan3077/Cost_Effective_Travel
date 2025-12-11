from typing import Dict, Any, List, Tuple
import pandas as pd
import json

# --- Internal Module Imports ---
from api import country_loader, api_loader, moveAvgDay
from data import export_json
from logic import calculator, basket

def run_analysis_pipeline(total_budget: float, days: int) -> Tuple[List[Dict[str, Any]], str]:
    """
    Runs full pipeline: Fetch -> Merge -> Calculate -> Export.
    """
    print("\n[Service Log] Starting Full PPI Analysis Pipeline...")
    
    # 1. Get Target Currencies
    target_currencies = country_loader.get_target_currencies() 
    if not target_currencies: return [], "Error: No target currencies."
        
    # 2. Fetch Exchange Rate & MA Data
    try:
        api_key, _, _ = api_loader.load_api_key()
        ma_data_df = moveAvgDay.get_50day_ma_data(api_key)
    except Exception as e: return [], f"Error: {e}"
    if ma_data_df.empty: return [], "Error: No MA data."
        
    # 3. Load Cost Data
    try:
        cost_dict = export_json.main()
        if not cost_dict: return [], "Error: Cost data is empty."
    except Exception as e: return [], f"Error: {e}"

    # 4. Calculate Scores
    print("  - 4. Calculating final scores...")
    final_results = []
    
    # Map: Currency Code -> Data
    ma_dict = ma_data_df.set_index('Currency Code').to_dict('index')
    
    for country_key, cost_data in cost_dict.items(): 
        # [Logic] Match via 'currency' key
        currency_code = cost_data.get('currency')
        
        if currency_code and currency_code in ma_dict:
            rate_data = ma_dict[currency_code]
            
            # Basic calculation (Before unit fix)
            lsb_cost = basket.calculate_lsb(
                meal_cost=cost_data.get('big_mac', 0),
                drink_cost=cost_data.get('starbucks', 0),
                accommodation_cost=cost_data.get('avg_hotel_krw', 0) 
            )
            
            tei_result = calculator.calculate_tei(
                budget=total_budget,
                duration=days,
                local_daily_cost=lsb_cost,
                current_rate=rate_data.get('Currency', 0),        
                ma_rate=rate_data.get('50-day_MA', 0)             
            )
            
            final_results.append({
                'country_code': country_key,
                'currency_code': currency_code,
                'ppi_score': tei_result.get('tei_score', 0.0),
            })
        else:
            print(f"  [WARN] Skip {country_key}: No rate data for {currency_code}")

    # 5. Export Results
    if final_results:
        if hasattr(export_json, 'export_data'): export_json.export_data(final_results)
        return final_results, "Success"
    else:
        return [], "Error: No results generated."