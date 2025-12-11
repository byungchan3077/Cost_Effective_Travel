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
        # Returns dict: { "CountryName": { "currency": "CODE", ... } }
        cost_dict = export_json.main()
        if not cost_dict:
            return [], "Error: Cost data is empty."
    except Exception as e:
        return [], f"Error: Cost data load failed: {e}"

    # 4. Calculate Scores
    print("  - 4. Calculating final scores...")
    final_results = []
    
    # Map: Currency Code -> Data
    ma_dict = ma_data_df.set_index('Currency Code').to_dict('index')
    
    for country_key, cost_data in cost_dict.items(): 
        # Extract currency code (e.g., "EUR", "JPY(100)")
        currency_code = cost_data.get('currency')
        
        if currency_code and currency_code in ma_dict:
            rate_data = ma_dict[currency_code]
            
            raw_curr = rate_data.get('Currency', 0)
            raw_ma = rate_data.get('50-day_MA', 0)

            # Fix: Normalize 100-unit currencies (e.g., JPY, IDR)
            if '(100)' in currency_code:
                current_rate = raw_curr / 100
                ma_rate = raw_ma / 100
            else:
                current_rate = raw_curr
                ma_rate = raw_ma
            
            # Fix: Convert Hotel(KRW) -> Local Currency for basket calc
            hotel_krw = cost_data.get('avg_hotel_krw', 0)
            hotel_local = hotel_krw / current_rate if current_rate > 0 else 0

            # Calc LSB (Local Survival Budget)
            lsb_cost = basket.calculate_lsb(
                meal_cost=cost_data.get('big_mac', 0),
                drink_cost=cost_data.get('starbucks', 0),
                accommodation_cost=hotel_local 
            )
            
            # Calc TEI (Purchasing Power)
            tei_result = calculator.calculate_tei(
                budget=total_budget,
                duration=days,
                local_daily_cost=lsb_cost,
                current_rate=current_rate,        
                ma_rate=ma_rate             
            )
            
            final_results.append({
                'country_code': country_key,
                'currency_code': currency_code,
                'ppi_score': tei_result.get('tei_score', 0.0),
                'trend_factor': tei_result.get('trend_impact', 0.0),
                'lsb_cost_local': round(lsb_cost, 2),
                'exchange_rate': current_rate
            })
        else:
            print(f"  [WARN] Skip {country_key}: No rate data for {currency_code}")

    # 5. Export Results
    print("  - 5. Exporting results...")
    if final_results:
        # Use export_data if available, else fallback to manual save
        if hasattr(export_json, 'export_data'):
            export_json.export_data(final_results)
        else:
            with open("result.json", "w", encoding="utf-8") as f:
                json.dump(final_results, f, indent=4, ensure_ascii=False)
            
        print("  Analysis complete.")
        return final_results, "Success"
    else:
        return [], "Error: No results generated."