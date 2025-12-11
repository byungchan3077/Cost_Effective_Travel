import pandas as pd
import json
import os
import sys

# Get the absolute path of the directory containing the current script (export_json.py)
script_dir = os.path.dirname(os.path.abspath(__file__))

country_map = {
    "Japan": "JPY(100)",
    "United States": "USD",
    "Italy": "EUR",
    "Spain": "EUR",
    "Indonesia": "IDR(100)",
    "Britain": "GBP",
    "UK": "GBP",
    "France": "EUR",
    "Singapore": "SGD",
    "Thailand": "THB",
    "Hong Kong": "HKD",
    "Hongkong": "HKD",
    "United Arab Emirates": "AED",
}

def load_csv_data():
    """Loads CSV files using paths relative to the script's location."""
    
    # Use os.path.join with script_dir to create a safe, absolute path
    hotel_path = os.path.join(script_dir, "hotel_price_index.csv")
    starbucks_path = os.path.join(script_dir, "starbucks_drink_index.csv")
    bigmac_path = os.path.join(script_dir, "big_mac_index.csv")
    
    try:
        hotel = pd.read_csv(hotel_path)
        starbucks = pd.read_csv(starbucks_path)
        bigmac = pd.read_csv(bigmac_path) 
        return hotel, starbucks, bigmac
    except FileNotFoundError as e:
        print(f"❌ Error: Cannot find required CSV file. Please ensure files are in {script_dir}. Details: {e}")
        # Return None on failure
        return None, None, None

# Load the data here so the country_loader can access the merged data structure (if needed)
hotel, starbucks, bigmac = load_csv_data()

# Exit if loading failed to prevent further errors in the module
if hotel is None or starbucks is None or bigmac is None:
    # If this module is imported, we should exit gracefully or handle the error.
    # Since this module is imported by country_loader, we let the ImportError in country_loader handle the fallback.
    # For now, we will simply not execute the rest of the code if loaded data is None.
    pass
else:
    # --- Data Processing Logic (Runs if data loading was successful) ---
    
    hotel = hotel.rename(columns={
        "Country": "Country",
        "Avg_price": "avg_hotel_krw"
    })
    hotel = hotel[["Country", "avg_hotel_krw"]]

    starbucks = starbucks.rename(columns={
        "Country": "Country",
        "Avg_price": "starbucks_price"
    })
    starbucks = starbucks[["Country", "starbucks_price"]]

    bigmac = bigmac.rename(columns={
        "Country": "Country",
        "local_price": "bigmac_price"
    })
    bigmac = bigmac[["Country", "bigmac_price"]]

    hotel["Country"] = hotel["Country"].replace(country_map)
    starbucks["Country"] = starbucks["Country"].replace(country_map)
    bigmac["Country"] = bigmac["Country"].replace(country_map)

    merged = bigmac.merge(starbucks, on="Country", how="inner")
    merged = merged.merge(hotel, on="Country", how="inner")

    # ---- JSON export ----
    result = {}

    for _, row in merged.iterrows():
        country_code = row["Country"]
        result[country_code] = {
            "big_mac": row["bigmac_price"],
            "starbucks": row["starbucks_price"],
            "avg_hotel_krw": row["avg_hotel_krw"],
        }

    # Print to console (optional)
    # print(json.dumps(result, indent=4, ensure_ascii=False))

    # Optionally: save to file
    output_path = os.path.join(script_dir, "result.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)
        # print(f"✅ Data exported to {output_path}")


if __name__ == "__main__":
    # If run directly, run the main logic and exit.
    if hotel is not None:
        print("Data processing and export completed.")
    else:
        print("Data processing failed due to file not found error.")