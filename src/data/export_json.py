import pandas as pd
import json
import os

script_dir = os.path.dirname(os.path.abspath(__file__))

# 1. Name Standardization Map
# Unifies various country name formats from Excel into a single standard name.
name_standardize_map = {
    "Britain": "UK",
    "United Kingdom": "UK",
    "United States": "USA",
    "Hongkong": "Hong Kong", 
    "Hong Kong": "Hong Kong",
    "United Arab Emirates": "UAE"
}

# 2. Currency Mapping
# Maps standard country names to their currency codes.
currency_map = {
    "Japan": "JPY(100)",
    "USA": "USD",
    "Italy": "EUR",
    "Spain": "EUR",
    "France": "EUR",
    "Austria": "EUR",
    "Indonesia": "IDR(100)",
    "UK": "GBP",
    "Singapore": "SGD",
    "Thailand": "THB",
    "Hong Kong": "HKD",
    "UAE": "AED",
    "Vietnam": "VND(100)",
    "Taiwan": "TWD",
}

# 3. Euro Zone Countries List
# Used to copy 'Euro area' Big Mac data to specific countries.
euro_zone_countries = ["France", "Italy", "Spain", "Austria"]

def load_and_process_data():
    # Construct file paths
    hotel_path = os.path.join(script_dir, "hotel_price_index.csv")
    starbucks_path = os.path.join(script_dir, "starbucks_drink_index.csv")
    bigmac_path = os.path.join(script_dir, "big_mac_index.csv")
    
    try:
        # Load Data
        hotel = pd.read_csv(hotel_path)
        starbucks = pd.read_csv(starbucks_path)
        bigmac = pd.read_csv(bigmac_path) 
    except FileNotFoundError as e:
        print(f"❌ Error: File not found: {e}")
        return None

    # --- Step 1: Clean Whitespace and Rename Columns ---
    
    # Rename columns for consistency
    hotel = hotel.rename(columns={"Avg_price": "avg_hotel_krw"})
    starbucks = starbucks.rename(columns={"Avg_price": "starbucks_price"})
    bigmac = bigmac.rename(columns={"local_price": "bigmac_price"})

    # [CRITICAL FIX] Force convert price data to numeric
    # 'errors="coerce"' converts non-numeric strings to NaN (preventing the previous KeyError)
    hotel["avg_hotel_krw"] = pd.to_numeric(hotel["avg_hotel_krw"], errors='coerce')
    starbucks["starbucks_price"] = pd.to_numeric(starbucks["starbucks_price"], errors='coerce')
    bigmac["bigmac_price"] = pd.to_numeric(bigmac["bigmac_price"], errors='coerce')

    # Remove leading/trailing whitespace from Country names
    for df in [hotel, starbucks, bigmac]:
        if "Country" in df.columns:
            df["Country"] = df["Country"].astype(str).str.strip()

    # --- Step 2: Calculate Country Averages ---
    # Hotel and Starbucks data have multiple cities per country.
    # Group by Country and calculate the mean.
    hotel = hotel.groupby("Country")[["avg_hotel_krw"]].mean().reset_index()
    starbucks = starbucks.groupby("Country")[["starbucks_price"]].mean().reset_index()

    # --- Step 3: Expand Euro Zone Data ---
    # Find the 'Euro area' row in the Big Mac index
    euro_row = bigmac[bigmac["Country"] == "Euro area"]
    
    if not euro_row.empty:
        euro_price = euro_row.iloc[0]["bigmac_price"]
        
        # Create new rows for individual Euro zone countries
        new_rows = []
        for country in euro_zone_countries:
            new_rows.append({"Country": country, "bigmac_price": euro_price})
        
        # Append these new rows to the dataframe
        euro_df = pd.DataFrame(new_rows)
        bigmac = pd.concat([bigmac, euro_df], ignore_index=True)

    # --- Step 4: Standardize Country Names ---
    # Apply the mapping map to handle variations (e.g., Britain -> UK)
    hotel["Country"] = hotel["Country"].replace(name_standardize_map)
    starbucks["Country"] = starbucks["Country"].replace(name_standardize_map)
    bigmac["Country"] = bigmac["Country"].replace(name_standardize_map)

    # --- Step 5: Merge Data ---
    # Use inner join to keep only countries present in all three datasets
    merged = bigmac.merge(starbucks, on="Country", how="inner")
    merged = merged.merge(hotel, on="Country", how="inner")
    
    # Fill any remaining NaNs with 0
    merged = merged.fillna(0)

    # --- Step 6: Map Currency Codes and Finalize ---
    merged["currency_code"] = merged["Country"].map(currency_map)
    
    result = {}
    for _, row in merged.iterrows():
        country_name = row["Country"]
        result[country_name] = {
            "currency": row["currency_code"] if row["currency_code"] != 0 else "Unknown",
            "big_mac": round(row["bigmac_price"], 2),
            "starbucks": round(row["starbucks_price"], 2),
            "avg_hotel_krw": round(row["avg_hotel_krw"], 0), # Remove decimals for hotel prices
        }
    
    return result

if __name__ == "__main__":
    data = load_and_process_data()
    
    if data:
        # Console Output
        print("\n" + "="*50)
        print(f"📊 Processing Complete! Generated data for {len(data)} countries.")
        print("="*50)
        print(json.dumps(data, indent=4, ensure_ascii=False))
        
        # Save to file
        output_path = os.path.join(script_dir, "result.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print("="*50 + "\n")