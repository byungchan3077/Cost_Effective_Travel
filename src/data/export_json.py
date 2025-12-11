import pandas as pd
import json
import os

# Get the folder path where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# 1. Country Name Standardization
# (This unifies different names like "Britain" and "UK" into one standard name)
name_map = {
    "Britain": "UK",
    "United Kingdom": "UK",
    "United States": "USA",
    "Hongkong": "Hong Kong",
    "Hong Kong": "Hong Kong",
    "United Arab Emirates": "UAE"
}

# 2. Country Map (Renamed from currency_map)
# (Only countries listed here will be included in the final result)
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

# 3. Euro Zone Countries
# (We need this because the Big Mac index groups these under "Euro area")
euro_countries = ["France", "Italy", "Spain", "Austria", "Germany"]

def clean_number(value):
    """
    Removes commas (,) and spaces from a value to make it a real number.
    Example: "1,000" -> 1000.0
    """
    try:
        # Convert to string, remove comma and space, then convert to float
        value = str(value).replace(',', '').strip()
        return float(value)
    except:
        # If it fails (e.g., text is not a number), return 0
        return 0

def main():
    # Load the CSV files
    try:
        hotel = pd.read_csv(os.path.join(script_dir, "hotel_price_index.csv"))
        starbucks = pd.read_csv(os.path.join(script_dir, "starbucks_drink_index.csv"))
        bigmac = pd.read_csv(os.path.join(script_dir, "big_mac_index.csv"))
    except:
        # If files are missing, print an empty object and exit
        print("{}")
        return

    # --- Start Data Cleaning ---

    # 1. Clean Column Names
    # .strip() removes spaces from the beginning and end of column names
    hotel.columns = hotel.columns.str.strip()
    starbucks.columns = starbucks.columns.str.strip()
    bigmac.columns = bigmac.columns.str.strip()

    # Rename columns to be consistent
    hotel = hotel.rename(columns={"Avg_price": "avg_hotel_krw"})
    starbucks = starbucks.rename(columns={"Avg_price": "starbucks_price"})
    bigmac = bigmac.rename(columns={"local_price": "bigmac_price"})

    # 2. Convert Text to Numbers
    # Apply the 'clean_number' function to every row in the price columns
    hotel["avg_hotel_krw"] = hotel["avg_hotel_krw"].apply(clean_number)
    starbucks["starbucks_price"] = starbucks["starbucks_price"].apply(clean_number)
    bigmac["bigmac_price"] = bigmac["bigmac_price"].apply(clean_number)

    # 3. Clean Country Names
    for df in [hotel, starbucks, bigmac]:
        if "Country" in df.columns:
            # Remove spaces and standardize names (e.g., Britain -> UK)
            df["Country"] = df["Country"].astype(str).str.strip()
            df["Country"] = df["Country"].replace(name_map)

    # 4. Calculate Averages
    # Group by Country (handles multiple cities like Tokyo/Osaka -> Japan Avg)
    hotel = hotel.groupby("Country")["avg_hotel_krw"].mean().reset_index()
    starbucks = starbucks.groupby("Country")["starbucks_price"].mean().reset_index()
    bigmac = bigmac.groupby("Country")["bigmac_price"].mean().reset_index()

    # 5. Handle 'Euro area'
    # Copy 'Euro area' Big Mac price to individual countries like France/Italy
    euro_row = bigmac[bigmac["Country"] == "Euro area"]
    
    if not euro_row.empty:
        price = euro_row.iloc[0]["bigmac_price"]
        
        # Check each Euro country and add it if missing from Big Mac data
        new_data = []
        for country in euro_countries:
            if country not in bigmac["Country"].values:
                new_data.append({"Country": country, "bigmac_price": price})
        
        # Add the new rows to the existing Big Mac data
        if new_data:
            bigmac = pd.concat([bigmac, pd.DataFrame(new_data)], ignore_index=True)

    # 6. Merge Data
    # Combine all three datasets (keep only countries present in ALL three)
    merged = bigmac.merge(starbucks, on="Country", how="inner")
    merged = merged.merge(hotel, on="Country", how="inner")
    merged = merged.fillna(0) # Fill any missing values with 0

    # 7. Create Result Dictionary
    result = {}
    
    # Loop through each row and add to dictionary
    for i, row in merged.iterrows():
        country = row["Country"]
        
        # [UPDATED] Check against country_map instead of currency_map
        if country in country_map:
            code = country_map[country]
            
            result[country] = {
                "currency": code,
                "big_mac": round(row["bigmac_price"], 2),     # Round to 2 decimals
                "starbucks": round(row["starbucks_price"], 2),
                "avg_hotel_krw": int(row["avg_hotel_krw"])    # Hotel price as integer
            }

    # Print Result to Console (JSON format)
    print(json.dumps(result, indent=4, ensure_ascii=False))

    # Save to File
    output_path = os.path.join(script_dir, "result.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    main()