import pandas as pd
import json

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

def export_data():
    try:
        # ---- Load datasets ----
        hotel = pd.read_csv("src/data/hotel_price_index.csv")
        starbucks = pd.read_csv("src/data/starbucks_drink_index.csv")
        bigmac = pd.read_csv("src/data/big_mac_index.csv")   
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return
# ---- Issue 1 + Issue 2 code ----
hotel = pd.read_csv("hotel_price_index.csv")
starbucks = pd.read_csv("starbucks_drink_index.csv")
bigmac = pd.read_csv("big_mac_index.csv")

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

# ---- Issue 3: JSON export ----
result = {}

for _, row in merged.iterrows():
    country = row["Country"]
    result[country] = {
        "big_mac": row["bigmac_price"],
        "starbucks": row["starbucks_price"],
        "avg_hotel_krw": row["avg_hotel_krw"],
    }

# Print to console
print(json.dumps(result, indent=4, ensure_ascii=False))

# Optionally: save to file
with open("result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    export_data()