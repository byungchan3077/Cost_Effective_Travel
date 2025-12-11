import pandas as pd
import json

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

country_map = {
    "Japan": "JPN",
    "United States": "USA",
    "Italy": "ITA",
    "Spain": "ESP",
    "Indonesia": "IDN",
    "Britain": "GBR",
    "UK": "GBR",
    "France": "FRA",
    "Vietnam": "VNM",
    "Singapore": "SGP",
    "Thailand": "THA",
    "Hong Kong": "HKG",
    "Hongkong": "HKG",
    "United Arab Emirates": "UAE",
    "Taiwan": "TWN",
}

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