import pandas as pd

# 1. Load CSV files
hotel = pd.read_csv("hotel_price_index.csv")
starbucks = pd.read_csv("starbucks_drink_index.csv")
bigmac = pd.read_csv("big_mac_index.csv")

# 2. Rename & keep only needed columns
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

# 3. Country mapping (normalize country names → ISO codes)
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

# 4. Check results (for debugging / verification)
print("=== Hotel (preprocessed) ===")
print(hotel.head())
print()

print("=== Starbucks (preprocessed) ===")
print(starbucks.head())
print()

print("=== Big Mac (preprocessed) ===")
print(bigmac.head())