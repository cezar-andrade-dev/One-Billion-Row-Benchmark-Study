# Extracts location data from the `worldcities.csv` file and retrieves temperature values.


import asyncio
import aiohttp
import csv
import os
from tqdm import tqdm

# Config
INPUT_FILE = "data\worldcities_teste.csv"
OUTPUT_FILE = "cities_temperatures.csv"
BATCH_SIZE = 1000
MAX_CITIES = None

def load_cities(filepath: str, max_cities: int | None) -> list[dict]:
    cities = []
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if max_cities and i >= max_cities:
                break
            try:
                cities.append({
                    "city": row["city"],
                    "lat":  float(row["lat"]),
                    "lng":  float(row["lng"]),
                })
            except (ValueError, KeyError):
                # Ignore lines with empty or invalid values
                continue
    print(cities)

async def main():
    if not os.path.exists(INPUT_FILE):
        print(f" File '{INPUT_FILE}' not found.")
        return
 
    cities  = load_cities(INPUT_FILE, MAX_CITIES)
    #results = await fetch_all_temperatures(cities)
    #   save_results(results, OUTPUT_FILE)
 
 
if __name__ == "__main__":
    asyncio.run(main())