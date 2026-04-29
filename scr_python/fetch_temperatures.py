# Extracts location data from the `worldcities.csv` file and retrieves temperature values.

import asyncio
import aiohttp
import csv
from pathlib import Path
from tqdm import tqdm

# Config ---------------------------------------------------------
BASE_DIR = Path(__file__).parent
INPUT_FILE = BASE_DIR / "data/worldcities.csv"
OUTPUT_FILE = BASE_DIR / "data/cities_temperatures.csv"
BATCH_SIZE = 100
MAX_CITIES = None
DELAY_SECONDS = 2.0 
# -------------------------------------------------------------------

def load_cities(filepath: Path, max_cities: int | None) -> list[dict]:
    cities = []
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if max_cities and i >= max_cities:
                break
            try:
                cities.append({
                    "city": row["city_ascii"],
                    "lat":  float(row["lat"]),
                    "lng":  float(row["lng"]),
                })
            except (ValueError, KeyError):
                # Ignore lines with empty or invalid values
                continue
    return cities
    
async def fetch_batch(
    session: aiohttp.ClientSession,
    batch: list[dict],
    retries: int = 5
) -> list[dict]:
    # Sends a batch of cities to Open-Meteo and returns their current temperatures.
 
    url = "https://api.open-meteo.com/v1/forecast"
    payload = {
        "latitude":        [c["lat"] for c in batch],
        "longitude":       [c["lng"] for c in batch],
        "current_weather": True,
        "forecast_days":   1,
    }    
 
    for attempt in range(1, retries + 1):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                
                # If a 429 response is received, respect the API’s Retry-After before retrying
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 15 * attempt))
                    tqdm.write(f"\n Rate limit reached — waiting {retry_after}s before retrying...")
                    await asyncio.sleep(retry_after)
                    continue
 
                resp.raise_for_status()
                data = await resp.json()

                if isinstance(data, dict):
                    data = [data]
                
                results = []                
                for city, info in zip(batch, data):
                    temp = info.get("current_weather", {}).get("temperature", None)
                    results.append({
                        "city":        city["city"],
                        "temperature": temp,
                    })
 
                pbar.update(len(batch))
                return results
 
        except Exception as e:
            if attempt == retries:
                print(f"\n Batch failed after {retries} attempts: {e}")
                # Returns null temperature for batch errors.
                pbar.update(len(batch))
                return [{"city": c["city"], "temperature": None} for c in batch]
            await asyncio.sleep(2 ** attempt)  # Exponential backoff before retrying.

    # Fallback
    pbar.update(len(batch))
    return [{"city": c["city"], "temperature": None} for c in batch]


async def fetch_all_temperatures(cities: list[dict]) -> list[dict]:
    #Splits cities into batches and processes them asynchronously.

    batches = [cities[i:i + BATCH_SIZE] for i in range(0, len(cities), BATCH_SIZE)]
    all_results = []
 
    print(f"\n{len(cities)} Loaded cities — {len(batches)} batches of {BATCH_SIZE}\n")
    print(f"⚙️  Concurrency: {CONCURRENCY} Delay: {DELAY_SECONDS}s between batches\n")

    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        with tqdm(total=len(cities), unit="city", desc="fetching temperature") as pbar:
 
            async def bounded_fetch(batch):
                async with semaphore:
                    result = await fetch_batch(session, batch, pbar)
                    await asyncio.sleep(DELAY_SECONDS)  # pausa após cada batch
                    return result
                
            tasks = [bounded_fetch(b) for b in batches]
            results_per_batch = await asyncio.gather(*tasks)
 
            for batch_result in results_per_batch:
                all_results.extend(batch_result)
 
    return all_results
 
 
def save_results(results: list[dict], filepath: Path) -> None:
    #Saves the final CSV with city and temperature.
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["city", "temperature"])
        writer.writeheader()
        writer.writerows(results)
    print(f"\n Saved File: {filepath}  ({len(results)} Cities)")

async def main():
    if not INPUT_FILE.exists():
        print(f" File '{INPUT_FILE}' not found.")
        return
 
    cities  = load_cities(INPUT_FILE, MAX_CITIES)
    results = await fetch_all_temperatures(cities)
    save_results(results, OUTPUT_FILE)
 
 
if __name__ == "__main__":
    asyncio.run(main())