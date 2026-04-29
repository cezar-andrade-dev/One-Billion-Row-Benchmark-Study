import asyncio
import aiohttp
import csv
import os
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv


# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent
DATA_DIR      = BASE_DIR.parent / "data"
ENV_PATH      = BASE_DIR.parent / ".env"
INPUT_FILE    = DATA_DIR / "cities_not_found.csv"
OUTPUT_FILE   = DATA_DIR / "cities_not_found_temperatures.csv"
load_dotenv(ENV_PATH)
API_KEY       = os.getenv("WEATHER_API_KEY")
CONCURRENCY   = 10                     # requisições simultâneas
DELAY_SECONDS = 0.1                    # pausa entre requisições (ms)
MAX_CITIES    = None                   # None = todas | ex: 500 = primeiras 500
# ──────────────────────────────────────────────────────────────────────────────


def load_cities(filepath: Path, max_cities: int | None) -> list[str]:
    """Lê o CSV e retorna apenas a lista de nomes de cidades."""
    cities = []
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if max_cities and i >= max_cities:
                break
            city = row.get("city_ascii", "").strip()
            if city:
                cities.append(city)
    return cities


async def fetch_temperature(
    session: aiohttp.ClientSession,
    city: str,
    semaphore: asyncio.Semaphore,
    retries: int = 3
) -> dict:
    """Busca a temperatura atual de uma cidade na WeatherAPI."""
    url = "https://api.weatherapi.com/v1/current.json"
    params = {"key": API_KEY, "q": city, "aqi": "no"}

    async with semaphore:
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:

                    if resp.status == 429:
                        wait = 10 * attempt
                        tqdm.write(f"⏳ Rate limit — aguardando {wait}s...")
                        await asyncio.sleep(wait)
                        continue

                    if resp.status == 400:
                        # Cidade não encontrada na WeatherAPI
                        return {"city": city, "temperature": None, "status": "not_found"}

                    resp.raise_for_status()
                    data = await resp.json()
                    temp = data["current"]["temp_c"]
                    return {"city": city, "temperature": temp, "status": "ok"}

            except Exception as e:
                if attempt == retries:
                    tqdm.write(f"⚠️  Falha em '{city}': {e}")
                    return {"city": city, "temperature": None, "status": "error"}
                await asyncio.sleep(2 ** attempt)

    return {"city": city, "temperature": None, "status": "error"}


async def fetch_all_temperatures(cities: list[str]) -> list[dict]:
    """Processa todas as cidades de forma assíncrona com concorrência controlada."""
    print(f"\n📍 {len(cities)} cidades carregadas")
    print(f"⚙️  Concorrência: {CONCURRENCY} | Delay: {DELAY_SECONDS}s\n")

    semaphore = asyncio.Semaphore(CONCURRENCY)
    results = []

    async with aiohttp.ClientSession() as session:
        with tqdm(total=len(cities), unit="cidade", desc="Buscando temperaturas") as pbar:

            async def fetch_and_update(city):
                result = await fetch_temperature(session, city, semaphore)
                await asyncio.sleep(DELAY_SECONDS)
                pbar.update(1)
                return result

            tasks = [fetch_and_update(city) for city in cities]
            results = await asyncio.gather(*tasks)

    return results


def save_results(results: list[dict], filepath: Path) -> None:
    """Salva o CSV final com city e temperature."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["city", "temperature"])
        writer.writeheader()
        for r in results:
            writer.writerow({"city": r["city"], "temperature": r["temperature"]})

    total      = len(results)
    ok         = sum(1 for r in results if r["status"] == "ok")
    not_found  = sum(1 for r in results if r["status"] == "not_found")
    errors     = sum(1 for r in results if r["status"] == "error")

    print(f"\n✅ Arquivo salvo: {filepath}")
    print(f"   ✔  {ok} cidades com temperatura")
    print(f"   ✘  {not_found} cidades não encontradas na API")
    print(f"   ⚠  {errors} erros")


async def main():
    if API_KEY == "SUA_CHAVE_AQUI":
        print("❌ Configure sua API_KEY no topo do script.")
        return

    if not INPUT_FILE.exists():
        print(f"❌ Arquivo '{INPUT_FILE}' não encontrado.")
        return

    cities  = load_cities(INPUT_FILE, MAX_CITIES)
    results = await fetch_all_temperatures(cities)
    save_results(results, OUTPUT_FILE)


if __name__ == "__main__":
    asyncio.run(main())
