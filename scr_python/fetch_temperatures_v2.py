import asyncio
import aiohttp
import csv
from pathlib import Path
from tqdm import tqdm

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent
INPUT_FILE    = BASE_DIR / "data/worldcities_teste.csv"
OUTPUT_FILE   = BASE_DIR / "data/cities_temperatures_teste.csv"
BATCH_SIZE    = 100    # cidades por requisição
MAX_CITIES    = None   # None = todas | ex: 500 = apenas as 500 primeiras
DELAY_SECONDS = 2.0    # pausa entre cada batch — aumente se ainda receber 429
# ──────────────────────────────────────────────────────────────────────────────


def load_cities(filepath: Path, max_cities: int | None) -> list[dict]:
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
                continue
    return cities


async def fetch_batch(
    session: aiohttp.ClientSession,
    batch: list[dict],
    retries: int = 5
) -> list[dict]:
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

                if resp.status == 429:
                    # Lê o Retry-After da API; se não vier, usa backoff crescente
                    retry_after = int(resp.headers.get("Retry-After", 15 * attempt))
                    tqdm.write(f"⏳ 429 recebido — aguardando {retry_after}s (tentativa {attempt}/{retries})...")
                    await asyncio.sleep(retry_after)
                    continue

                resp.raise_for_status()
                data = await resp.json()

                if isinstance(data, dict):
                    data = [data]

                return [
                    {"city": city["city"], "temperature": info.get("current_weather", {}).get("temperature")}
                    for city, info in zip(batch, data)
                ]

        except Exception as e:
            if attempt == retries:
                tqdm.write(f"⚠️  Batch falhou definitivamente: {e}")
                return [{"city": c["city"], "temperature": None} for c in batch]
            wait = 5 * attempt
            tqdm.write(f"⚠️  Erro na tentativa {attempt}: {e} — aguardando {wait}s...")
            await asyncio.sleep(wait)

    return [{"city": c["city"], "temperature": None} for c in batch]


async def fetch_all_temperatures(cities: list[dict]) -> list[dict]:
    batches = [cities[i:i + BATCH_SIZE] for i in range(0, len(cities), BATCH_SIZE)]
    all_results = []

    print(f"\n📍 {len(cities)} cidades — {len(batches)} batches de {BATCH_SIZE}")
    print(f"⚙️  Modo sequencial | Delay: {DELAY_SECONDS}s entre batches\n")

    async with aiohttp.ClientSession() as session:
        with tqdm(total=len(cities), unit="cidade", desc="Buscando temperaturas") as pbar:
            for batch in batches:
                result = await fetch_batch(session, batch)
                all_results.extend(result)
                pbar.update(len(batch))
                await asyncio.sleep(DELAY_SECONDS)  # pausa fixa entre cada batch

    return all_results


def save_results(results: list[dict], filepath: Path) -> None:
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["city", "temperature"])
        writer.writeheader()
        writer.writerows(results)

    total     = len(results)
    com_temp  = sum(1 for r in results if r["temperature"] is not None)
    sem_temp  = total - com_temp
    print(f"\n✅ Arquivo salvo: {filepath}")
    print(f"   {com_temp}/{total} cidades com temperatura | {sem_temp} sem dados")


async def main():
    if not INPUT_FILE.exists():
        print(f"❌ Arquivo '{INPUT_FILE}' não encontrado.")
        return

    cities  = load_cities(INPUT_FILE, MAX_CITIES)
    results = await fetch_all_temperatures(cities)
    save_results(results, OUTPUT_FILE)


if __name__ == "__main__":
    asyncio.run(main())
