import csv
from pathlib import Path

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR.parent / "data"
FILE_1      = DATA_DIR / "cities_temperatures.csv"
FILE_2      = DATA_DIR / "cities_not_found_temperatures.csv"
OUTPUT_FILE = DATA_DIR / "cities_temperatures_final.csv"
# ──────────────────────────────────────────────────────────────────────────────


def load_temperatures(filepath: Path) -> dict[str, str]:
    """Carrega o CSV e retorna dicionário {city: temperature}."""
    results = {}
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row["city"].strip()
            temp = row["temperature"].strip()
            results[city] = temp
    return results


def consolidate(file1: Path, file2: Path) -> list[dict]:
    """
    Mescla os dois CSVs priorizando o resultado da 2ª rodada (city_ascii)
    para as cidades que antes estavam sem temperatura.
    """
    print(f"📂 Carregando {file1.name}...")
    base = load_temperatures(file1)
    print(f"   {len(base)} cidades carregadas\n")

    print(f"📂 Carregando {file2.name}...")
    complemento = load_temperatures(file2)
    atualizadas = 0
    for city, temp in complemento.items():
        if temp and not base.get(city):
            base[city] = temp
            atualizadas += 1
    print(f"   {atualizadas} cidades atualizadas com temperatura\n")

    return [{"city": city, "temperature": temp} for city, temp in base.items()]


def save_results(results: list[dict], filepath: Path) -> None:
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["city", "temperature"])
        writer.writeheader()
        writer.writerows(results)

    total      = len(results)
    com_temp   = sum(1 for r in results if r["temperature"])
    sem_temp   = total - com_temp

    print(f"✅ Arquivo salvo: {filepath}")
    print(f"   ✔  {com_temp} cidades com temperatura")
    print(f"   ✘  {sem_temp} cidades sem temperatura")


def main():
    results = consolidate(FILE_1, FILE_2)
    save_results(results, OUTPUT_FILE)


if __name__ == "__main__":
    main()