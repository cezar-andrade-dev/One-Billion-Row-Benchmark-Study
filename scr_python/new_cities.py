import csv
from pathlib import Path

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
BASE_DIR         = Path(__file__).parent
DATA_DIR         = BASE_DIR.parent / "data"
TEMPERATURES_FILE = DATA_DIR / "cities_temperatures.csv"   # CSV gerado pelo script
WORLDCITIES_FILE  = DATA_DIR / "worldcities.csv"           # CSV original
OUTPUT_FILE       = DATA_DIR / "cities_not_found.csv"      # CSV com as não encontradas
# ──────────────────────────────────────────────────────────────────────────────


def load_not_found(filepath: Path) -> set[str]:
    """Retorna o conjunto de cidades sem temperatura no CSV de resultados."""
    not_found = set()
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row["temperature"].strip():
                not_found.add(row["city"].strip())
    return not_found


def extract_ascii_names(worldcities: Path, not_found: set[str]) -> list[dict]:
    """Cruza com o worldcities.csv e retorna city_ascii das cidades não encontradas."""
    results = []
    with open(worldcities, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["city"].strip() in not_found:
                results.append({
                    "city":       row["city"].strip(),
                    "city_ascii": row["city_ascii"].strip(),
                })
    return results


def save_results(results: list[dict], filepath: Path) -> None:
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["city", "city_ascii"])
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ Arquivo salvo: {filepath}  ({len(results)} cidades)")


def main():
    print("🔍 Carregando cidades não encontradas...")
    not_found = load_not_found(TEMPERATURES_FILE)
    print(f"   {len(not_found)} cidades sem temperatura\n")

    print("🔗 Cruzando com worldcities.csv para obter city_ascii...")
    results = extract_ascii_names(WORLDCITIES_FILE, not_found)

    save_results(results, OUTPUT_FILE)


if __name__ == "__main__":
    main()