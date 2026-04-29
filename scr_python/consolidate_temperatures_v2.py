import csv
from pathlib import Path

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
DATA_DIR       = BASE_DIR.parent / "data"
FILE_1         = DATA_DIR / "cities_temperatures.csv"           # 1ª rodada (city)
FILE_2         = DATA_DIR / "cities_not_found_temperatures.csv" # 2ª rodada (city_ascii)
NOT_FOUND_FILE = DATA_DIR / "cities_not_found.csv"             # ponte: city <-> city_ascii
OUTPUT_FILE    = DATA_DIR / "cities_temperatures_final_v2.csv"
# ──────────────────────────────────────────────────────────────────────────────


def load_ascii_to_city_map(filepath: Path) -> dict[str, str]:
    """
    Carrega o cities_not_found.csv e retorna um dicionário
    {city_ascii: city} para usar como ponte na consolidação.
    """
    mapping = {}
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping[row["city_ascii"].strip()] = row["city"].strip()
    return mapping


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


def consolidate(file1: Path, file2: Path, not_found_file: Path) -> list[dict]:
    """
    Mescla os dois CSVs usando cities_not_found.csv como ponte
    para mapear city_ascii de volta para o city original.
    """
    print(f"📂 Carregando {file1.name}...")
    base = load_temperatures(file1)
    print(f"   {len(base)} cidades\n")

    print(f"📂 Carregando mapa city_ascii → city...")
    ascii_map = load_ascii_to_city_map(not_found_file)
    print(f"   {len(ascii_map)} entradas no mapa\n")

    print(f"📂 Carregando {file2.name}...")
    complemento = load_temperatures(file2)
    atualizadas = 0
    nao_mapeadas = 0

    for city_ascii, temp in complemento.items():
        if not temp:
            continue
        # Converte city_ascii de volta para o city original
        city_original = ascii_map.get(city_ascii)
        if city_original and not base.get(city_original):
            base[city_original] = temp
            atualizadas += 1
        elif not city_original:
            nao_mapeadas += 1

    print(f"   {atualizadas} cidades atualizadas com temperatura")
    if nao_mapeadas:
        print(f"   ⚠️  {nao_mapeadas} entradas sem mapeamento (ignoradas)\n")

    return [{"city": city, "temperature": temp} for city, temp in base.items()]


def save_results(results: list[dict], filepath: Path) -> None:
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["city", "temperature"])
        writer.writeheader()
        writer.writerows(r for r in results if r["temperature"])

    total    = len(results)
    com_temp = sum(1 for r in results if r["temperature"])
    sem_temp = total - com_temp

    print(f"✅ Arquivo salvo: {filepath}")
    print(f"   ✔  {com_temp} cidades com temperatura")
    print(f"   ✘  {sem_temp} cidades sem temperatura")
    print(f"   📊 Total: {total} cidades")


def main():
    results = consolidate(FILE_1, FILE_2, NOT_FOUND_FILE)
    save_results(results, OUTPUT_FILE)


if __name__ == "__main__":
    main()