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
