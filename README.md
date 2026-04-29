# One Billion Rows: A Data Processing Study

## Introduction

This project aims to advance the study of Python and Big Data–related technologies. To this end, a dataset containing one billion rows will be processed to compute statistical measures, including aggregation and sorting, which are computationally intensive operations, using a variety of technologies.

This study is based on the [One-Billion-Row-Challenge-Python](https://github.com/lvgalvao/One-Billion-Row-Challenge-Python) git repository, which in turn is based on [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc).

The dataset consists of temperature measurements from multiple weather stations. Each record follows the format <string: station name>;<double: measurement>, with temperatures reported to one decimal place.

The challenge is to develop a program capable of reading this file and computing the minimum, average (rounded to one decimal place), and maximum temperature for each station, presenting the results in a table sorted by station name.

Multiple processing approaches will be evaluated using different programming languages and libraries. Ultimately, a benchmark will be developed to compare the execution times of the task.

## Generation of the 1 Billion Rows Dataset

The cities dataset was obtained from: [Simplemaps](https://simplemaps.com/data/world-cities). With approximately 48,000 cities worldwide, the CSV file contains several attributes, such as "city","city_ascii","lat","lng","country", etc.

To retrieve temperature data, the API from [Weather API](https://www.weatherapi.com) will be used. It is free, but requires registration and API key, and allows temperature queries based on city names. The advantage of using the WeatherAPI is its limit of 1 million requests for the free tier; therefore, individual requests were made to the WeatherAPI with a concurrency of 10 simultaneous requests.

Next, the requests will be processed asynchronously using **asyncio** and **aiohttp** to improve performance. The results will be saved to a CSV file.

After generating the new CSV file with approximately 48,000 rows in the format `"city, temperature"`, a separate Python script will be used to generate additional random samples for these cities in order to scale the dataset up to 1 billion rows.

## Dependencies

### 1 Billion row csv file

#### External Libraries

- **aiohttp** — Asynchronous HTTP requests to the WeatherAPI
- **tqdm** — Progress bar for tracking city processing
- **python-dotenv** — Loading environment variables from `.env` file

#### Native Python Libraries

- **asyncio** — Manages asynchronous execution and concurrency control
- **csv** — Reads the input CSV and writes the output CSV
- **pathlib** — Cross-platform file path handling
- **os** — Reads environment variables loaded by python-dotenv

#### Installation

```bash
poetry add aiohttp tqdm python-dotenv
```

## Resultados

Mensionar o modelo do computador utilizado e onde serão expostos os dados comparativos

## Conclusão

O que foi concluído com o projeto?

## Extras e observações
