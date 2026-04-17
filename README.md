# One Billion Rows: A Data Processing Study

This project aims to advance the study of Python and Big Data–related technologies. To this end, a dataset containing one billion rows will be processed to compute statistical measures, including aggregation and sorting, which are computationally intensive operations, using a variety of technologies.

This study is based on the [One-Billion-Row-Challenge-Python](https://github.com/lvgalvao/One-Billion-Row-Challenge-Python) git repository, which in turn is based on [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc).

The dataset consists of temperature measurements from multiple weather stations. Each record follows the format <string: station name>;<double: measurement>, with temperatures reported to one decimal place.

The challenge is to develop a program capable of reading this file and computing the minimum, average (rounded to one decimal place), and maximum temperature for each station, presenting the results in a table sorted by station name.

Multiple processing approaches will be evaluated using different programming languages and libraries. Ultimately, a benchmark will be developed to compare the execution times of the task.
