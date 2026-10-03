# scalable-systems-project

CS-E4780 Course Project — Detecting Trading Trends in Financial Tick Data

## Setup

1. Install Python 3.11 and Java 11.
2. Create a venv and install dependencies:

python3.11 -m venv venv
source venv/bin/activate # on Windows: venv\Scripts\activate
pip install apache-flink

3. Download the full dataset (7 CSV files, one per day) from:
   https://doi.org/10.5281/zenodo.6382482
4. Place all 7 CSVs in `data/raw/` (this folder is gitignored — you must
   download the data yourself, it is not included in this repo).

## Generating a test sample

The full dataset is 3-5.5GB per weekday file, which is slow to iterate on.
`job.py` currently reads from `data/raw_test/`, a smaller subset, for
development. To generate it:

1. Copy one or two weekday CSVs (avoid weekends — Nov 13/14 — since those
   contain almost no real trades, only housekeeping events) into
   `data/raw_test/`:

mkdir -p data/raw_test
cp data/raw/debs2022-gc-trading-day-10-11-21.csv data/raw_test/
cp data/raw/debs2022-gc-trading-day-11-11-21.csv data/raw_test/

2. Run `python job.py` — output is written to `output/results/` as CSV
   part-files (symbol, window end timestamp, EMA38, EMA100, advisory).

## Known issues

- **Windows**: running `job.py` with `parallelism > 1` crashes immediately
  with `Process died with exit code 0` (a known PyFlink/Windows subprocess
  spawning issue). Use WSL2 (Ubuntu) instead — confirmed working there at
  parallelism=2 against real multi-GB data.

## Next steps

- Scale up to the full 7-day dataset and higher parallelism
- Performance/scalability measurements (throughput at parallelism 1/2/4/8)

