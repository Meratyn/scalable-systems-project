# scalable-systems-project



\## Setup



1\. Install Python 3.11 and Java 11.

2\. Create a venv and install dependencies:



python -m venv venv

venv\\Scripts\\activate # or source venv/bin/activate on Mac/Linux

pip install apache-flink



3\. Download the full dataset (7 CSV files, one per day) from:

&#x20;  https://doi.org/10.5281/zenodo.6382482

4\. Place all 7 CSVs in `data/raw/` (this folder is gitignored — you must

&#x20;  download the data yourself, it is not included in this repo).

5\. Run `python job.py` to process the data.

