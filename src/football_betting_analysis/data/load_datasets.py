import os
import logging
from football_betting_analysis.config import FOOTBALL_DATA_PATH, UNDERSTAT_DATA_PATH, ELO_RATINGS_DATA_PATH, SOCCER_DATA_PATH
from football_betting_analysis.data.fetch_football_data_co_uk_data import fetch_football_data
from football_betting_analysis.data.fetch_understat_data import fetch_understat_data
from football_betting_analysis.data.cloude_storage.load_gcs_datasets import load_all_datasets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# Ensuring that everydata has its folder existing:
os.makedirs(FOOTBALL_DATA_PATH, exist_ok=True)
os.makedirs(UNDERSTAT_DATA_PATH, exist_ok=True)
os.makedirs(ELO_RATINGS_DATA_PATH, exist_ok=True)
os.makedirs(SOCCER_DATA_PATH, exist_ok=True)

if __name__ == "__main__":
    fetch_understat_data()
    fetch_football_data()
    load_all_datasets()
