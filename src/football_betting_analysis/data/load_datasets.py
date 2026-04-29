import logging
from football_betting_analysis.data.fetch_football_data_co_uk_data import fetch_football_data
from football_betting_analysis.data.fetch_understat_data import fetch_understat_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

if __name__ == "__main__":
    fetch_understat_data()
    fetch_football_data()
