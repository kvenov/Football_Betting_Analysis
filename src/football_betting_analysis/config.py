# CORE PROJECT CONFIGURATION

# League configuration
LEAGUE = "La_Liga"
LEAGUE_CODE_FD = "SP1"  # football-data.co.uk code

# Seasons
START_SEASON = 2014
END_SEASON = 2025

SEASONS = list(range(START_SEASON, END_SEASON))

# Data paths
RAW_DATA_PATH = "data/raw/"
INTERIM_DATA_PATH = "data/interim/"
PROCESSED_DATA_PATH = "data/processed/"

# Understat
UNDERSTAT_DATA_PATH = f"{RAW_DATA_PATH}/understat_data/"
UNDERSTAT_BASE_URL = "https://understat.com/league"
UNDERSTAT_INTERIM_PATH = f"{INTERIM_DATA_PATH}/understat_data/"

# Football-Data.co.uk
FOOTBALL_DATA_PATH = f"{RAW_DATA_PATH}/football_data_co_uk/"
FOOTBALL_DATA_BASE_URL = "https://www.football-data.co.uk/mmz4281/{season}/{league}.csv"
FOOTBALL_DATA_INTERIM_PATH = f"{INTERIM_DATA_PATH}/football_data_co_uk/"