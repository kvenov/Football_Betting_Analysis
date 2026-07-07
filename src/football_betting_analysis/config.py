# CORE PROJECT CONFIGURATION

# Target variable for predictions:
TARGET = "result_full"

# League configuration
SOCCERDATA_LEAGUE = "ESP-La Liga"
LEAGUE = "La_Liga"
LEAGUE_CODE_FD = "SP1"

# Seasons
START_SEASON = 2014
END_SEASON = 2025

# Start/End date of the Spanish La Liga:
START_DATE = '2014-08-01'
END_DATE = '2025-05-31'

SEASONS = list(range(START_SEASON, END_SEASON))

# Project date format:
DATE_FORMAT = '%Y-%m-%d'

# Time window: used for the creation of the player importance features(in the transfermarkt cleaning notebook)
WINDOW = 10

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

# Elo Ratings:
ELO_RATINGS_DATA_PATH = f"{RAW_DATA_PATH}/elo_ratings/"
ELO_RATINGS_INTERIM_PATH = f"{INTERIM_DATA_PATH}/elo_ratings/"

# Soccerdata:
SOCCER_DATA_PATH = f"{RAW_DATA_PATH}/soccer_data/"
SOCCER_DATA_INTERIM_PATH = f"{INTERIM_DATA_PATH}/soccer_data/"

# Transfermarkt:
TRANSFERMARKT_DATA_PATH = f"{RAW_DATA_PATH}/transfermarkt_data/"
TRANSFERMARKT_DATA_INTERIM_PATH = f"{INTERIM_DATA_PATH}/transfermarkt_data/"