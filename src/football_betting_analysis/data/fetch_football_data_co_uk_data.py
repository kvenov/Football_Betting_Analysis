import pandas as pd
import numpy as np

import os
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from football_betting_analysis.config import START_SEASON, END_SEASON, LEAGUE_CODE_FD, FOOTBALL_DATA_PATH, FOOTBALL_DATA_BASE_URL

# Define seasons (format: last two digits):
SEASONS = [f"{str(year)[-2:]}{str(year+1)[-2:]}" for year in range(START_SEASON, END_SEASON)]

# The http url on which the csv data will is fetched from:
base_url = FOOTBALL_DATA_BASE_URL

# The output file path in which the fetched data will be loaded:
OUTPUT_FILE = f"{FOOTBALL_DATA_PATH}matches.csv"

# Configuration variables for the fetching process:
MAX_WORKERS = 5
RETRIES = 3
BASE_DELAY = 1.0

logger = logging.getLogger(__name__)

# This is the data schema that the data will based to, and the schema is defined, 
# because the datasets are inconsistent and dont provide the data in the same way with the same features!
EXPECTED_COLUMNS = [
    "Div", "Date", "Time", "HomeTeam", "AwayTeam",
    "FTHG", "FTAG", "FTR",
    "HTHG", "HTAG", "HTR",
    "Attendance", "Referee",
    "HS", "AS", "HST", "AST",
    "HHW", "AHW",
    "HC", "AC",
    "HF", "AF",
    "HFKC", "AFKC",
    "HO", "AO",
    "HY", "AY",
    "HR", "AR",
    "1XBH", "1XBD", "1XBA",
    "B365H", "B365D", "B365A",
    "BFH", "BFD", "BFA",
    "season", "league"
]

# COLUMN ALIASES
# These array is made, becasue these special columns may be represented in different ways across the datasets!  
COLUMN_ALIASES = {
    "HG": "FTHG",
    "AG": "FTAG",
    "Res": "FTR"
}

def fetch_csv_with_retry(url: str) -> pd.DataFrame:
    """
    Fetches football-data.co.uk csv data, and loads the data into a pandas Data Frame.
    Parameters:
        url: The http url from which the csv data to be taken from
    Returns:
        A pandas Data Frame containg the csv data for the specified url!
    """
    
    for attempt in range(1, RETRIES + 1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            return pd.read_csv(url)

        except Exception as e:
            if attempt == RETRIES:
                logger.error(f"Final failure: {url} -> {e}")
                return None

            delay = BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(f"Retry {attempt}/{RETRIES} for {url} in {delay:.2f}s")
            time.sleep(delay)


def parse_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parses the `Date` column of the pandas Data Frame into a datetime object.
    Parameters:
        df: A pandas Data Frame containg the matches data
    Returns:
        A pandas Data Frame with the processed datetime `Date` column of the matches dataset.
    """
    raw_dates = df["Date"].copy()  # keeping the original values

    # First attempt: 4-digit year
    parsed = pd.to_datetime(
        raw_dates,
        format="%d/%m/%Y",
        errors="coerce"
    )

    # Second attempt: 2-digit year
    mask = parsed.isna()
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(
            raw_dates[mask],
            format="%d/%m/%y",
            errors="coerce"
        )

    df["Date"] = parsed
    
    return df


def is_correct_season(df: pd.DataFrame, season: str) -> bool:
    """
    Checks if the provided Data Frame is with valid season, with start date bigger than or equal to the provided season start date and with end date smaller than or equal to the provided season end date.
    Parameters:
        df: The matches Data Frame
        season: The season with which the data to be validated with
    Returns:
        A boolean, specifing if the Data Frame is with valid season ot not.
    """
    try:
        years = df["Date"].dt.year.dropna()

        if years.empty:
            logger.error(f'No data succeed to be fetched for season {season}')
            return False

        start_year = int("20" + season[:2])
        end_year = int("20" + season[2:])

        return (
            years.min() >= start_year - 1 and
            years.max() <= end_year + 1
        )
    except Exception:
        return False


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures dataframe matches EXPECTED_COLUMNS schema.
    - Renames aliases
    - Adds missing columns
    - Drops extra columns
    - Orders columns
    """

    # Rename known aliases
    df = df.rename(columns=COLUMN_ALIASES)

    # Add missing columns
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    # Keep only expected columns
    df = df[EXPECTED_COLUMNS]

    return df


def process_season(season: str, league_code: str, base_url: str) -> pd.DataFrame:
    """
    Processes a single season by downloading, validating, and enriching match data for the given league.
    Parameters:
        season: The season identifier in format 'YYZZ' (e.g., '1415' for 2014/2015)
        league_code: The league code used in the data source (e.g., 'SP1')
        base_url: The base URL template used to construct the CSV download link
    Returns:
        A pandas DataFrame containing the processed data for the given season with added metadata,
        or None if the data could not be fetched, parsed, or validated.
    """
    url = base_url.format(season=season, league=league_code)

    df = fetch_csv_with_retry(url)

    if df is None or df.empty:
        logger.error(f'process_season: The provided Data Frame is None or Empty!')
        return None

    df = parse_date_column(df)

    if not is_correct_season(df, season):
        logger.warning(f"Skipping wrong data: {season}")
        return None

    # Add useful metadata
    full_season = f"20{season[:2]}/20{season[2:]}"
    
    df = df.assign(
                season=full_season,
                league=league_code
            )

    df = standardize_columns(df)

    return df


def write_chunk(df: pd.DataFrame, file_path: str) -> None:
    """
    Writes a DataFrame chunk to a CSV file at the specified file path
    Parameters:
        df: The pandas DataFrame chunk to be written to the file
        file_path: The full file path where the CSV file will be stored
    Returns:
        None
    """
    if df is None or df.empty:
        logger.error(f'write_chunk: The provided Data Frame is None or Empty!')
        return

    # Cheking if the output dir exists:
    write_header = not os.path.exists(file_path)

    df.to_csv(
        file_path,
        mode="a",
        header=write_header,
        index=False
    )


def load_football_data(league_code: str, seasons: list) -> None:
    """
    Loads football match data for multiple seasons and a given league, processes each season in parallel, and stores the combined results into a single CSV file.
    Parameters:
        league_code: The league code used in the data source (e.g., 'SP1')
        seasons: A list of season identifiers in format 'YYZZ' (e.g., ['1415', '1516', ...])
    Returns:
        None
    """
    # Ensuring that the output directory esists!
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    all_data = []
    failed = []
    
    logger.info(f"Processing {len(seasons)} seasons")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_season, season, league_code, base_url): season
            for season in seasons
        }

        for future in as_completed(futures):
            season = futures[future]

            try:
                df = future.result()
            except Exception as e:
                logger.error(f"Error for season {season}: {e}")
                failed.append(season)
                continue

            if df is None:
                failed.append(season)
                continue

            all_data.append(df)


    if not all_data:
        logger.error("No data collected!")
        return None

    final_df = pd.concat(all_data, ignore_index=True)

    # Saving the data into csv file:
    final_df.to_csv(OUTPUT_FILE, index=False)
    
    logger.info(f"Failed seasons: {len(failed)}")
    

def fetch_football_data():
    load_football_data(LEAGUE_CODE_FD, SEASONS)

    print("ALL DATA COLLECTED SUCCESSFULLY")
