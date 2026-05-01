import pandas as pd
import understatapi
import time
import os
import random
import logging
from football_betting_analysis.config import LEAGUE, SEASONS, UNDERSTAT_DATA_PATH
from concurrent.futures import ThreadPoolExecutor, as_completed

# The output directory in which the datasets to be loaded.
OUTPUT_DIR = UNDERSTAT_DATA_PATH

# Guaranteeing that the output dir exists!
os.makedirs(OUTPUT_DIR, exist_ok=True)

# The undestat client from which the api requests are made
client = understatapi.UnderstatClient()

logger = logging.getLogger(__name__)

def safe_call(
    func,
    retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0
):
    """
    Retry wrapper with exponential backoff + jitter.
    """

    for attempt in range(1, retries + 1):
        try:
            return func()

        except Exception as e:
            if attempt == retries:
                logger.error(f"Final failure after {retries} attempts: {e}")
                return None

            # Exponential backoff with jitter
            # With this delay we prevent retry storms that overwhelm services during recovery, 
            # ensuring improved system resilience and reducing load on overloaded servers
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            jitter = random.uniform(0, delay * 0.3)

            logger.warning(
                f"Attempt {attempt}/{retries} failed: {e}. "
                f"Retrying in {delay + jitter:.2f}s"
            )

            time.sleep(delay + jitter)


def load_match_data(league: str, seasons: list) -> pd.DataFrame:
    """
    Loads understat matches data.
    Saves the data into a csv file.
    Parameters:
        league: The name of the league (e.g. La Liga, Premier League).
        seasons: The range of seasons to data to be loaded from.
    Returns:
        Returns the loaded matches as a pandas Data Frame
    """
    file_path = f"{OUTPUT_DIR}matches.csv"
    failed_seasons = []

    total_rows = 0

    for season in seasons:
        data = safe_call(
            lambda: client.league(league).get_match_data(season)
        )

        if data is None or len(data) == 0:
            logger.error(f'get_match_data: Something went wrong while fetching the matches data for season {season}')
            failed_seasons.append(season)
            continue

        for match in data:
            match["season"] = f"{season}/{season+1}"

        df_chunk = pd.json_normalize(data, sep='_')

        if not df_chunk.empty:
            write_header = not os.path.exists(file_path)

            df_chunk.to_csv(
                file_path,
                mode="a",
                header=write_header,
                index=False
            )

            total_rows += len(df_chunk)

    print(f"Saved matches: {total_rows} rows")
    print(f"Failed seasons: {len(failed_seasons)}")

    if len(failed_seasons) == 0:
        logger.info(msg='All of the matches have been loaded successfully!')

    return pd.read_csv(file_path)


def load_player_data(league: str, seasons: list) -> None:
    """
    Loads understat players data.
    Saves the data into a csv file.
    Parameters:
        league: The name of the league (e.g. La Liga, Premier League).
        seasons: The range of seasons to data to be loaded from.
    Returns:
        None
    """
    file_path = f"{OUTPUT_DIR}players.csv"
    failed_seasons = []

    total_rows = 0

    for season in seasons:
        data = safe_call(
            lambda: client.league(league).get_player_data(season)
        )

        if data is None or len(data) == 0:
            logger.error(f'get_player_data: Something went wrong while fetching the players data for season {season}')
            failed_seasons.append(season)
            continue

        for player in data:
            player["season"] = f"{season}/{season+1}"

        df_chunk = pd.json_normalize(data, sep='_')

        if not df_chunk.empty:
            write_header = not os.path.exists(file_path)

            df_chunk.to_csv(
                file_path,
                mode="a",
                header=write_header,
                index=False
            )

            total_rows += len(df_chunk)

    print(f"Saved players: {total_rows} rows")
    print(f"Failed seasons: {len(failed_seasons)}")
    
    if len(failed_seasons) == 0:
        logger.info(msg='All of the players have been loaded successfully')


def fetch_team_context(team_name: str, season: int):
    """
    Fetches team context data by a team_name.
    Parameters:
        team_name: The name of the team the context data to be loaded from (e.g. Barcelona, Real Madrid).
        season: The season from which the team context to be taken from
    Returns:
        Returns the loaded team context as a dictionary.
    """
    data = safe_call(
        lambda: client.team(team=team_name).get_context_data(season=season)
    )

    if data is None or len(data) == 0:
        logger.error(f'Failed: {team_name} ({season})')
        return None

    # Removing all of the formation type data, because it is varying amoung the teams, and it causes problems with the structure of the data!
    # The choice is made on my own responsibility that this data is not too important and influential enough for the predictive models, and it is acceptable at this moment to not be fetched! 
    data.pop("formation", None)

    data["team_name"] = team_name
    data["season"] = f"{season}/{season+1}"
    
    return data

def load_team_data(league: str, seasons: list) -> None:
    """
    Loads understat team context data.
    Saves the data into a csv file.
    Parameters:
        league: The name of the league (e.g. La Liga, Premier League).
        seasons: The range of seasons to data to be fatches from.
    Returns:
        None
    """
    file_path = f"{OUTPUT_DIR}teams_context.csv"
    failed_teams = []

    tasks = []

    for season in seasons:
        team_data = safe_call(lambda: client.league(league).get_team_data(season))

        if team_data is None or len(team_data) == 0:
            logger.error(f'Failed league data for season {season}')
            continue

        # Getting the names of the fetched teams, in order for the get_context_data to work properly!
        team_names = [data['title'] for data in team_data.values()]

        for team_name in team_names:
            tasks.append((team_name, season))

    print(f"Total team-season tasks: {len(tasks)}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(fetch_team_context, team, season): (team, season)
            for team, season in tasks
        }

        for future in as_completed(futures):
            team, season = futures[future]

            try:
                rows = future.result()
            except Exception as e:
                logger.error(f"Error: {team} ({season}) -> {e}")
                failed_teams.append((team, season))
                continue

            if not rows:
                logger.error(f'get_context_data: Something went wrong while fetching the context data for team {team} at season {season}')
                failed_teams.append((team, season))
                continue

            df_chunk = pd.json_normalize(rows, sep='_')

            if not df_chunk.empty:
                write_header = not os.path.exists(file_path)

                df_chunk.to_csv(
                    file_path,
                    mode="a",
                    header=write_header,
                    index=False
                )

    print(f"Failed teams: {len(failed_teams)}")
    if len(failed_teams) == 0:
        logger.info('All teams contexts have been loaded successfully!')


def fetch_match_shots(match_id) -> list:
    """
    Fetches macth shots data by a match id.
    Parameters:
        match_id: the id of the match from which the shot data to be taken from
    Returns:
        Returns the loaded matches shots as an array of shots data.
    """
    # Converting to string, becase the get_shot_data accepts the matche_id as a string.
    try:
        match_id = str(int(match_id))
    except Exception:
        logger.error(f"Invalid match_id: {match_id}")
        return None
    
    data = safe_call(
        lambda: client.match(match_id).get_shot_data()
    )
    
    if data is None or len(data) == 0:
        logger.error(f'get_shot_data: Something went wrong while fetching the shot data for match number {match_id}')
        return None

    rows = []
    
    for side in ["h", "a"]:
        for shot in data.get(side, []):
            shot_season = int(shot['season'])
            shot['season'] = f'{shot_season}/{shot_season+1}'
            rows.append(shot)
            
    return rows

def load_shot_data(matches_df: pd.DataFrame) -> None:
    """
    Loads understat matches shots data.
    Saves the data into a csv file.
    Parameters:
        matches_df: A pandas Data Frame containg the all of the matches from which the shot data to be taken from.
    Returns:
        None
    """
    match_ids = matches_df["id"].dropna().unique()
    
    file_path = f"{OUTPUT_DIR}matches_shots.csv"
    failed_matches = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_match_shots, mid): mid for mid in match_ids}

        for future in as_completed(futures):
            match_id = futures[future]
            
            try:
                rows = future.result()
            except Exception as e:
                logger.error(f"Error for match {match_id}: {e}")
                failed_matches.append(match_id)
                continue
            
            if not rows:
                failed_matches.append(match_id)
                logger.error(f'get_shot_data: Something went wrong while fetching the shot data for match number {match_id}')
                continue

            df_chunk = pd.json_normalize(rows, sep='_')

            if not df_chunk.empty:
                write_header = not os.path.exists(file_path)
                
                df_chunk.to_csv(
                    file_path,
                    mode="a",
                    header=write_header,
                    index=False
                )
        
    print(f"Failed matches: {len(failed_matches)}")
    if len(failed_matches) == 0:
        logger.info('All matches shots have been loaded successfully!')


def fetch_understat_data():
    matches_df = load_match_data(LEAGUE, SEASONS)
    
    load_player_data(LEAGUE, SEASONS)
    load_team_data(LEAGUE, SEASONS)
    load_shot_data(matches_df)

    print("ALL DATA COLLECTED SUCCESSFULLY")
