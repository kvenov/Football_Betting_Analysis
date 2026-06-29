from dataclasses import dataclass
from typing import Dict

from football_betting_analysis.config import SOCCER_DATA_PATH, TRANSFERMARKT_DATA_PATH

@dataclass(frozen=True)
class Dataset:
    name: str
    url: str
    filename: str
    required_columns: tuple[str, ...] = ()


DATASETS: Dict[str, Dataset] = {
    "matches": Dataset(
        name="matches",
        filename=f"{SOCCER_DATA_PATH}matches.csv",
        url=(
            "https://storage.googleapis.com/bucket-football_matches_predictions-103ec46c-f533-43bd-b21/data/matches.csv"
        ),
        required_columns=(
            'league', 'season', 'team', 'game', 'date', 'time', 'round', 'day',
            'venue', 'result', 'GF', 'GA', 'opponent', 'Poss', 'Attendance',
            'Captain', 'Formation', 'Opp Formation', 'Referee', 'match_report',
            'Notes'
        )
    ),
    "appearances": Dataset(
        name="appearances",
        filename=f"{TRANSFERMARKT_DATA_PATH}appearances.csv",
        url=(
            "https://storage.googleapis.com/bucket-football_matches_predictions-103ec46c-f533-43bd-b21/transfermarkt/appearances.csv"
        ),
        required_columns=(
            'appearance_id', 'game_id', 'player_id', 'player_club_id',
            'player_current_club_id', 'date', 'player_name', 'competition_id',
            'yellow_cards', 'red_cards', 'goals', 'assists', 'minutes_played'
        )
    ),
    "competitions": Dataset(
        name="competitions",
        filename=f"{TRANSFERMARKT_DATA_PATH}competitions.csv",
        url=(
            "https://storage.googleapis.com/bucket-football_matches_predictions-103ec46c-f533-43bd-b21/transfermarkt/competitions.csv"
        ),
        required_columns=(
            'competition_id', 'competition_code', 'name', 'sub_type', 'type',
            'country_id', 'country_name', 'domestic_league_code', 'confederation',
            'total_clubs', 'url'
        )
    ),
    "game_lineups": Dataset(
        name="game_lineups",
        filename=f"{TRANSFERMARKT_DATA_PATH}game_lineups.csv",
        url=(
            "https://storage.googleapis.com/bucket-football_matches_predictions-103ec46c-f533-43bd-b21/transfermarkt/game_lineups.csv"
        ),
        required_columns=(
            'game_lineups_id', 'date', 'game_id', 'player_id', 'club_id',
            'player_name', 'type', 'position', 'number', 'team_captain'
        )
    ),
    "games": Dataset(
        name="games",
        filename=f"{TRANSFERMARKT_DATA_PATH}games.csv",
        url=(
            "https://storage.googleapis.com/bucket-football_matches_predictions-103ec46c-f533-43bd-b21/transfermarkt/games.csv"
        ),
        required_columns=(
            'game_id', 'competition_id', 'season', 'round', 'date', 'home_club_id',
            'away_club_id', 'home_club_goals', 'away_club_goals',
            'home_club_position', 'away_club_position', 'home_club_manager_name',
            'away_club_manager_name', 'stadium', 'attendance', 'referee', 'url',
            'home_club_formation', 'away_club_formation', 'home_club_name',
            'away_club_name', 'aggregate', 'competition_type'
        )
    ),
    "player_injuries": Dataset(
        name="player_injuries",
        filename=f"{TRANSFERMARKT_DATA_PATH}player_injuries.csv",
        url=(
            "https://storage.googleapis.com/bucket-football_matches_predictions-103ec46c-f533-43bd-b21/transfermarkt/player_injuries.csv"
        ),
        required_columns=(
            'player_id', 'season_name', 'injury_reason', 'from_date', 'end_date',
            'days_missed', 'games_missed'
        )
    ),
    "transfer_history": Dataset(
        name="transfer_history",
        filename=f"{TRANSFERMARKT_DATA_PATH}transfer_history.csv",
        url=(
            "https://storage.googleapis.com/bucket-football_matches_predictions-103ec46c-f533-43bd-b21/transfermarkt/transfer_history.csv"
        ),
        required_columns=(
            'player_id', 'season_name', 'transfer_date', 'from_team_id',
            'from_team_name', 'to_team_id', 'to_team_name', 'transfer_type',
            'value_at_transfer', 'transfer_fee'
        )
    )
}