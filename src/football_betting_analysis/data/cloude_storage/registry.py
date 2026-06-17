from dataclasses import dataclass
from typing import Dict

from football_betting_analysis.config import SOCCER_DATA_PATH

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
        ),
    )
}