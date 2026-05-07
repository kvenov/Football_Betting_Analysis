import pandas as pd

from football_betting_analysis.config import START_SEASON, END_SEASON

seasons = [f"{str(year)}/{str(year+1)}" for year in range(START_SEASON, END_SEASON)]
SEASON_TEAM_MATCHES = 38

def validate_team_matches(data: pd.DataFrame, home_team_title: str, away_team_title: str) -> dict:
    """
        Validates if every team in each of the seasons from **2014/2015 to 2024/2025** has played a total of 38 matches

        Parameters:
            data : A pandas data frame, which contains the matches data
            home_team_title : The column name of the dataset home team
            away_team_title : The column name of the dataset away team
            
        Returns:
            A dictionary with keys: the seasons(from **2014/2015 to 2024/2025**) and with values: the amount of valid teams for the season!
            A valid team is considered as a team which has played a total of 38 matches for the specifis season!
    """
    
    valid_teams = {}
    for season in seasons:
        all_teams = data[data['season'] == season][home_team_title].unique()
        for team_name in all_teams:
            
            # Here we are checking taking the team home matches which are expected to be 19
            current_team_home_matches = data[
                (data[home_team_title] == team_name) &
                (data['season'] == season)
            ]
            
            # Here we are checking taking the team away matches which are expected to be 19
            current_team_away_matches = data[
                (data[away_team_title] == team_name) &
                (data['season'] == season)
            ]      
            
            # Total expected to be 38
            team_total_matches = len(current_team_home_matches) + len(current_team_away_matches)
            
            # Here for every team, if he has played a total of 38 matches is added to the dict of valid teams, which at the end is expected to contains 20 valid teams for every season!
            if team_total_matches == SEASON_TEAM_MATCHES:
                if season not in valid_teams.keys():
                    valid_teams[season] = 1
                else:
                    valid_teams[season] += 1
    
    return valid_teams
