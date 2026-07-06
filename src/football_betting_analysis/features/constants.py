# STADIUM CAPACITIES
# Official capacities of the clubs' primary La Liga stadiums

STADIUM_CAPACITIES = {
    "Malaga": 30044,                   
    "Sevilla": 43883,                   
    "Granada": 19336,                   
    "Almeria": 15274,                   
    "Eibar": 8164,                      
    "Barcelona": 99354,               
    "Celta Vigo": 29000,                
    "Levante": 26354,                   
    "Real Madrid": 81044,               
    "Rayo Vallecano": 14708,            
    "Getafe": 16800,                    
    "Valencia": 49430,                  
    "Cordoba": 21822,                  
    "Athletic Club": 53289,             
    "Atletico Madrid": 68456,           
    "Espanyol": 40500,                  
    "Villarreal": 23500,                
    "Deportivo La Coruna": 32660,       
    "Real Sociedad": 39500,             
    "Elche": 31388,                     
    "Sporting Gijon": 29538,            
    "Real Betis": 60721,                
    "Las Palmas": 32392,               
    "Osasuna": 23576,                  
    "Leganes": 12450,                   
    "Alaves": 19840,                 
    "Girona": 14500,                    
    "Real Valladolid": 27618,           
    "SD Huesca": 9100,                  
    "Mallorca": 23142,                  
    "Cadiz": 20724                      
}

FEATURE_GROUPS = {
 
    "G0_identifiers": [
        "game_id", "date", "time", "season", "round",
        "home_club_id", "away_club_id",
        "h_title", "a_title",
        "home_club_manager_name", "away_club_manager_name",
        "result_full"
    ],
 
    "G1_binary_flags": [
        # Squad availability flags
        "home_starting_goalkeeper_missing", "away_starting_goalkeeper_missing",
        "home_missing_captain", "away_missing_captain",
        "home_missing_top1_player", "away_missing_top1_player",
        # Match importance flags
        "home_title_race_flag", "away_title_race_flag",
        "home_cl_race_flag", "away_cl_race_flag",
        "home_el_race_flag", "away_el_race_flag",
        "home_relegation_flag", "away_relegation_flag",
        "home_important_match_ahead", "away_important_match_ahead",
        "home_cup_final_recent", "away_cup_final_recent",
        # Direct clash flags
        "title_direct_clash", "cl_direct_clash",
        "el_direct_clash",
        "relegation_direct_clash", "relegation_six_pointer",
        "direct_position_clash", "season_meaningful",
        # Formation / manager
        "home_using_new_formation", "away_using_new_formation",
        "home_manager_stable", "away_manager_stable",
        # Attendance
        "high_crowd_pressure",
    ],
 
    "G2_categorical": [
        "season", "h_title", "a_title",
        "home_formation", "away_formation",
        "result_full", "referee", "stadium",
        "home_club_position", "away_club_position"
    ],
 
    "G3_counts_integers": [
        # Missing players
        "home_missing_players_count", "away_missing_players_count",
        "home_missing_key_players_count", "away_missing_key_players_count",
        "home_missing_star_players_count", "away_missing_star_players_count",
        "home_missing_defenders", "away_missing_defenders",
        "home_missing_midfielders", "away_missing_midfielders",
        "home_missing_forwards", "away_missing_forwards",
        "home_missing_top3_player", "away_missing_top3_player",
        # Fixture counts
        "home_matches_last_7d", "away_matches_last_7d",
        "home_matches_last_14d", "away_matches_last_14d",
        "home_matches_played", "away_matches_played",
        # League position / spots
        "home_league_position", "away_league_position",
        "cl_spots", "el_spots", "conf_spots",
        # Formation changes
        "home_formation_changes_5", "away_formation_changes_5",
        "home_manager_matches_in_post", "away_manager_matches_in_post",
    ],
 
    "G4_bounded_ratios": [
        # Squad stability / importance ratios [0,1]
        "home_squad_stability", "away_squad_stability",
        "home_missing_importance_ratio", "away_missing_importance_ratio",
        # Pressure scores [0,1]
        "home_title_pressure", "away_title_pressure",
        "home_cl_pressure", "away_cl_pressure",
        "home_el_pressure", "away_el_pressure",
        "home_relegation_pressure", "away_relegation_pressure",
        "home_match_importance", "away_match_importance",
        # Form last 3/5 (should be [0,1] or bounded)
        "home_form_last_3", "home_form_last_5",
        "away_form_last_3", "away_form_last_5",
        # Formation consistency [0,1]
        "home_formation_consistency", "away_formation_consistency",
        # Manager win rate [0,1]
        "home_manager_win_rate", "away_manager_win_rate",
        # Season progress [0,1]
        "season_progress",
        # Congestion / rotation [0,1]
        "home_fixture_congestion_score", "away_fixture_congestion_score",
        "home_rotation_risk", "away_rotation_risk",
        # Attendance ratio [0,1]
        "attendance_ratio",
        "home_attendance_vs_avg",
    ],
 
    "G5_signed_gaps": [
        # Points gaps (can be negative = already in zone)
        "home_pts_behind_1st", "away_pts_behind_1st",
        "home_pts_to_cl", "away_pts_to_cl",
        "home_pts_to_el", "away_pts_to_el",
        "home_pts_to_conf", "away_pts_to_conf",
        "home_pts_above_relegation", "away_pts_above_relegation",
        # Cumulative stats
        "home_cum_points", "away_cum_points",
        "home_cum_gd", "away_cum_gd",
        # Diff features
        "elo_rating_diff", "position_gap", "points_gap_between_teams",
        # Days until next match
        "home_days_until_next_match", "away_days_until_next_match",
        "home_days_until_next_nonleague", "away_days_until_next_nonleague",
        "home_next_match_priority", "away_next_match_priority",
        "home_next_nonleague_priority", "away_next_nonleague_priority",
    ],
 
    "G6_rolling_form": [
        # Rolling last 5 — home
        "h_rolling_goals_5", "h_rolling_goals_against_5",
        "h_rolling_shots_5", "h_rolling_shots_against_5",
        "h_rolling_sot_5", "h_rolling_sot_against_5",
        "h_rolling_xG_5", "h_rolling_xGA_5",
        "h_rolling_goal_diff_5",
        "h_rolling_poss_5", "h_rolling_poss_against_5", "h_rolling_poss_diff_5",
        "h_rolling_big_chances_5", "h_rolling_avg_xG_per_shot_5",
        "h_rolling_shot_conversion_5",
        "h_rolling_open_play_xG_5", "h_rolling_set_piece_xG_5",
        # Rolling last 5 — away
        "a_rolling_goals_5", "a_rolling_goals_against_5",
        "a_rolling_shots_5", "a_rolling_shots_against_5",
        "a_rolling_sot_5", "a_rolling_sot_against_5",
        "a_rolling_xG_5", "a_rolling_xGA_5",
        "a_rolling_goal_diff_5",
        "a_rolling_poss_5", "a_rolling_poss_against_5", "a_rolling_poss_diff_5",
        "a_rolling_big_chances_5", "a_rolling_avg_xG_per_shot_5",
        "a_rolling_shot_conversion_5",
        "a_rolling_open_play_xG_5", "a_rolling_set_piece_xG_5",
        # H2H rolling
        "h_h2h_rolling_goal_diff_5", "h_h2h_rolling_xG_diff_5", "h_h2h_rolling_elo_diff_5",
        "a_h2h_rolling_goal_diff_5", "a_h2h_rolling_xG_diff_5", "a_h2h_rolling_elo_diff_5",
    ],
 
    "G7_odds": [
        "odds_bet365_home", "odds_bet365_draw", "odds_bet365_away",
        "handicap_home", "handicap_away",
    ],
 
    "G8_strength": [
        "home_elo", "away_elo",
        "home_available_strength", "away_available_strength",
        "home_missing_importance_sum", "away_missing_importance_sum",
        "home_missing_expected_starter_strength", "away_missing_expected_starter_strength",
        "home_missing_gk_strength", "away_missing_gk_strength",
        "home_missing_def_strength", "away_missing_def_strength",
        "home_missing_mid_strength", "away_missing_mid_strength",
        "home_missing_fwd_strength", "away_missing_fwd_strength",
        "home_missing_top3_player", "away_missing_top3_player",
    ],
 
    "G9_attendance": [
        "attendance", "attendance_num",
    ],
 
    "G10_referee_manager": [
        "referee_home_bias", "referee_foul_rate", "referee_card_rate",
        "home_manager_matches_in_post", "away_manager_matches_in_post",
        "home_manager_win_rate", "away_manager_win_rate",
    ],
}