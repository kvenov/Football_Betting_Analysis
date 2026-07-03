import pandas as pd
import numpy as np

from football_betting_analysis.features.constants import STADIUM_CAPACITIES

NEWCOMER_IMPORTANCE_THRESHOLD = 0.15 # min normalised score to include newcomer

NEWCOMER_VALUE_WINDOW_DAYS = 730 # +-days around transfer to compute club baseline
MAX_MEMBERSHIP_GAP_DAYS = 548 # fallback backstop: ~18 months after last appearance

NEWCOMER_STARTER_THRESHOLD = 0.60  # blended score above which -> expected_starter=1
NEWCOMER_KEY_THRESHOLD = 0.70 # normalised score above which -> is_key_player

NEWCOMER_HIST_WEIGHT = 0.60  # weight on historical importance 
NEWCOMER_VALUE_WEIGHT = 0.40  # weight on transfer-value signal

PHANTOM_CLUB_IDS = {515, 123, 75} # Without Club, Retired, Unknown

# Formation stability window: last 5 matches
FORMATION_WINDOW = 5

# Minimum number of matches a referee must have officiated for bias stats
# to be considered reliable - below this threshold, bias features go up to 0 (neutral)
REFEREE_MIN_MATCHES = 15

# Attendance "full house" threshold - crowd proportion above this = high pressure
ATTENDANCE_HIGH_THRESHOLD = 0.85

# Manager tenure threshold for "stable" regime (in matches managed)
MANAGER_STABLE_THRESHOLD = 38 # approximately one half-season

def create_season_feature(df: pd.DataFrame, date_str: str) -> pd.Series:
    """
    Creating a season feature, based on the **date** values of a data frame.
    
    Parameters:
        df(pd.DateFrame) : The data frame to which the season feature to be created
        date_str(str) : The name of date feature in the data frame
    
    Returns:
        pd.Series : The season feature represented as a string in format[**YY:YY+1**]
    """
    
    return np.where(
        df[date_str].dt.month >= 8,
        df[date_str].dt.year.astype(str) + '/' + (df[date_str].dt.year + 1).astype(str),
        (df[date_str].dt.year - 1).astype(str) + '/' + df[date_str].dt.year.astype(str)
    )
    
def calculate_rolling_metric(
    data: pd.DataFrame, 
    group_col: str, 
    column:str,
    func: str = 'mean',
    window=5
) -> pd.Series:
    """
    Calculate a rolling metric (e.g., mean, sum) of a column for each group.
    Avoids data leakage by shifting the values.
    
    Parameters:
        data (DataFrame): The input DataFrame
        group_col (str): The column by which to group the data
        column (str): The column to calculate the rolling metric for
        func (str): The function to be used for the calculation of the rolling metric(e.g. mean, sum)
        window (int): The number of games to include in the rolling average

    Returns:
        Series: The rolling metric
    """
    
    return data.groupby(group_col, observed=False)[column].transform(
        lambda x: x
                .shift(1)
                .rolling(window=window, min_periods=1)
                .agg(func.lower())
    )

def calculate_ewma_average(
    data: pd.DataFrame, 
    group_col: str, 
    column: str,
    span: int = 5,
    adjust: bool = False,
    min_periods: int = 1
) -> pd.Series:
    """
    Calculate an Exponentially Weighted Moving Average (EWMA) for each group.
    Shifts data by 1 row to prevent data leakage for the current match.
    
    Parameters:
        data (DataFrame): The input DataFrame
        group_col (str): The column by which to group the data
        column (str): The column to calculate the rolling metric for
        span (int): this parameter specifies the decay rate of the moving average, directly dictating how heavily past values influence the current value.
        adjust (bool): controls how weights are assigned to historical observations during the **early** periods of a calculation. 
            It determines if weights are normalized to account for the imbalance of fewer data points at the start of a series.
            The default value of the function is set to **False**, as that the influence of older data to decays exponentially ensuring that very past values does not have big affect over the final calculations!
        min_periods (int): this parameter specifies how many data points it needs to perform a calculation. If there are fewer than the specified number of points, the result will be NaN.

    Returns:
        Series: The Exponentially Weighted Moving Average of the specific column
    """
    return data.groupby(group_col, observed=False)[column].transform(
        lambda x: x.shift(1)
                   .ewm(span=span, 
                        min_periods=min_periods, 
                        adjust=adjust)
                   .mean()
    )

def calculate_time_based_ewma(
    data: pd.DataFrame, 
    group_col: str, 
    metric_col: str, 
    date_col: str,
    adjust: bool = False,
    halflife_days: int = 30,
    min_periods: int = 1
) -> pd.Series:
    """
    Calculates an Exponentially Weighted Moving Average (EWMA) for each group metric using Time-Based EWMA.
    
    Shifts data by 1 row to prevent data leakage for the current match.
    
    Parameters:
        data (DataFrame): The input DataFrame
        group_col (str): The column by which to group the data
        metric_col (str): The column with which to calculate the rolling metric
        date_col (str): The name of the date feature in the input dataframe
        adjust (bool): controls how weights are assigned to historical observations during the **early** periods of a calculation. 
            It determines if weights are normalized to account for the imbalance of fewer data points at the start of a series.
            The default value of the function is set to **False**, as that the influence of older data to decays exponentially ensuring that very past values does not have big affect over the final calculations!
        halflife_days (int): The number of days it takes for a performance's 
                        importance to be cut in half (e.g., 30 days).
        min_periods (int): this parameter specifies how many data points it needs to perform a calculation. If there are fewer than the specified number of points, the result will be NaN.

    Returns:
        Series: The Time based Exponentially Weighted Moving Average of the specific metric column.
    """
    # Ensuring the data is strictly sorted chronologically to prevent lookahead leakage
    df_sorted = data.sort_values(by=date_col)
    
    # Converting the date column to datetime if it isn't already
    times = pd.to_datetime(df_sorted[date_col])
    
    return df_sorted.groupby(group_col, observed=False).apply(
        lambda x: x[metric_col]
                   .shift(1)  # This ensures that the current match is not taken into the calculation of the metric
                   .ewm(halflife=pd.Timedelta(days=halflife_days), 
                        times=times.loc[x.index],
                        min_periods=min_periods,
                        adjust=adjust)
                   .mean()
    ).reset_index(level=0, drop=True)

def calculate_weighted_rolling_average(values, window=5) -> pd.Series:
    """
    Calculates weighted rolling average where recent matches matter more.

    Parameters:
        values (Series) : the column on which the rolling average to be calculated from
        window (int) : The max amount of past matches to include into the calculation of the rolling average!

    Returns:
        Series: The calculated rolling averages 
    
    Example weights for window=5:
    [1, 2, 3, 4, 5]

    Most recent match receives highest weight.
    """

    results = []

    for i in range(len(values)):

        # Exclude current match to prevent leakage
        historical = values.iloc[max(0, i-window):i]

        # No previous history
        if len(historical) == 0:
            results.append(np.nan)
            continue

        # Increasing weights:
        # older -> lower
        # recent -> higher
        weights = np.arange(1, len(historical) + 1)

        weighted_avg = np.average(
            historical,
            weights=weights
        )

        results.append(weighted_avg)

    return pd.Series(results, index=values.index)


def compute_formation_stability_features(
    df: pd.DataFrame, 
    window: int = FORMATION_WINDOW
) -> pd.DataFrame:
    """
    Compute formation stability features per team per match.
    Requires home_club_id, away_club_id, date, home_formation, away_formation.
    """
    
    # Build long-format formation history
    home_f = df[["game_id", "date", "home_club_id", "home_formation"]].rename(
        columns={"home_club_id": "team_id", "home_formation": "formation"}
    )
    away_f = df[["game_id", "date", "away_club_id", "away_formation"]].rename(
        columns={"away_club_id": "team_id", "away_formation": "formation"}
    )
    
    form_hist = pd.concat([home_f, away_f], ignore_index=True)
    form_hist = form_hist.sort_values(["team_id", "date"]).reset_index(drop=True)
    form_hist["formation"] = form_hist["formation"].fillna("Unknown")
 
    # Rolling window features per team
    results = []
    for team_id, grp in form_hist.groupby("team_id"):
        grp = grp.sort_values("date").reset_index(drop=True)
        formations = grp["formation"].values
        game_ids = grp["game_id"].values
        n = len(grp)
 
        changes_list = []
        modal_list = []
        new_form_list = []
 
        for i in range(n):
            # Only look at strictly PRIOR matches (no leakage)
            start = max(0, i - window)
            window_forms = formations[start:i] # prior to current
 
            if len(window_forms) == 0:
                changes_list.append(0)
                modal_list.append(formations[i])
                new_form_list.append(0)
                continue
 
            # Number of distinct formations in the window
            unique_forms = len(set(window_forms))
            changes = unique_forms - 1
            changes_list.append(changes)
 
            # Most common formation in window
            modal_form = pd.Series(window_forms).mode().iloc[0]
            modal_list.append(modal_form)
 
            # Is current formation different from the most common prior one?
            new_form_list.append(int(formations[i] != modal_form))
 
        grp["formation_changes"] = changes_list
        grp["modal_formation"] = modal_list
        grp["using_new_formation"] = new_form_list
        grp["formation_consistency"] = 1 - (grp["formation_changes"] / max(window - 1, 1))
        grp["formation_consistency"] = grp["formation_consistency"].clip(0, 1)
        results.append(grp[["game_id", "team_id", "formation_changes",
                              "formation_consistency", "using_new_formation"]])
 
    stability_df = pd.concat(results, ignore_index=True)
 
    # Pivot back to home/away
    home_stab = stability_df.merge(
        df[["game_id", "home_club_id"]].rename(columns={"home_club_id": "team_id"}),
        on=["game_id", "team_id"], how="inner"
    ).rename(columns={
        "formation_changes": "home_formation_changes_5",
        "formation_consistency": "home_formation_consistency",
        "using_new_formation": "home_using_new_formation",
    }).drop(columns="team_id")
 
    away_stab = stability_df.merge(
        df[["game_id", "away_club_id"]].rename(columns={"away_club_id": "team_id"}),
        on=["game_id", "team_id"], how="inner"
    ).rename(columns={
        "formation_changes": "away_formation_changes_5",
        "formation_consistency": "away_formation_consistency",
        "using_new_formation": "away_using_new_formation",
    }).drop(columns="team_id")
 
    df = df.merge(home_stab, on="game_id", how="left")
    df = df.merge(away_stab, on="game_id", how="left")
    
    return df


def compute_referee_bias_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute referee bias features.
    Uses only matches prior to the current one (no leakage).
    Requires: referee, home_goals_full, away_goals_full, home_fouls, away_fouls,
              home_yellow_cards, away_yellow_cards, home_red_cards, away_red_cards and date.
    """
    df = df.copy()
    df = df.sort_values("date").reset_index(drop=True)
    
    df["_result_home_win"] = (df["home_goals_full"] > df["away_goals_full"]).astype(int)
    df["_total_fouls"] = df["home_fouls"].fillna(0) + df["away_fouls"].fillna(0)
    df["_total_yellows"] = df["home_yellow_cards"].fillna(0) + df["away_yellow_cards"].fillna(0)
    df["_total_reds"] = df["home_red_cards"].fillna(0) + df["away_red_cards"].fillna(0)
 
    # League-wide averages (used for normalisation)
    league_hw_rate = df["_result_home_win"].mean()
    league_foul_avg = df["_total_fouls"].mean()
    league_yelow_card_avg = df["_total_yellows"].mean()
    league_red_card_avg = df["_total_reds"].mean()
    league_card_avg = league_yelow_card_avg + league_red_card_avg
 
    ref_home_bias_list = []
    ref_foul_rate_list = []
    ref_card_rate_list = []
 
    for idx, row in df.iterrows():
        ref = row["referee"]
        if pd.isna(ref) or ref == "":
            ref_home_bias_list.append(0.0)
            ref_foul_rate_list.append(0.0)
            ref_card_rate_list.append(0.0)
            continue
 
        # Only use matches strictly before this one
        prior = df[(df["referee"] == ref) & (df["date"] < row["date"])]
 
        if len(prior) < REFEREE_MIN_MATCHES:
            ref_home_bias_list.append(0.0)
            ref_foul_rate_list.append(0.0)
            ref_card_rate_list.append(0.0)
            continue
 
        hw_rate = prior["_result_home_win"].mean()
        foul_rate = prior["_total_fouls"].mean()
        card_rate = prior["_total_yellows"].mean() + prior["_total_reds"].mean()
 
        # Bias = deviation from league average (zero-centred)
        ref_home_bias_list.append(round(hw_rate - league_hw_rate, 4))
        ref_foul_rate_list.append(round(foul_rate / max(league_foul_avg, 1), 4))
        ref_card_rate_list.append(round(card_rate / max(league_card_avg, 1), 4))
 
    df["referee_home_bias"] = ref_home_bias_list
    df["referee_foul_rate"] = ref_foul_rate_list
    df["referee_card_rate"] = ref_card_rate_list
 
    df = df.drop(columns=["_result_home_win", "_total_fouls", "_total_yellows", "_total_reds"])
    return df


def compute_attendance_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute attendance-based pressure features.
    Requires: attendance, home_club_name, home_match_importance (from match importance section),
              away_relegation_pressure (from match importance section).
    """
    df = df.copy()
 
    df["_capacity"] = df["h_title"].map(STADIUM_CAPACITIES).fillna(30000).astype(float)
    df["attendance_num"] = pd.to_numeric(df["attendance"], errors="coerce").fillna(0).astype(float)
 
    df["attendance_ratio"] = (
        df["attendance_num"] / df["_capacity"]
    ).clip(0, 1.0)
 
    df["high_crowd_pressure"] = (
        df["attendance_ratio"] >= ATTENDANCE_HIGH_THRESHOLD
    ).astype(int)
 
    # Team avg attendance (rolling, strictly prior matches)
    home_att = df[["game_id", "date", "home_club_id", "attendance_num"]].rename(
        columns={"home_club_id": "team_id"}
    )
    away_att = df[["game_id", "date", "away_club_id", "attendance_num"]].rename(
        columns={"away_club_id": "team_id"}
    )
    all_att = pd.concat([home_att, away_att], ignore_index=True).sort_values(["team_id", "date"])
 
    all_att["team_avg_att_prior"] = (
        all_att.groupby("team_id")["attendance_num"]
        .transform(lambda s: s.shift(1).expanding().mean())
    )
    all_att["attendance_vs_avg"] = (
        all_att["attendance_num"] / all_att["team_avg_att_prior"].replace(0, np.nan)
    ).fillna(1.0).clip(0, 3.0)
 
    home_avg = all_att.merge(
        df[["game_id", "home_club_id"]].rename(columns={"home_club_id": "team_id"}),
        on=["game_id", "team_id"]
    ).rename(columns={"attendance_vs_avg": "home_attendance_vs_avg"})[["game_id", "home_attendance_vs_avg"]]
 
    df = df.merge(home_avg, on="game_id", how="left")
    df["home_attendance_vs_avg"] = df["home_attendance_vs_avg"].fillna(1.0)
 
    df = df.drop(columns=["_capacity"])
    return df


def compute_manager_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute manager tenure and stability features.
    Manager changes are detected when home_club_manager_name changes between
    consecutive matches for the same team.
    Requires: game_id, date, home_club_id, away_club_id,
              home_club_manager_name, away_club_manager_name,
              home_goals_full, away_goals_full.
    """
    df = df.copy()
 
    # Build long-format manager history
    home_m = df[["game_id", "date", "home_club_id", "home_club_manager_name",
                  "home_goals_full", "away_goals_full"
                ]].rename(columns={
                    "home_club_id": "team_id", "home_club_manager_name": "manager",
                    "home_goals_full": "gf", "away_goals_full": "ga",
                })
    home_m["win"] = (home_m["gf"] > home_m["ga"]).astype(int)
 
    away_m = df[["game_id", "date", "away_club_id", "away_club_manager_name",
                  "away_goals_full", "home_goals_full"
                ]].rename(columns={
                    "away_club_id": "team_id", "away_club_manager_name": "manager",
                    "away_goals_full": "gf", "home_goals_full": "ga",
                })
    away_m["win"] = (away_m["gf"] > away_m["ga"]).astype(int)
 
    mgr_hist = pd.concat([home_m, away_m], ignore_index=True)
    mgr_hist = mgr_hist.sort_values(["team_id", "date"]).reset_index(drop=True)
 
    results = []
    for team_id, grp in mgr_hist.groupby("team_id"):
        grp = grp.sort_values("date").reset_index(drop=True)
        managers = grp["manager"].values
        wins = grp["win"].values
 
        matches_in_post = []
        win_rates = []
 
        current_mgr = None
        stint_start_idx = 0
 
        for i in range(len(grp)):
            mgr = managers[i]
            if mgr != current_mgr:
                current_mgr = mgr
                stint_start_idx = i
 
            # Matches in current stint before this match
            stint_matches_prior = i - stint_start_idx # 0 on first match of new manager
            matches_in_post.append(stint_matches_prior)
 
            # Win rate in current stint (prior matches only)
            if stint_matches_prior > 0:
                stint_wins = wins[stint_start_idx:i]
                win_rates.append(round(stint_wins.mean(), 4))
            else:
                win_rates.append(np.nan) # no prior data yet for this manager
 
        grp["manager_matches_in_post"] = matches_in_post
        grp["manager_win_rate"] = win_rates
        results.append(grp[["game_id", "team_id",
                              "manager_matches_in_post", "manager_win_rate"]])
 
    mgr_df = pd.concat(results, ignore_index=True)
    mgr_df["manager_stable"] = (
        mgr_df["manager_matches_in_post"] >= MANAGER_STABLE_THRESHOLD
    ).astype(int)
    mgr_df["manager_win_rate"] = mgr_df["manager_win_rate"].fillna(0.33) # prior neutral
 
    # Merge home
    df = df.merge(
        mgr_df[mgr_df["game_id"].isin(df["game_id"])].merge(
            df[["game_id", "home_club_id"]].rename(columns={"home_club_id": "team_id"}),
            on=["game_id", "team_id"]
        ).rename(columns={
            "manager_matches_in_post": "home_manager_matches_in_post",
            "manager_stable": "home_manager_stable",
            "manager_win_rate": "home_manager_win_rate",
        }).drop(columns="team_id"),
        on="game_id", how="left"
    )
    
    # Merge away
    df = df.merge(
        mgr_df[mgr_df["game_id"].isin(df["game_id"])].merge(
            df[["game_id", "away_club_id"]].rename(columns={"away_club_id": "team_id"}),
            on=["game_id", "team_id"]
        ).rename(columns={
            "manager_matches_in_post": "away_manager_matches_in_post",
            "manager_stable": "away_manager_stable",
            "manager_win_rate": "away_manager_win_rate",
        }).drop(columns="team_id"),
        on="game_id", how="left"
    )
 
    # Default fills
    for side in ["home", "away"]:
        df[f"{side}_manager_matches_in_post"] = df[f"{side}_manager_matches_in_post"].fillna(0).astype(int)
        df[f"{side}_manager_stable"] = df[f"{side}_manager_stable"].fillna(0).astype(int)
        df[f"{side}_manager_win_rate"] = df[f"{side}_manager_win_rate"].fillna(0.33)
 
    return df


def create_team_squad(
    player_snapshot: pd.DataFrame, 
    tenure_table: pd.DataFrame,
    transfers_clean: pd.DataFrame,
    team_matches: pd.DataFrame,
    club_history: pd.DataFrame,
    player_matches: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
        Creates the team's squads, using player's appearances, transfers and teams matches data.
        
        Parameters:
            player_snapshot(pd.DataFrame) : The player's appearances dataset
            tenure_table(pd.DataFrame) : The player's transfers in format 
                **[player_id, team_id, joined_date, left_date]**
            transfers_clean(pd.DataFrame) : The player's full transfer history in format:
                **[player_id, from_team_id, to_team_id, transfer_date]**
            team_matches(pd.DataFrame) : The team's full matches history!
            club_history(pd.DataFrame) : The players full appearance history for all of the teams that they have played.
            player_matches(pd.DataFrame) : The full history of appearances for all of the players.
                This dataset will be used to define the long term quanlity of the player as his 
                importance score relative to his club's other players scores
         
        Returns:
            team_match_players(pd.DataFrame) : The final full team squad!
        
    """
    # All tenure records:
    all_tenure_pairs = tenure_table[["player_id", "club_id"]].drop_duplicates()
    
    # All appearances records:
    appearance_pairs = player_snapshot[["player_id", "player_club_id"]].rename(
        columns={"player_club_id": "club_id"}
    ).drop_duplicates()
    
    # Identify players who have a tenure record but no prior appearance
    # Players in tenure but with NO appearance at that specific club -> newcomers
    newcomer_pairs = all_tenure_pairs.merge(
        appearance_pairs,
        on=["player_id", "club_id"],
        how="left",
        indicator=True,
    ).query('_merge == "left_only"')[["player_id", "club_id"]]
    
    # Pull the most recent transfer record for each newcomer (player, club)
    transfers_for_newcomers = (
        transfers_clean[
            transfers_clean["player_id"].isin(newcomer_pairs["player_id"])
            & transfers_clean["to_team_id"].isin(newcomer_pairs["club_id"])
        ]
        .merge(newcomer_pairs.rename(columns={"club_id": "to_team_id"}),
            on=["player_id", "to_team_id"])
        .copy()
    )
    
    # Primary value signal: value_at_transfer, fallback to transfer_fee
    transfers_for_newcomers["value_signal"] = (
        transfers_for_newcomers["value_at_transfer"]
        .fillna(transfers_for_newcomers["transfer_fee"])
        .fillna(0.0)
    )
    
    transfers_for_newcomers = transfers_for_newcomers[
        transfers_for_newcomers["value_signal"] > 0
    ].copy()
    
    # Keep only the most recent transfer per (player, club)
    transfers_for_newcomers = (
        transfers_for_newcomers
        .sort_values(["player_id", "to_team_id", "transfer_date"])
        .groupby(["player_id", "to_team_id"], as_index=False)
        .last()
    )
    
    # Compute per-club baseline for value normalisation
    all_valued_transfers = transfers_clean.copy()
    all_valued_transfers["value_signal_pool"] = (
        all_valued_transfers["value_at_transfer"]
        .fillna(all_valued_transfers["transfer_fee"])
        .fillna(0.0)
    )
    all_valued_transfers = all_valued_transfers[
        all_valued_transfers["value_signal_pool"] > 0
    ][["to_team_id", "transfer_date", "value_signal_pool"]].copy()
    
    window = pd.Timedelta(days=NEWCOMER_VALUE_WINDOW_DAYS)
    
    newcomer_with_window = transfers_for_newcomers[
        ["player_id", "to_team_id", "transfer_date", "value_signal"]
    ].merge(
        all_valued_transfers.rename(columns={
            "transfer_date": "pool_date",
            "value_signal_pool": "pool_value",
        }),
        on="to_team_id",
        how="left",
    )
    
    newcomer_with_window = newcomer_with_window[
        (newcomer_with_window["pool_date"] >= newcomer_with_window["transfer_date"] - window)
        & (newcomer_with_window["pool_date"] <= newcomer_with_window["transfer_date"] + window)
    ]
    
    club_window_max = (
        newcomer_with_window
        .groupby(["player_id", "to_team_id"])["pool_value"]
        .max()
        .rename("club_window_max")
        .reset_index()
    )
    
    transfers_for_newcomers = transfers_for_newcomers.merge(
        club_window_max, on=["player_id", "to_team_id"], how="left"
    )
    
    # Transfer-value component: normalised relative to club's own spending window
    transfers_for_newcomers["value_importance"] = (
        transfers_for_newcomers["value_signal"]
        / transfers_for_newcomers["club_window_max"].replace(0, np.nan)
    ).clip(0.0, 1.0).fillna(0.0)
    
    # Defining the player's long-term importance scores using their full
    # appearance history from their previous clubs:
    player_lt = calculate_player_importance(player_matches, transfers_for_newcomers, transfers_clean)
    
    # Aattach the results back to transfers_for_newcomers:
    transfers_for_newcomers = transfers_for_newcomers.merge(
        player_lt[[
            "player_id", "to_team_id",
            "hist_importance", "last_position_group",
        ]],
        on=["player_id", "to_team_id"],
        how="left",
    )
    
    transfers_for_newcomers["hist_importance"] = (
        transfers_for_newcomers["hist_importance"].fillna(0.0)
    )
    
    # Position: taken from last appearance at the previous club (stable, form-independent)
    # Fallback "MID" only for players with zero career history anywhere
    transfers_for_newcomers["hist_position_group"] = (
        transfers_for_newcomers["last_position_group"].fillna("MID")
    )
    
    # Blended importance score
    # When historical importance exists: importance_score * 0.60 + value_importance * 0.40
    # When hist = 0 (no prior history): fall back to value_importance alone
    # so truly debut players are still represented by their transfer value.
    transfers_for_newcomers["has_history"] = (
        transfers_for_newcomers["hist_importance"] > 0
    ).astype(int)
    
    # Defining the player's final importance score as a result 
    # from their long-term contribution to their previous clubs and the
    # value_importance which is the value from their transfer calulated using player market value
    # and the transfer_fee as fallback and the result is based on the relative team transfers(using the max value of their transfers!) 
    transfers_for_newcomers["importance_score"] = np.where(
        transfers_for_newcomers["has_history"] == 1,
        (NEWCOMER_HIST_WEIGHT * transfers_for_newcomers["hist_importance"]
        + NEWCOMER_VALUE_WEIGHT * transfers_for_newcomers["value_importance"]),
        transfers_for_newcomers["value_importance"], # pure value signal for debut players
    ).clip(0.0, 1.0)
    
    # Apply significance threshold
    transfers_for_newcomers = transfers_for_newcomers[
        transfers_for_newcomers["importance_score"] >= NEWCOMER_IMPORTANCE_THRESHOLD
    ].copy()
    
    # Gathering the player's position from his last appearance at previous club
    # hist_position_group was set above from the player's last recorded appearance
    # at their previous club
    # The Fallback "MID" for players with zero career history anywhere, has already been applied!
    transfers_for_newcomers["position_group"] = (
        transfers_for_newcomers["hist_position_group"]
    )
    
    # Expected_starter derived from blended importance
    # expected_starter=0 for everyone is wrong for elite newcomers.
    # A player with blended importance >= NEWCOMER_STARTER_THRESHOLD has the
    # quality profile of a regular starter - treat them as one.
    # This mirrors how start_share >= 0.60 works for appearance-based players.
    transfers_for_newcomers["expected_starter"] = (
        transfers_for_newcomers["importance_score"] >= NEWCOMER_STARTER_THRESHOLD
    ).astype(int)
    
    # Build synthetic snapshot rows:
    newcomer_snapshots = pd.DataFrame({
        "player_id": transfers_for_newcomers["player_id"].values,
        "player_club_id": transfers_for_newcomers["to_team_id"].values,
        "date": transfers_for_newcomers["transfer_date"].values,
        "importance_score": transfers_for_newcomers["importance_score"].values,
        "position_group": transfers_for_newcomers["position_group"].values,
        "captain_flag": 0,
        "is_star_player": 0,
        "is_key_player": (
            transfers_for_newcomers["importance_score"].values >= NEWCOMER_KEY_THRESHOLD
        ).astype(int),
        "expected_starter": transfers_for_newcomers["expected_starter"].values,
        "is_newcomer": 1,
    })
    
    # Tag existing snapshot rows so we can distinguish them later
    player_snapshot["is_newcomer"] = 0
    
    # Merge synthetic rows into player_snapshot.
    # merge_asof backward will always prefer a real appearance row over a synthetic
    # one once the player has debuted — synthetic rows are only active pre-debut.
    player_snapshot_extended = pd.concat(
        [player_snapshot, newcomer_snapshots],
        ignore_index=True,
    ).sort_values(["player_id", "date"]).reset_index(drop=True)
    
    # Update the scoreable set to include newcomers who now have a synthetic row
    scoreable_players_extended = frozenset(player_snapshot_extended["player_id"].unique())
    
    # Update tenure filter to use the extended scoreable set
    tenure_table_scoreable = tenure_table[
        tenure_table["player_id"].isin(scoreable_players_extended)
    ].copy()
    
    # Pre-compute loan exclusion table (used by both paths)
    # Built from the FULL tenure_table (not the scoreable subset) because a
    # non-scoreable player on loan must still block fallback-path players at the
    # parent club from incorrectly appearing in the squad.
    loan_windows = tenure_table[tenure_table["tenure_type"] == "loan"][
        ["player_id", "joined_date", "left_date"]
    ].copy()
    
    # PATH A: Craete the team's PRIMARY squad (transfer-record players):
    primary_candidates = create_team_primary_squad(team_matches, tenure_table_scoreable)
    
    # PATH B: FALLBACK (players with appearances but no transfer record)
    fallback_candidates = create_team_fallback_squad( \
        team_matches, tenure_table_scoreable, club_history,
        scoreable_players_extended, loan_windows
    )
    
    # COMBINE both paths to form the full team squad!
    team_match_players = (
        pd.concat([primary_candidates, fallback_candidates], ignore_index=True)
        .drop_duplicates(subset=["game_id", "player_id"])
        .sort_values(["game_id", "club_id", "player_id"])
        .reset_index(drop=True)
    )
    
    return team_match_players, player_snapshot_extended


def create_team_primary_squad(
    team_matches: pd.DataFrame,
    tenure_table_scoreable: pd.DataFrame
) -> pd.DataFrame:
    """
        Creates the team primary squad using the team's matches and player's tenures datasets.
        The team primary squad refers to the players which have appearances in the team matches and
        also have a recorded transfer for the team.
        
        Parameters:
            player_matches(pd.DataFrame) : The full history of appearances for all of the players.
                This dataset will be used to define the long term quanlity of the player as his 
                importance score relative to his club's other players scores
            tenure_table_scoreable(pd.DataFrame) : The tenure records to all of the players which have
                appeared in their current team's matches and also have a trasnfer to this team!
        
        Returns:
            primary_candidates(pd.DataFrame) : The team primary squad!
    """
    
    # PATH A: PRIMARY (transfer-record players)
    # Step 1 — cross-join tenure windows onto the matches that share the same club_id.
    # Uses tenure_table_scoreable so players with no snapshot history are
    # excluded before they generate any candidate rows.
    primary_candidates = team_matches[["game_id", "date", "club_id"]].merge(
        tenure_table_scoreable[["player_id", "club_id", "joined_date", "left_date"]],
        on="club_id",
        how="inner",
    )
    
    # Step 2 — keep only rows where the match date falls inside the tenure window.
    # NaT left_date means the window is still open → player is eligible.
    primary_candidates = primary_candidates[
        (primary_candidates["joined_date"] <= primary_candidates["date"])
        & (
            primary_candidates["left_date"].isna()
            | (primary_candidates["left_date"] >= primary_candidates["date"])
        )
    ][["game_id", "date", "club_id", "player_id"]].drop_duplicates()
    
    return primary_candidates


def create_team_fallback_squad(
    team_matches: pd.DataFrame,
    tenure_table_scoreable: pd.DataFrame,
    club_history: pd.DataFrame,
    scoreable_players_extended: frozenset,
    loan_windows: pd.DataFrame
) -> pd.DataFrame:
    """
        Parameters:
            team_matches(pd.DataFrame) : All of the team matches.
                This dataset will be used to define the long term quanlity of the player as his 
                importance score relative to his club's other players scores
            tenure_table_scoreable(pd.DataFrame) : The tenure records to all of the players which have
                appeared in their current team's matches and also have a trasnfer to this team!
            club_history(pd.DataFrame) : The players full appearance history for all of the teams that they have played.
            scoreable_players_extended(frozenset) : The list of all player's ids, which either have 
                been played for the team(has appearance for the team) or have been included as an important
                player, based on the player's transfer!
            loan_windows(pd.DataFrame) : The player's transfers which have recorded as loans(not a standard transfer)

        Returns:
            fallback_candidates(pd.DataFrame) : The team's fallback squad!
    """
    
    # Build a fallback tenure table: one row per (player_id, club_id).
    # Uses tenure_table_scoreable for the "already has a primary record"
    # exclusion so the two paths remain non-overlapping.
    # club_history is pre-filtered to scoreable players only.
    players_with_tenure_per_club = (
        tenure_table_scoreable[["player_id", "club_id"]]
        .drop_duplicates()
    )
    
    club_history_scoreable = club_history[
        club_history["player_id"].isin(scoreable_players_extended)
    ].copy()
    
    all_appearances = (
        club_history_scoreable[["player_id", "club_id"]]
        .drop_duplicates()
    )
    
    # Keep only appearance pairs that have NO tenure record for that (player, club)
    fallback_pairs = all_appearances.merge(
        players_with_tenure_per_club,
        on=["player_id", "club_id"],
        how="left",
        indicator=True,
    ).query('_merge == "left_only"')[["player_id", "club_id"]]
    
    # Compute first / last appearance per (player, club) in one groupby
    fallback_bounds = (
        club_history_scoreable[["player_id", "club_id", "date"]]
        .merge(fallback_pairs, on=["player_id", "club_id"], how="inner")
        .groupby(["player_id", "club_id"])["date"]
        .agg(first_app="min", last_app="max")
        .reset_index()
    )
    fallback_bounds["effective_left"] = (
        fallback_bounds["last_app"] + pd.Timedelta(days=MAX_MEMBERSHIP_GAP_DAYS)
    )
    
    # Step 2 — fan fallback bounds out to all matches for the same club
    fallback_candidates = team_matches[["game_id", "date", "club_id"]].merge(
        fallback_bounds[["player_id", "club_id", "first_app", "effective_left"]],
        on="club_id",
        how="inner",
    )
    
    # Step 3 — keep only rows where match_date falls inside the fallback window
    fallback_candidates = fallback_candidates[
        (fallback_candidates["first_app"] <= fallback_candidates["date"])
        & (fallback_candidates["effective_left"] >= fallback_candidates["date"])
    ][["game_id", "date", "club_id", "player_id"]].drop_duplicates()
    
    # Step 4 - exclude the players which have been on loan at time of the matches:
    fallback_candidates = execute_loan_window_exclusion(loan_windows, fallback_candidates)
    
    return fallback_candidates


def execute_loan_window_exclusion(
    loan_windows: pd.DataFrame,
    fallback_candidates: pd.DataFrame,
) -> pd.DataFrame:
    """
        Loan exclusion for fallback players.Removes the players which have been on loan 
        at the time of the matches.These players will not be included into the team pre-match squad!
        
        Parameters:
            loan_windows(pd.DataFrame) : The player's transfers which have recorded as loans(not a standard transfer)
            fallback_candidates(pd.DataFrame) : The team's fallback squad 
        
        Returns:
            fallback_candidates(pd.DataFrame) : The updates fallback team squad without and loaned players! 
    """
    
    if len(loan_windows) > 0 and len(fallback_candidates) > 0:
    
        loan_check = fallback_candidates[["game_id", "date", "player_id"]].merge(
            loan_windows, on="player_id", how="left",
        )
    
        loan_check["on_loan"] = (
            loan_check["joined_date"].notna()
            & (loan_check["joined_date"] <= loan_check["date"])
            & (
                loan_check["left_date"].isna()
                | (loan_check["left_date"] >= loan_check["date"])
            )
        )
    
        loaned_out = (
            loan_check[loan_check["on_loan"]][["game_id", "player_id"]]
            .drop_duplicates()
        )
    
        fallback_candidates = fallback_candidates.merge(
            loaned_out.assign(_loaned=True), on=["game_id", "player_id"], how="left",
        )
        fallback_candidates = fallback_candidates[
            fallback_candidates["_loaned"].isna()
        ][["game_id", "date", "club_id", "player_id"]]
    
    return fallback_candidates


def calculate_player_importance(
    player_matches: pd.DataFrame,
    transfers_for_newcomers: pd.DataFrame,
    transfers_clean: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute a stable long-term quality score for each player at each
    previous club using their FULL appearance history there, then normalise it
    against ALL players who appeared for that same club over the same period.
    This produces a peer-relative percentile that is:
      - Long-term stable (not form-dependent)
      - Cross-club comparable (percentile 0–1 means the same regardless of club)
      - Multi-dimensional (combines minutes, starts, and production rate)

    Components (all computed over the player's entire stint at the club,
    restricted to appearances strictly before the transfer date):
      minutes_share_mean - long-term average share of available minutes played
      start_share_mean - long-term average rate of starting appearances
      contribution_rate - goals_per90 + assists_per90, averaged over the stint
      importance_score_mean - mean of the per-match importance_score across stint(averages out form spikes; more stable than last value)
      
    Parameters:
        player_matches(pd.DataFrame) : The full history of appearances for all of the players.
            This dataset will be used to define the long term quanlity of the player as his 
            importance score relative to his club's other players scores
        transfers_for_newcomers(pd.DataFrame) : The transfers of the newcomers.
            This dataset alongside with the transfers_clean will be used to get the previous club 
            of the newcomers!
        transfers_clean(pd.DataFrame) : The full history of all player's transfers.
        
    Returns:
        player_lt(pd.DataFrame) : The player's long_term historical importance score!
    """
    
    raw_history = player_matches[[
        "player_id", "player_club_id", "date",
        "minutes_share", "is_starter",
        "goals_per90", "assists_per90",
        "contribution_score", "importance_score",
        "position_group",
    ]].copy()
    
    raw_history = raw_history.rename(columns={"player_club_id": "prev_club_id"})
    
    # For each newcomer, identify (player_id, previous_club, transfer_date)
    # previous club = from_team_id on the transfer record
    newcomer_prev_club = transfers_for_newcomers[[
        "player_id", "to_team_id", "transfer_date"
    ]].merge(
        transfers_clean[["player_id", "to_team_id", "from_team_id", "transfer_date"]],
        on=["player_id", "to_team_id", "transfer_date"],
        how="left",
    ).rename(columns={"from_team_id": "prev_club_id"})
    
    # Drop rows where the previous club is a phantom (retired, without club, unknown)
    newcomer_prev_club = newcomer_prev_club[
        newcomer_prev_club["prev_club_id"].notna()
        & ~newcomer_prev_club["prev_club_id"].isin(PHANTOM_CLUB_IDS)
    ].copy()
    
    # Join appearance history to each newcomer (player x prev_club)
    # Keep only appearances at the recorded previous club strictly before transfer
    newcomer_history = newcomer_prev_club.merge(
        raw_history,
        on=["player_id", "prev_club_id"],
        how="left",
    )
    
    newcomer_history = newcomer_history[
        newcomer_history["date"] < newcomer_history["transfer_date"]
    ].copy()
    
    # Compute long-term per-player metrics at the previous club
    player_lt = (
        newcomer_history
        .groupby(["player_id", "to_team_id", "prev_club_id"])
        .agg(
            appearances = ("date", "count"),
            minutes_share_mean = ("minutes_share", "mean"),
            start_share_mean = ("is_starter", "mean"),
            contribution_rate = ("contribution_score", "mean"),
            importance_mean = ("importance_score", "mean"),
            last_position_group = ("position_group", "last"),  # most recent position
            last_app_date = ("date", "max"),
        )
        .reset_index()
    )
    
    # Require a minimum number of appearances for the score to be reliable.
    # Players with fewer than 5 appearance get hist_importance=0 (-> value-only blend).
    MIN_APPEARANCES = 5
    player_lt["enough_history"] = (player_lt["appearances"] >= MIN_APPEARANCES).astype(int)
    
    # Raw composite score (equal-weight average of the four components, all already ~[0,1])
    player_lt["composite_raw"] = (
        player_lt["minutes_share_mean"]
        + player_lt["start_share_mean"]
        + player_lt["contribution_rate"].clip(0, 1)    # cap outlier scorers at 1
        + player_lt["importance_mean"]
    ) / 4.0
    
    # Peer-normalise within each previous club
    # For each previous club, collect ALL players who appeared there and compute
    # the same composite, then express each newcomer's score as a percentile.
    # This makes a 0.8 mean "top 80% of all players at that club" regardless of
    # the club's overall quality level.
    
    # All players at each of the relevant previous clubs (full history, no date filter)
    all_prev_clubs = player_lt["prev_club_id"].unique()
    
    peer_pool = (
        raw_history[raw_history["prev_club_id"].isin(all_prev_clubs)]
        .groupby(["player_id", "prev_club_id"])
        .agg(
            appearances = ("date", "count"),
            minutes_share_mean = ("minutes_share", "mean"),
            start_share_mean = ("is_starter", "mean"),
            contribution_rate = ("contribution_score","mean"),
            importance_mean = ("importance_score", "mean"),
        )
        .reset_index()
    )
    
    peer_pool["composite_raw"] = (
        peer_pool["minutes_share_mean"]
        + peer_pool["start_share_mean"]
        + peer_pool["contribution_rate"].clip(0, 1)
        + peer_pool["importance_mean"]
    ) / 4.0
    
    # Percentile rank within each previous club (0 = worst, 1 = best)
    peer_pool["peer_percentile"] = (
        peer_pool
        .groupby("prev_club_id")["composite_raw"]
        .rank(pct=True, method="average")
    )
    
    # Attach each newcomer's percentile rank using their own composite_raw
    player_lt = player_lt.merge(
        peer_pool[["player_id", "prev_club_id", "peer_percentile"]],
        on=["player_id", "prev_club_id"],
        how="left",
    )
    
    # Final hist_importance: percentile rank where history is sufficient, else 0
    player_lt["hist_importance"] = np.where(
        player_lt["enough_history"] == 1,
        player_lt["peer_percentile"].fillna(0.0),
        0.0,
    )
    
    return player_lt
