import pandas as pd

def resolve_date_shift_matches(
        left_df, 
        right_df, 
        use_index, 
        left_index_col=None, 
        right_index_col=None, 
        max_days=1) -> pd.DataFrame:
    """
        Matches football games with small date inconsistencies.

        Conditions:
        - same season
        - same home team
        - same away team
        - date difference <= max_days
        
        Parameters:
            left_df : The left unmatching data frame
            right_df : The right unmatching data frame
            use_index : An idenifier which determenes if there is a specifix column which should be used a the index of the dataframes. \
                The column would be setted to true if the indices of the dataframes are more specific(not default auto incremented!)
            left_index_col : The index col of the left dataset
            right_index_col : The index col of the right dataset
            max_days : The maximum allowed difference between the matches dates

        Returns:
            DataFrame with resolved matches
    """

    left_df = left_df.copy()
    right_df = right_df.copy()

    matches = []

    grouped_right = right_df.groupby(
        ["season", "h_title", "a_title"]
    )

    for _, left_row in left_df.iterrows():

        # The key by which the right matches will be searched 
        key = (
            left_row["season"],
            left_row["h_title"],
            left_row["a_title"]
        )

        if key not in grouped_right.groups:
            continue
        
        # Getting only the matches which match the key
        candidates = grouped_right.get_group(key)


        date_diff = (
            candidates["datetime"] - left_row["datetime"]
        ).abs().dt.days

        valid = candidates[date_diff <= max_days]

        if valid.empty:
            continue

        # Choosing the closest date
        best_match = valid.iloc[
            date_diff[date_diff <= max_days].argmin()
        ]

        # Saving the labels of the rows
        matches.append({
            "left_index": left_row[left_index_col] if use_index else left_row.name,
            "right_index": best_match[right_index_col] if use_index else best_match.name
        })

    return pd.DataFrame(matches)