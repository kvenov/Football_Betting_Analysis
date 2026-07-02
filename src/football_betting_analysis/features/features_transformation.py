import pandas as pd

def transform_match_round(round: str) -> int:
    """
        Given a round str in format **n. Matchday**, the function transforms 
            the round into integer in format **n**!
        
        Parameters:
            round(str) : The round feature to be transformed
        
        Returns:
            interger : The transformed round in format **n**
    """
    
    round_transformed = int(round.split('.')[0])
    return round_transformed


def build_player_tenure(player_df, phantom_clubs_ids):
    """
    Given all transfer events for a single player (sorted by date), emit a list of tenure windows:
      **[club_id, joined_date, left_date, tenure_type]**
 
    
    Parameters:
        player_df(pd.DataFrame) : All of the transfer windows (events) of a given player.
        phantom_clubs_ids(list) : A list of predifined phantom clubs ids(e.g. Without CLub, Unknown, Retired)
            - This list used, so that the function to be able to distinguish the valid clubs from the more specific ones, which should be handled differently!
    
    Returns:
        windows(list) : A list of the transformed player transfers in format: **[club_id, joined_date, left_date, tenure_type]**
    
    Rules:
      - "permanent" arrival -> close current open window, open new one
      - "loan" departure -> close current open window (parent), open loan window
      - "return_from_loan" -> close loan window, re-open parent club window
      - arrival at "phantom" club -> close current window, open nothing
    """
    windows = [] # completed windows
    open_window = None # {club_id, joined_date, tenure_type}
    parent_club = None # remembered while player is on loan
 
    for row in player_df.itertuples(index=False):
        date = row.transfer_date
        to_club = row.to_team_id
        from_club = row.from_team_id
        etype = row.event_type
 
        to_phantom = pd.isna(to_club) or int(to_club) in phantom_clubs_ids
 
        if etype == "permanent":
            # Close whatever was open
            if open_window is not None:
                open_window["left_date"] = date
                windows.append(open_window)
            parent_club = None
            open_window = None
            if not to_phantom:
                open_window = {
                    "club_id": int(to_club),
                    "joined_date": date,
                    "left_date": pd.NaT,
                    "tenure_type": "permanent",
                }
 
        elif etype == "loan":
            # Suspend the parent club window
            if open_window is not None:
                parent_club = open_window["club_id"]
                open_window["left_date"] = date
                windows.append(open_window)
            else:
                # Loan with no recorded prior club — remember from_club as parent
                parent_club = int(from_club) if not pd.isna(from_club) else None
            
            open_window = None
            if not to_phantom:
                open_window = {
                    "club_id": int(to_club),
                    "joined_date": date,
                    "left_date": pd.NaT,
                    "tenure_type": "loan",
                }
 
        elif etype == "return_from_loan":
            # Close the loan window
            if open_window is not None and open_window.get("tenure_type") == "loan":
                open_window["left_date"] = date
                windows.append(open_window)
                open_window = None
            
            # Re-open at parent club (use to_team_id if available, else remembered parent)
            returning_to = int(to_club) if not to_phantom and not pd.isna(to_club) else parent_club
            if returning_to is not None and returning_to not in phantom_clubs_ids:
                open_window = {
                    "club_id": returning_to,
                    "joined_date": date,
                    "left_date": pd.NaT,
                    "tenure_type": "permanent",
                }
            parent_club = None
 
    # Flush the last open window (left_date = NaT = still at club)
    if open_window is not None:
        windows.append(open_window)
 
    return windows