import pandas as pd
import numpy as np

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
