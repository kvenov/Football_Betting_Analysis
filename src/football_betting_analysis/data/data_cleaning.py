import pandas as pd
import numpy as np

def convert_string_to_datetime(
    datetime_series: pd.Series,
    format_string: str,
    errors: str = 'raise'
) -> pd.Series:
    """
    Converts a pandas Series of string datetime values to a datetime Series,
    after validating that all values conform to the specified format.

    This function first checks the consistency of the datetime strings against
    the provided format by attempting to parse them with pandas.to_datetime.
    If all values parse successfully (i.e., no NaT values are produced), the
    Series is converted to datetime using the specified format. If inconsistencies
    are found, the behavior depends on the 'errors' parameter.

    Parameters:
    -----------
    datetime_series : pd.Series
        A pandas Series containing string representations of datetime values.
        The Series should not contain NaN or None values for accurate conversion.

    format_string : str
        The datetime format string (e.g., '%Y-%m-%d %H:%M:%S') that describes
        the expected structure of the datetime strings. This should match the
        format used in the Series.

    errors : str, optional (default='raise')
        How to handle parsing errors:
        - 'raise': Raise a ValueError if any value does not match the format.
        - 'coerce': Convert unparseable values to NaT (Not a Time) and proceed.
        - 'ignore': Return the original Series unchanged if inconsistencies are found.

    Returns:
    --------
    pd.Series
        A pandas Series with datetime64[ns] dtype if conversion is successful.
        If errors='ignore' and inconsistencies are found, returns the original Series.

    Raises:
    -------
    ValueError
        If errors='raise' and any datetime string does not match the specified format.
    TypeError
        If datetime_series is not a pandas Series or format_string is not a string.
    """
    # Input validation
    if not isinstance(datetime_series, pd.Series):
        raise TypeError("datetime_series must be a pandas Series.")
    if not isinstance(format_string, str):
        raise TypeError("format_string must be a string.")

    # Check for consistency by attempting to parse with errors='coerce'
    parsed_series = pd.to_datetime(datetime_series, format=format_string, errors='coerce')
    
    # Identify any NaT values, which indicate parsing failures
    inconsistent_mask = parsed_series.isna()
    if inconsistent_mask.any():
        if errors == 'raise':
            raise ValueError(
                f"Some datetime values do not conform to the specified format '{format_string}'. "
                f"Examples of problematic values: {datetime_series[inconsistent_mask].head().tolist()}"
            )
        elif errors == 'coerce':
            # Proceed with the coerced series (NaT for invalid values)
            return parsed_series
        elif errors == 'ignore':
            # Return the original series unchanged
            return datetime_series
        else:
            raise ValueError("errors must be one of 'raise', 'coerce', or 'ignore'.")
    
    # If all values are consistent, perform the conversion
    return pd.to_datetime(datetime_series, format=format_string, errors='raise')


def optimize_dataframe_memory(df: pd.DataFrame):
    """
    Optimize memory usage of a DataFrame by converting columns to more efficient data types.
    
    This function analyzes each column and applies appropriate optimizations:
    - Integer columns: Converts to smallest possible int type (int8, int16, int32, int64)
    - Float columns: Downcast to the smallest float type if precision allows
    - Object/String columns: Converts to category if cardinality is low (< 50% unique values)
    
    Args:
        df (pd.DataFrame): Input dataframe to optimize
        
    Returns:
        pd.DataFrame: Optimized dataframe with reduced memory usage
    """
    
    # Dividing by 1024**2 to get the memory in mega bytes!
    initial_memory = df.memory_usage(deep=True).sum() / 1024**2
    optimized_df = df.copy()
    
    for column in optimized_df.columns:
        col_type = optimized_df[column].dtype
        
        # Optimize integer columns
        if pd.api.types.is_integer_dtype(col_type):
            col_min = optimized_df[column].min()
            col_max = optimized_df[column].max()
            
            if col_min > np.iinfo(np.int8).min and col_max < np.iinfo(np.int8).max:
                optimized_df[column] = optimized_df[column].astype(np.int8)
            elif col_min > np.iinfo(np.int16).min and col_max < np.iinfo(np.int16).max:
                optimized_df[column] = optimized_df[column].astype(np.int16)
            elif col_min > np.iinfo(np.int32).min and col_max < np.iinfo(np.int32).max:
                optimized_df[column] = optimized_df[column].astype(np.int32)
        
        # Optimize float columns
        elif pd.api.types.is_float_dtype(col_type):
            optimized_df[column] = pd.to_numeric(
                optimized_df[column],
                downcast="float"
            )
            
            # optimized_df[column] = optimized_df[column].astype(np.float32)
        
        # Optimize object/string columns
        elif pd.api.types.is_object_dtype(col_type) or pd.api.types.is_string_dtype(col_type):
            num_unique = optimized_df[column].nunique()
            num_total = len(optimized_df[column])
            cardinality_ratio = num_unique / num_total
            
            # Convert to category if cardinality is low
            # A cardinality is considered to be low if the number of unique values is less than 5% of the total rows
            # This is known and tested to be true!
            if cardinality_ratio < 0.5:
                optimized_df[column] = optimized_df[column].astype('category')
    
    final_memory = optimized_df.memory_usage(deep=True).sum() / 1024**2
    memory_reduction = ((initial_memory - final_memory) / initial_memory) * 100
    
    print(f"Initial Memory Usage: {initial_memory:.2f} MB")
    print(f"Final Memory Usage: {final_memory:.2f} MB")
    print(f"Memory Reduction: {memory_reduction:.2f}%")
    print(f"\nOptimized Data Types:")
    print(optimized_df.dtypes)
    
    return optimized_df