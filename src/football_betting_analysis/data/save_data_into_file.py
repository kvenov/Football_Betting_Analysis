
import pandas as pd
import os
from pathlib import WindowsPath

def save_data(data: pd.DataFrame, file_path: WindowsPath) -> None:
    """
    Saves a pandas Data Frame into a file

    Parameters
    ----------
    data : pandas.DataFrame
        The pandas data frame which will be saved
    file_path : pathlib.WindowsPath
        The file path at which the data frame to be saved into
        
    Returns
    -------
    None
    """
    
    if os.path.exists(file_path):
        test_data = pd.read_parquet(path=file_path, engine="pyarrow")
        
        if not data.equals(test_data):
            print('The data which is at the specified file path exist, but it is not identical to the one that should be saved!')
            os.remove(path=file_path)
            
            data.to_parquet(
                path=file_path, 
                engine="pyarrow",
                compression="snappy",
                index=True
            )
            
            print(f'The data has successfully been saved into: {file_path}')
            
        else:
            print('The file has already been created and it contains the exact data as the original dataset!')
    else:
        data.to_parquet(
            path=file_path, 
            engine="pyarrow",
            compression="snappy",
            index=True
        )
        
        print(f'The data has successfully been saved into: {file_path}')