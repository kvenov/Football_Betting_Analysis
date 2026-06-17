import pandas as pd
from football_betting_analysis.data.cloude_storage.registry import Dataset


def validate_dataset(
    df: pd.DataFrame,
    dataset: Dataset,
) -> None:

    if df.empty:
        raise ValueError(
            f"{dataset.name} dataset is empty."
        )

    missing_columns = (
        set(dataset.required_columns)
        - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            f"{dataset.name} missing columns: "
            f"{missing_columns}"
        )

    if df.isna().all().all():
        raise ValueError(
            f"{dataset.name} contains only NaNs."
        )