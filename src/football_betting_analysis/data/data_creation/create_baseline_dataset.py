import pandas as pd
import yaml

class BaselineDatasetBuilder:
    """
    Creates the baseline dataset used for all experiments.

    This script only:
    - selects required features
    - enforces correct column order
    - ensures target exists
    """

    def __init__(self, config: dict):
        self.config = config

    def build(self, df: pd.DataFrame) -> pd.DataFrame:

        features = self.config["features"]
        target = self.config["target"]

        missing = [f for f in features + [target] if f not in df.columns]

        if missing:
            raise ValueError(f"Missing columns in dataset: {missing}")

        df = df[features + [target]].copy()

        # Basic cleanup
        df = df.dropna(subset=[target])

        return df


def create_baseline_dataset(input_path: str, output_path: str, config: dict):
    df = pd.read_parquet(input_path)

    builder = BaselineDatasetBuilder(config)
    df_out = builder.build(df)

    df_out.to_parquet(output_path, index=False)

    print(f"[DATASET] Saved baseline dataset into {output_path}")


if __name__ == "__main__":
    with open("configs/data/baseline_processed.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    create_baseline_dataset(
        input_path=cfg["dataset"]["input_path"],
        output_path=cfg["dataset"]["path"],
        config=cfg["dataset"],
    )