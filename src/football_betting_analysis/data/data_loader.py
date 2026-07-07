from __future__ import annotations

import pandas as pd
import dvc.api
from dataclasses import dataclass

import logging
from pathlib import Path


logger = logging.getLogger(__name__)

@dataclass
class DataLoader:
    """
        Loads a versioned dataset tracked with DVC.
        
        The loader supports two modes:
            1. Remote (recommended)
            Streams the exact dataset revision directly from the DVC remote
            (Google Cloud Storage).

            2. Local
            Reads an already existing local file.

        Expected configuration:
            data:
                - source: dvc
                - path: data/processed/baseline_logistic.parquet
                - repo: .
                - revision: baseline_lr_v1
                - target: result_full
                - format: parquet
    """

    SUPPORTED_FORMATS = {
        "csv",
        "parquet",
    }

    def __init__(
        self,
        dataset_cfg: dict,
        experiment_name: str | None = None,
    ):

        self.cfg = dataset_cfg
        self.experiment_name = experiment_name

        self.source = self.cfg.get(
            "source",
            "dvc",
        )

        self.path = self.cfg["path"]
        
        self.repo = self.cfg.get(
            "repo",
            ".",
        )

        self.revision = self.cfg.get(
            "revision",
            None,
        )

        self.features = self.cfg["features"]
        self.target = self.cfg["target"]

        self.file_format = self.cfg.get(
            "format",
            Path(self.path).suffix.replace(".", ""),
        )

        if self.file_format not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported dataset format '{self.file_format}'."
            )

    def load(self) -> pd.DataFrame:

        logger.info("Loading dataset")

        logger.info(
            "Experiment : %s",
            self.experiment_name,
        )

        logger.info(
            "Dataset path : %s",
            self.path,
        )

        logger.info(
            "Revision : %s",
            self.revision,
        )

        logger.info(
            "Source : %s",
            self.source,
        )

        if self.source == "dvc":
            df = self._load_from_dvc()
        elif self.source == "local":
            df = self._load_local()
        else:
            raise ValueError(
                f"Unknown data source '{self.source}'."
            )

        logger.info(
            "Loaded dataset with shape %s",
            df.shape,
        )

        logger.info("=" * 80)

        return df

    # SPLIT X / Y
    def split_X_y(
        self,
        dataframe: pd.DataFrame,
    ):

        if self.target not in dataframe.columns:
            raise ValueError(
                f"Target column '{self.target}' not found."
            )

        X = dataframe[self.features]
        y = dataframe[self.target]

        logger.info(
            "Target column : %s",
            self.target,
        )

        logger.info(
            "Features : %d",
            X.shape[1],
        )

        return X, y

    def _load_from_dvc(self):

        logger.info(
            "Streaming dataset from DVC remote..."
        )

        with dvc.api.open(
            path=self.path,
            repo=self.repo,
            rev=self.revision,
            mode="rb",
        ) as file:

            if self.file_format == "csv":
                dataframe = pd.read_csv(file)
            elif self.file_format == "parquet":
                dataframe = pd.read_parquet(file)
            else:
                raise ValueError(
                    "Unsupported format."
                )

        logger.info(
            "Dataset successfully streamed from DVC."
        )

        return dataframe

    def _load_local(self):

        logger.info(
            "Loading local dataset..."
        )

        path = Path(self.path)
        if not path.exists():
            raise FileNotFoundError(
                path
            )

        if self.file_format == "csv":
            dataframe = pd.read_csv(path)
        elif self.file_format == "parquet":
            dataframe = pd.read_parquet(path)
        else:
            raise ValueError(
                "Unsupported format."
            )

        logger.info(
            "Dataset successfully loaded locally."
        )

        return dataframe