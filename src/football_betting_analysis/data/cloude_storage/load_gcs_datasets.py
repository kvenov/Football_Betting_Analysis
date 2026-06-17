import pandas as pd
import requests

import logging
from pathlib import Path

from football_betting_analysis.data.cloude_storage.registry import Dataset, DATASETS
from football_betting_analysis.data.cloude_storage.validation import validate_dataset

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 60
CHUNK_SIZE = 1024 * 1024

def download_dataset(
    dataset: Dataset,
    force: bool = False,
) -> Path:

    local_file = Path(dataset.filename)

    if local_file.exists() and not force:
        logger.info(
            "Using cached dataset: %s",
            dataset.name,
        )
        return local_file

    logger.info(
        "Downloading dataset: %s",
        dataset.name,
    )

    response = requests.get(
        dataset.url,
        timeout=REQUEST_TIMEOUT,
        stream=True,
    )

    response.raise_for_status()

    tmp_file = local_file.with_suffix(".tmp")

    with open(tmp_file, "wb") as f:
        for chunk in response.iter_content(
            chunk_size=CHUNK_SIZE
        ):
            if chunk:
                f.write(chunk)

    tmp_file.replace(local_file)

    logger.info(
        "Downloaded dataset: %s",
        dataset.name,
    )

    return local_file

def load_dataset(
    dataset_name: str,
    force_download: bool = False,
) -> None:

    dataset = DATASETS[dataset_name]

    path = download_dataset(
        dataset,
        force=force_download,
    )

    df = pd.read_csv(path)

    try:
        validate_dataset(
            df,
            dataset,
        )
        
        logger.info(f"The validation of the {dataset.name} dataset has passed successfully!")
    except ValueError:
        logger.error(
            f"The validation of the {dataset.name} dataset does not passed successfully!"
        )    

def load_all_datasets(
    force_download: bool = False,
) -> None:

    for dataset_name in DATASETS:

        load_dataset(
            dataset_name,
            force_download,
        )