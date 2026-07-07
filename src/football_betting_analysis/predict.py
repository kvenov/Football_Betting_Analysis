import argparse

from football_betting_analysis.data.data_loader import DataLoader
from football_betting_analysis.prediction.predictor import Predictor
from football_betting_analysis.utils.config_loader import ConfigLoader
from football_betting_analysis.utils.logger import setup_logger


def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        required=True,
    )

    return parser.parse_args()


def main():
    args = parse_args()

    cfg = ConfigLoader(
        args.config
    ).load()

    logger = setup_logger(
        cfg["logging"],
        mode='predict'
    )

    logger.info("=" * 100)
    logger.info("Prediction started.")
    logger.info("=" * 100)

    loader = DataLoader(
        dataset_cfg=cfg["data"],
        experiment_name=cfg["experiment"]["name"],
    )

    df = loader.load()

    X, _ = loader.split_X_y(df)

    predictor = Predictor(
        cfg
    )

    model_path = (
        cfg["serialization"]["output_dir"]
        + "/"
        + cfg["model"]["type"]
        + "/"
        + cfg["experiment"]["experiment_name"]
        + ".joblib"
    )

    predictor.predict(
        model_path=model_path,
        dataframe=X,
    )

    logger.info(
        "Prediction completed."
    )


if __name__ == "__main__":
    main()