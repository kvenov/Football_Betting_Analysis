import argparse

from football_betting_analysis.training.trainer import Trainer
from football_betting_analysis.utils.config_loader import ConfigLoader
from football_betting_analysis.utils.logger import setup_logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train experiment"
    )

    parser.add_argument(
        "--config",
        required=True,
        type=str,
        help="Experiment configuration yaml",
    )

    return parser.parse_args()

def main():

    args = parse_args()

    experiment_cfg = ConfigLoader(
        args.config
    ).load()

    logger = setup_logger(
        experiment_cfg["logging"],
        mode='train'
    )

    logger.info("=" * 100)
    logger.info(
        "Starting Experiment: %s",
        experiment_cfg["experiment"]["experiment_name"],
    )
    logger.info("=" * 100)

    trainer = Trainer(
        experiment_cfg
    )

    trainer.train()

    logger.info(
        "Experiment finished successfully."
    )


if __name__ == "__main__":
    main()