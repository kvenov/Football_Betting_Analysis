import logging
from pathlib import Path

import joblib
import pandas as pd


logger = logging.getLogger(__name__)

class Predictor:
    """
        Loads a trained pipeline and generates predictions.

        Responsibilities:
            - Load serialized pipeline
            - Predict labels
            - Predict probabilities (if supported)
            - Save predictions
    """

    def __init__(self, config: dict):
        self.cfg = config

        base_dir = Path(
            self.cfg["prediction"]["output_dir"]
        )

        exp_name = self.cfg["experiment"]["experiment_name"]

        self.output_dir = base_dir / exp_name

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    def predict(
        self,
        model_path,
        dataframe: pd.DataFrame,
    ) -> pd.DataFrame:
        logger.info("=" * 50)
        logger.info("Prediction started")
        logger.info("=" * 50)

        pipeline = self._load_pipeline(
            model_path
        )

        logger.info("Predicting classes...")

        predictions = pipeline.predict(
            dataframe
        )

        prediction_df = dataframe.copy()
        prediction_df["prediction"] = predictions

        if hasattr(
            pipeline,
            "predict_proba",
        ):

            logger.info(
                "Predicting probabilities..."
            )

            probabilities = pipeline.predict_proba(
                dataframe
            )

            classes = pipeline.classes_
            for i, cls in enumerate(classes):
                prediction_df[
                    f"probability_{cls}"
                ] = probabilities[:, i]

        self._save_predictions(
            prediction_df
        )

        logger.info(
            "Prediction finished."
        )

        logger.info("=" * 80)
        return prediction_df

    def _load_pipeline(
        self,
        model_path,
    ):

        logger.info(
            "Loading model from %s",
            model_path,
        )

        pipeline = joblib.load(
            model_path
        )

        logger.info(
            "Pipeline loaded successfully."
        )

        return pipeline

    def _save_predictions(
        self,
        predictions: pd.DataFrame,
    ):

        output_path = (
            self.output_dir
            / "predictions.csv"
        )

        predictions.to_csv(
            output_path,
            index=False,
        )

        logger.info(
            "Predictions saved to %s",
            output_path,
        )