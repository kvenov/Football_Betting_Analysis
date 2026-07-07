import logging
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd

from sklearn.model_selection import train_test_split

from football_betting_analysis.data.data_loader import DataLoader
from football_betting_analysis.pipelines.pipeline_builder import PipelineBuilder
from football_betting_analysis.evaluation.evaluator import Evaluator

logger = logging.getLogger(__name__)

class Trainer:
    """
        Main training component.

        Responsibilities:
            1. Load dataset
            2. Split train / test
            3. Build sklearn pipeline
            4. Train pipeline
            5. Evaluate pipeline
            6. Log everything to MLflow
            7. Serialize trained pipeline
    """

    def __init__(self, experiment_config: dict):

        self.cfg = experiment_config

    def train(self):

        logger.info("Starting experiment...")

        experiment_name = self.cfg["experiment"]["experiment_name"]
        self._setup_mlflow()

        with mlflow.start_run(run_name=experiment_name):
            # Load dataset
            logger.info("Loading dataset...")

            loader = DataLoader(
                dataset_cfg=self.cfg["data"],
                experiment_name=experiment_name,
            )

            # Loading the dataset and splitting it into features and target:
            df = loader.load()
            X, y = loader.split_X_y(df)

            logger.info(
                "Dataset loaded successfully | Shape=%s",
                X.shape,
            )

            # Sequential Split
            logger.info("Performing sequential train/test split...")

            X_train, X_test, y_train, y_test = self._train_test_split(
                X,
                y,
            )

            logger.info(
                "Train samples=%d | Test samples=%d",
                len(X_train),
                len(X_test),
            )

            # Building the Pipeline
            logger.info("Building sklearn pipeline...")

            pipeline = PipelineBuilder(
                self.cfg
            ).build(X_train)

            logger.info("Pipeline created successfully.")

            # Train
            logger.info("Training model...")

            pipeline.fit(
                X_train,
                y_train,
            )

            logger.info("Training finished.")

            # Evaluate
            logger.info("Evaluating model...")

            evaluator = Evaluator(
                experiment_config=self.cfg,
            )

            evaluation_results = evaluator.evaluate(
                pipeline,
                X_test,
                y_test,
            )

            logger.info("Evaluation completed.")

            # MLflow Logging
            self._log_metrics(
                evaluation_results
            )

            self._log_configuration()

            mlflow.sklearn.log_model(
                sk_model=pipeline,
                name="model",
                 skops_trusted_types=[
                    "football_betting_analysis.preprocessing.custom_transformers.Log1pTransformer",
                    "football_betting_analysis.preprocessing.custom_transformers.SqrtTransformer",
                    "numpy.dtype"
                ]
            )

            # Serialize
            self._save_pipeline(
                pipeline
            )

            logger.info("Experiment finished successfully.")

        return pipeline

    # Private methods:
    def _setup_mlflow(self):

        mlflow.set_tracking_uri(
            self.cfg["global"]["mlflow"]["tracking_uri"]
        )

        mlflow.set_experiment(
            self.cfg["mlflow"]["experiment_name"]
        )

    def _train_test_split(
        self,
        X: pd.DataFrame,
        y: pd.Series,
    ):

        split_cfg = self.cfg["split"]

        return train_test_split(
            X,
            y,
            test_size=split_cfg["test_size"],
            shuffle=False,
        )

    def _log_metrics(
        self,
        metrics: dict,
    ):

        logger.info("Logging metrics to MLflow...")

        for metric_name, value in metrics.items():

            if isinstance(
                value,
                (int, float),
            ):
                mlflow.log_metric(
                    metric_name,
                    value,
                )

    def _log_configuration(self):

        logger.info("Logging configuration...")

        mlflow.log_dict(
            self.cfg,
            "experiment_config.yaml",
        )

        mlflow.log_param(
            "dataset_revision",
            self.cfg["data"]["revision"],
        )

        mlflow.log_param(
            "dataset_path",
            self.cfg["data"]["path"],
        )

        mlflow.log_param(
            "model_type",
            self.cfg["model"]["type"],
        )

    def _save_pipeline(
        self,
        pipeline,
    ):
        base_dir = Path(
            self.cfg["serialization"]["output_dir"]
        )
        
        model_name = self.cfg["model"]["type"] 
        exp_name = self.cfg["experiment"]["experiment_name"]

        output_dir = base_dir / model_name
        
        output_dir.mkdir(parents=True, exist_ok=True)

        model_file_name = f"{exp_name}.joblib"
        model_path = output_dir / model_file_name

        logger.info(
            "Serializing trained pipeline to %s",
            model_path,
        )

        joblib.dump(
            pipeline,
            model_path,
        )

        mlflow.log_artifact(
            str(model_path),
            artifact_path="serialized_model",
        )

        logger.info("Pipeline saved successfully.")