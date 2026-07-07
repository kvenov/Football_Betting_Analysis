from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay,
    RocCurveDisplay,
    roc_curve,
    auc,
    precision_recall_curve,
    average_precision_score
)

from sklearn.preprocessing import label_binarize

from football_betting_analysis.utils.logger import setup_logger

class Evaluator:
    """
        Configuration-driven evaluation framework.
        The entire evaluation is controlled from the experiment's evaluation config yaml file.

        Example:

            evaluation:
                metrics:
                    - accuracy
                    - balanced_accuracy
                    - precision_macro
                    - recall_macro
                    - f1_macro
                    - f1_weighted

                plots:
                    classification_report: true
                    confusion_matrix: true
                    normalized_confusion_matrix: true
                    roc_curve: true
                    pr_curve: true
                    probability_distribution: true
                    predictions_csv: true

        output_dir: reports/evaluation
    """

    def __init__(self, experiment_config):

        self.cfg = experiment_config["evaluation"]

        logger = setup_logger(
            experiment_config["logging"],
            mode='evaluate'
        )

        self.logger = logger
        
        self.metrics_cfg = set(
            self.cfg.get("metrics", [])
        )

        self.plots_cfg = self.cfg.get(
            "plots",
            {},
        )

        base_dir = Path(self.cfg["output_dir"])
        
        exp_name = experiment_config["experiment"]["experiment_name"]

        self.output_dir = base_dir / exp_name
        
        self.output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    def evaluate(
        self,
        pipeline,
        X_test,
        y_test,
    ):

        self.logger.info("Starting evaluation...")

        y_pred = pipeline.predict(X_test)

        y_prob = None

        if hasattr(
            pipeline,
            "predict_proba",
        ):
            y_prob = pipeline.predict_proba(
                X_test
            )

        metrics = self._calculate_metrics(
            y_test,
            y_pred,
        )

        # Reports
        if self.plots_cfg.get(
            "classification_report",
            True,
        ):
            self._save_classification_report(
                y_test,
                y_pred,
            )

        # Confusion Matrix
        if self.plots_cfg.get(
            "confusion_matrix",
            False,
        ):
            self._plot_confusion_matrix(
                y_test,
                y_pred,
            )

        if self.plots_cfg.get(
            "normalized_confusion_matrix",
            False,
        ):
            self._plot_normalized_confusion_matrix(
                y_test,
                y_pred,
            )

        # Probability-based plots
        if y_prob is not None:

            if self.plots_cfg.get(
                "roc_curve",
                False,
            ):
                self._plot_roc_curves(
                    y_test,
                    y_prob,
                )

            if self.plots_cfg.get(
                "pr_curve",
                False,
            ):
                self._plot_precision_recall_curves(
                    y_test,
                    y_prob,
                )

            if self.plots_cfg.get(
                "probability_distribution",
                False,
            ):
                self._plot_probability_distribution(
                    y_prob,
                )

        # Predictions
        if self.plots_cfg.get(
            "predictions_csv",
            True,
        ):
            self._save_predictions(
                X_test,
                y_test,
                y_pred,
                y_prob,
            )

        # MLFlow
        self._log_artifacts()

        self.logger.info("Evaluation finished.")

        return metrics

    # METRICS
    def _calculate_metrics(
        self,
        y_true,
        y_pred,
    ):

        available_metrics = {

            "accuracy":
                lambda: accuracy_score(
                    y_true,
                    y_pred,
                ),

            "balanced_accuracy":
                lambda: balanced_accuracy_score(
                    y_true,
                    y_pred,
                ),

            "precision_macro":
                lambda: precision_score(
                    y_true,
                    y_pred,
                    average="macro",
                    zero_division=0,
                ),

            "recall_macro":
                lambda: recall_score(
                    y_true,
                    y_pred,
                    average="macro",
                    zero_division=0,
                ),

            "f1_macro":
                lambda: f1_score(
                    y_true,
                    y_pred,
                    average="macro",
                    zero_division=0,
                ),

            "f1_weighted":
                lambda: f1_score(
                    y_true,
                    y_pred,
                    average="weighted",
                    zero_division=0,
                ),
        }

        metrics = {}

        for metric_name in self.metrics_cfg:

            if metric_name not in available_metrics:
                raise ValueError(
                    f"Unknown metric '{metric_name}'"
                )

            metrics[metric_name] = available_metrics[
                metric_name
            ]()

        return metrics

    # REPORT
    def _save_classification_report(
        self,
        y_true,
        y_pred,
    ):

        report = classification_report(
            y_true,
            y_pred,
            digits=4,
        )

        with open(
            self.output_dir /
            "classification_report.txt",
            "w",
        ) as f:
            f.write(report)

    # CONFUSION MATRICES
    def _plot_confusion_matrix(
        self,
        y_true,
        y_pred,
    ):

        cm = confusion_matrix(
            y_true,
            y_pred,
        )

        fig, ax = plt.subplots(
            figsize=(8, 8)
        )

        ConfusionMatrixDisplay(
            cm
        ).plot(
            ax=ax,
            colorbar=False,
        )

        plt.tight_layout()

        plt.savefig(
            self.output_dir /
            "confusion_matrix.png"
        )

        plt.close(fig)

    def _plot_normalized_confusion_matrix(
        self,
        y_true,
        y_pred,
    ):

        cm = confusion_matrix(
            y_true,
            y_pred,
            normalize="true",
        )

        fig, ax = plt.subplots(
            figsize=(8, 8)
        )

        ConfusionMatrixDisplay(
            cm
        ).plot(
            ax=ax,
            colorbar=False,
        )

        plt.tight_layout()

        plt.savefig(
            self.output_dir /
            "normalized_confusion_matrix.png"
        )

        plt.close(fig)

    # ROC
    def _plot_roc_curves(
        self,
        y_true,
        y_prob,
    ):

        classes = np.unique(y_true)

        y_true_bin = label_binarize(
            y_true,
            classes=classes,
        )

        plt.figure(figsize=(8, 6))

        for i, cls in enumerate(classes):

            fpr, tpr, _ = roc_curve(
                y_true_bin[:, i],
                y_prob[:, i],
            )

            score = auc(
                fpr,
                tpr,
            )

            plt.plot(
                fpr,
                tpr,
                label=f"{cls} (AUC={score:.3f})",
            )

        plt.plot(
            [0, 1],
            [0, 1],
            "--",
        )

        plt.legend()

        plt.tight_layout()

        plt.savefig(
            self.output_dir /
            "roc_curves.png"
        )

        plt.close()

    # PR
    def _plot_precision_recall_curves(
        self,
        y_true,
        y_prob,
    ):

        classes = np.unique(
            y_true
        )

        y_true_bin = label_binarize(
            y_true,
            classes=classes,
        )

        plt.figure(figsize=(8, 6))

        for i, cls in enumerate(classes):

            precision, recall, _ = precision_recall_curve(
                y_true_bin[:, i],
                y_prob[:, i],
            )

            ap = average_precision_score(
                y_true_bin[:, i],
                y_prob[:, i],
            )

            plt.plot(
                recall,
                precision,
                label=f"{cls} (AP={ap:.3f})",
            )

        plt.legend()

        plt.tight_layout()

        plt.savefig(
            self.output_dir /
            "precision_recall_curves.png"
        )

        plt.close()

    # Probability Distribution
    def _plot_probability_distribution(
        self,
        y_prob,
    ):

        plt.figure(figsize=(10, 5))

        for i in range(
            y_prob.shape[1]
        ):

            plt.hist(
                y_prob[:, i],
                bins=30,
                alpha=0.5,
                label=f"Class {i}",
            )

        plt.legend()

        plt.tight_layout()

        plt.savefig(
            self.output_dir /
            "probability_distribution.png"
        )

        plt.close()

    # Predictions
    def _save_predictions(
        self,
        X_test,
        y_true,
        y_pred,
        y_prob,
    ):

        predictions = X_test.copy()

        predictions["Actual"] = y_true.values
        predictions["Prediction"] = y_pred

        if y_prob is not None:

            for i in range(
                y_prob.shape[1]
            ):
                predictions[
                    f"Probability_{i}"
                ] = y_prob[:, i]

        predictions.to_csv(
            self.output_dir /
            "predictions.csv",
            index=False,
        )

    # MLFLOW
    def _log_artifacts(self):

        for artifact in self.output_dir.glob("*"):

            mlflow.log_artifact(
                str(artifact),
                artifact_path="evaluation",
            )