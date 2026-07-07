from sklearn.pipeline import Pipeline

from football_betting_analysis.preprocessing.preprocessing_builder import PreprocessingBuilder

from football_betting_analysis.features.feature_selection_builder import FeatureSelectionBuilder
from football_betting_analysis.features.dimensionality_reduction_builder import DimensionalityReductionBuilder

from football_betting_analysis.models.model_factory import ModelFactory


class PipelineBuilder:
    """
        Builds the complete sklearn Pipeline for an experiment.

        Pipeline structure:
            - Preprocessing
            - Feature Selection (optional)
            - Dimensionality Reduction (optional)
            - ML Model
    """

    def __init__(self, experiment_cfg: dict):
        self.cfg = experiment_cfg

    def build(self, dataframe):

        steps = []

        # Preprocessing
        preprocessing_cfg = self.cfg["preprocessing"]

        preprocessing = PreprocessingBuilder(
            preprocessing_cfg
        ).build(dataframe)

        steps.append(
            (
                "preprocessing",
                preprocessing,
            )
        )

        # Performing Feature Selection if enabled
        feature_selection_cfg = self.cfg.get(
            "feature_selection",
            {"enabled": False},
        )

        if feature_selection_cfg.get("enabled", False):
            selector = FeatureSelectionBuilder(
                feature_selection_cfg
            ).build()

            steps.append(
                (
                    "feature_selection",
                    selector,
                )
            )

        # Performing Dimensionality Reduction if enabled
        reduction_cfg = self.cfg.get(
            "dimensionality_reduction",
            {"enabled": False},
        )

        if reduction_cfg.get("enabled", False):
            reducer = DimensionalityReductionBuilder(
                reduction_cfg
            ).build()

            steps.append(
                (
                    "dimensionality_reduction",
                    reducer,
                )
            )

        # Model
        model_cfg = self.cfg["model"]

        model = ModelFactory(
            model_cfg
        ).build()

        steps.append(
            (
                "model",
                model,
            )
        )

        # Final sklearn Pipeline
        pipeline = Pipeline(
            steps=steps,
            verbose=True,
        )

        return pipeline