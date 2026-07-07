import pandas as pd
from sklearn.compose import ColumnTransformer

from football_betting_analysis.preprocessing.feature_groups import FeatureGroupSelector
from football_betting_analysis.preprocessing.pipeline_validator import PipelineValidator
from football_betting_analysis.preprocessing.transformer_factory import TransformerFactory


class PreprocessingBuilder:
    """
        Builds a full ColumnTransformer preprocessing pipeline
        from YAML configuration.
    """

    def __init__(self, config: dict):
        self.config = config

    def build(self, df: pd.DataFrame) -> ColumnTransformer:

        # Validate + parse groups
        groups = PipelineValidator.validate_full_pipeline(
            self.config,
            df.columns.tolist(),
        )

        # Filter enabled groups
        groups = FeatureGroupSelector.filter_enabled(groups)

        transformers = []

        # Build pipelines per group
        for group in groups:

            pipeline = TransformerFactory.create_pipeline(
                group.config
            )

            transformers.append(
                (
                    group.name,
                    pipeline,
                    group.features,
                )
            )

        # Create ColumnTransformer
        preprocessing_pipeline = ColumnTransformer(
            transformers=transformers,
            remainder=self.config.get(
                "remainder", "drop"
            ),
            verbose_feature_names_out=False,
        )

        return preprocessing_pipeline