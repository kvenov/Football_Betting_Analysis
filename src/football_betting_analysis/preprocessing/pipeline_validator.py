from typing import Dict, Any, List

from football_betting_analysis.preprocessing.feature_groups import FeatureGroup, FeatureGroupParser


class PipelineValidator:
    """
        Validates preprocessing configuration before pipeline construction.
    """

    @staticmethod
    def validate_config(config: Dict[str, Any]):

        if "feature_pipeline" not in config:
            raise ValueError("Missing 'feature_pipeline' section in config")

        if not isinstance(config["feature_pipeline"], dict):
            raise ValueError("feature_pipeline must be a dictionary")

        if len(config["feature_pipeline"]) == 0:
            raise ValueError("feature_pipeline cannot be empty")

    @staticmethod
    def validate_feature_groups(groups: List[FeatureGroup]):

        if len(groups) == 0:
            raise ValueError("No feature groups provided")

        for g in groups:
            if not g.features:
                raise ValueError(f"Feature group {g.name} has no features")

            if g.type not in ["numeric", "categorical"]:
                raise ValueError(
                    f"Invalid type {g.type} in group {g.name}"
                )

    @staticmethod
    def validate_feature_existence(groups: List[FeatureGroup], dataframe_columns: List[str]):

        missing_features = []

        for g in groups:
            for f in g.features:
                if f not in dataframe_columns:
                    missing_features.append(f)

        if missing_features:
            raise ValueError(
                f"Features not found in dataset: {set(missing_features)}"
            )

    @staticmethod
    def validate_no_feature_overlap(groups: List[FeatureGroup]):
        
        seen = set()
        duplicates = set()

        for g in groups:
            for f in g.features:
                if f in seen:
                    duplicates.add(f)
                seen.add(f)

        if duplicates:
            raise ValueError(
                f"Overlapping features across groups: {duplicates}"
            )

    @staticmethod
    def validate_full_pipeline(config: Dict[str, Any], dataframe_columns: List[str]):

        PipelineValidator.validate_config(config)

        parser = FeatureGroupParser(config)
        groups = parser.parse()

        PipelineValidator.validate_feature_groups(groups)
        PipelineValidator.validate_no_feature_overlap(groups)
        PipelineValidator.validate_feature_existence(groups, dataframe_columns)

        return groups