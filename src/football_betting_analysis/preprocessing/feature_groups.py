from dataclasses import dataclass
from typing import Dict, List, Any


@dataclass
class FeatureGroup:
    """
        Internal representation of a feature group
        parsed from YAML configuration.
    """
    name: str
    type: str
    features: List[str]
    config: Dict[str, Any]


class FeatureGroupParser:
    """
        Converts raw YAML preprocessing config into structured feature groups.
    """

    def __init__(self, raw_config: Dict[str, Any]):
        self.raw_config = raw_config

    def parse(self) -> List[FeatureGroup]:
        feature_pipeline = self.raw_config.get("feature_pipeline", {})

        groups = []

        for name, cfg in feature_pipeline.items():

            groups.append(
                FeatureGroup(
                    name=name,
                    type=cfg["type"],
                    features=cfg["features"],
                    config=cfg,
                )
            )

        return groups


class FeatureGroupSelector:
    """
        Utility for selecting active feature groups.
        If a certain feature group from the preprocessing config is not enabled(enabled: False),
        the feature group will not be included into the data for the experiment!
    """

    @staticmethod
    def filter_enabled(groups: List[FeatureGroup]) -> List[FeatureGroup]:
        """
            If 'enabled' is not specified, default = True.
        """

        enabled_groups = []

        for g in groups:
            enabled = g.config.get("enabled", True)

            if enabled:
                enabled_groups.append(g)

        return enabled_groups


class FeatureGroupValidator:
    """
        Ensures feature groups are valid and non-overlapping.
    """

    @staticmethod
    def validate(groups: List[FeatureGroup]):

        all_features = []

        for g in groups:
            all_features.extend(g.features)

        duplicates = set(
            [f for f in all_features if all_features.count(f) > 1]
        )

        if duplicates:
            raise ValueError(
                f"Duplicate features found across groups: {duplicates}"
            )