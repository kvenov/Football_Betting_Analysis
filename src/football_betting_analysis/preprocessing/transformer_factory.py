from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    StandardScaler,
    MinMaxScaler,
    RobustScaler,
    PowerTransformer,
    OneHotEncoder,
    OrdinalEncoder,
    TargetEncoder
)

from sklearn.impute import SimpleImputer

from football_betting_analysis.preprocessing.custom_transformers import (
    Log1pTransformer,
    SqrtTransformer
)


class TransformerFactory:
    """
        Factory responsible for creating sklearn transformers
        from preprocessing configuration.
    """

    @classmethod
    def create_pipeline(cls, group_cfg: dict) -> Pipeline:
        """
            Creates a preprocessing pipeline for one feature group.
        """

        steps = []

        # Optional custom transformation
        transform_cfg = group_cfg.get("transform")

        if transform_cfg:
            transformer = cls.create_custom_transformer(
                transform_cfg["type"]
            )
            steps.append(("transform", transformer))

        # Optional imputation
        imputer_cfg = group_cfg.get("imputer")

        if (
            imputer_cfg
            and imputer_cfg.get("strategy", "none") != "none"
        ):
            steps.append(
                (
                    "imputer",
                    cls.create_imputer(imputer_cfg),
                )
            )

        # Numeric pipeline
        if group_cfg["type"] == "numeric":
            scaler = cls.create_scaler(
                group_cfg["transformer"]["scaler"]
            )

            if scaler is not None:
                steps.append(("scaler", scaler))

        # Categorical pipeline
        elif group_cfg["type"] == "categorical":
            encoder = cls.create_encoder(
                group_cfg["transformer"]
            )

            steps.append(("encoder", encoder))

        else:
            raise ValueError(
                f"Unsupported feature type: {group_cfg['type']}"
            )

        return Pipeline(steps)

    # Individual builders
    @staticmethod
    def create_scaler(name: str):

        scalers = {
            "standard": StandardScaler(),
            "minmax": MinMaxScaler(),
            "robust": RobustScaler(),
            "power": PowerTransformer(),
            "none": None,
        }

        if name not in scalers:
            raise ValueError(f"Unknown scaler: {name}")

        return scalers[name]

    @staticmethod
    def create_imputer(cfg: dict):

        return SimpleImputer(
            strategy=cfg["strategy"]
        )

    @staticmethod
    def create_encoder(cfg: dict):
        encoder_name = cfg["encoding"]
        params = cfg.get("params", {})

        supported_encoders = ["onehot", "ordinal", "target"]
        if encoder_name not in supported_encoders:
            raise ValueError(
                f"Unknown encoder: {encoder_name}"
            )

        if encoder_name == "onehot":
            return OneHotEncoder(
                handle_unknown="ignore",
                sparse_output=False,
                **params
            )
            
        elif encoder_name == "ordinal":
            return OrdinalEncoder(
                handle_unknown="use_encoded_value",
                unknown_value=-1,
                **params
            )
            
        elif encoder_name == "target":
            return TargetEncoder(
                **params
            )


    @staticmethod
    def create_custom_transformer(name: str):

        transformers = {
            "log1p": Log1pTransformer(),
            "sqrt": SqrtTransformer(),
            "none": None,
        }

        if name not in transformers:
            raise ValueError(
                f"Unknown custom transform: {name}"
            )

        return transformers[name]