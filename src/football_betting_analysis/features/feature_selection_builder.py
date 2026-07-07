from sklearn.feature_selection import (
    SelectKBest,
    SelectPercentile,
    SelectFromModel,
    VarianceThreshold,
    RFE,
    RFECV,
    mutual_info_classif,
    f_classif,
    chi2,
)

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

try:
    from xgboost import XGBClassifier
except ImportError:
    XGBClassifier = None

from football_betting_analysis.models.model_factory import ModelFactory 


class FeatureSelectionBuilder:
    """
        Builds sklearn-compatible feature selection transformers.

        Supported methods
        -----------------
        - VarianceThreshold
        - SelectKBest
        - SelectPercentile
        - SelectFromModel
        - RFE
        - RFECV
    """

    def __init__(self, config: dict):
        self.config = config

    def build(self):

        method = self.config["method"]

        if method == "VarianceThreshold":
            return self._build_variance_threshold()

        elif method == "SelectKBest":
            return self._build_select_k_best()

        elif method == "SelectPercentile":
            return self._build_select_percentile()

        elif method == "SelectFromModel":
            return self._build_select_from_model()

        elif method == "RFE":
            return self._build_rfe()

        elif method == "RFECV":
            return self._build_rfecv()

        else:
            raise ValueError(
                f"Unknown feature selection method: {method}"
            )

    # Filter methods:
    def _build_variance_threshold(self):

        threshold = self.config.get("threshold", 0.0)

        return VarianceThreshold(
            threshold=threshold
        )

    def _build_select_k_best(self):

        score_func = self._get_score_function()

        k = self.config.get("k", 20)

        return SelectKBest(
            score_func=score_func,
            k=k,
        )

    def _build_select_percentile(self):

        score_func = self._get_score_function()

        percentile = self.config.get(
            "percentile",
            50,
        )

        return SelectPercentile(
            score_func=score_func,
            percentile=percentile,
        )

    # Embedded methods:
    def _build_select_from_model(self):

        # estimator = self._create_estimator(
        #     self.config["estimator"]
        # )
        
        estimator = ModelFactory(
            self.config["estimator"]
        ).build()

        return SelectFromModel(
            estimator=estimator,
            threshold=self.config.get(
                "threshold",
                "mean",
            ),
            max_features=self.config.get(
                "max_features",
                None,
            ),
        )

    # Wrapper methods
    def _build_rfe(self):

        # estimator = self._create_estimator(
        #     self.config["estimator"]
        # )
        
        estimator = ModelFactory(
            self.config["estimator"]
        ).build()

        return RFE(
            estimator=estimator,
            n_features_to_select=self.config.get(
                "n_features_to_select",
                None,
            ),
            step=self.config.get(
                "step",
                1,
            ),
        )

    def _build_rfecv(self):

        # estimator = self._create_estimator(
        #     self.config["estimator"]
        # )

        estimator = ModelFactory(
            self.config["estimator"]
        ).build()

        return RFECV(
            estimator=estimator,
            step=self.config.get(
                "step",
                1,
            ),
            cv=self.config.get(
                "cv",
                5,
            ),
            scoring=self.config.get(
                "scoring",
                "accuracy",
            ),
            min_features_to_select=self.config.get(
                "min_features_to_select",
                1,
            ),
            n_jobs=self.config.get(
                "n_jobs",
                -1,
            ),
        )

    # Utilities:
    def _get_score_function(self):

        score = self.config.get(
            "score_function",
            "f_classif",
        )

        functions = {
            "f_classif": f_classif,
            "mutual_info": mutual_info_classif,
            "chi2": chi2,
        }

        if score not in functions:
            raise ValueError(
                f"Unknown score function: {score}"
            )

        return functions[score]

    def _create_estimator(self, estimator_cfg):

        model_type = estimator_cfg["type"]

        params = estimator_cfg.get(
            "params",
            {},
        )

        if model_type == "RandomForestClassifier":
            return RandomForestClassifier(**params)

        elif model_type == "LogisticRegression":
            return LogisticRegression(**params)

        elif model_type == "XGBClassifier":

            if XGBClassifier is None:
                raise ImportError(
                    "xgboost is not installed."
                )

            return XGBClassifier(**params)

        else:
            raise ValueError(
                f"Unsupported estimator: {model_type}"
            )