from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (
    RandomForestClassifier,
    AdaBoostClassifier,
    VotingClassifier,
    StackingClassifier,
)

from sklearn.svm import SVC

try:
    from xgboost import XGBClassifier
except ImportError:
    XGBClassifier = None

class ModelFactory:
    """
        Factory responsible for constructing machine learning models
        from an experiment configuration.

        Example comfig:
            model:
                type: LogisticRegression

                params:
                    penalty: l2
                    C: 1.0
                    solver: lbfgs
                    max_iter: 1000
                    random_state: 42
    """

    def __init__(self, model_cfg: dict):
        self.model_cfg = model_cfg

    def build(self):

        # Getting the model type and hyperparameters
        model_type = self.model_cfg["type"]
        params = self.model_cfg.get("params", {})

        if model_type == "LogisticRegression":
            return self._build_logistic_regression(params)

        elif model_type == "RandomForestClassifier":
            return self._build_random_forest(params)

        elif model_type == "AdaBoostClassifier":
            return self._build_adaboost(params)

        elif model_type == "SVC":
            return self._build_svc(params)

        elif model_type == "XGBClassifier":
            return self._build_xgboost(params)

        elif model_type == "VotingClassifier":
            return self._build_voting_classifier(params)

        elif model_type == "StackingClassifier":
            return self._build_stacking_classifier(params)

        else:
            raise ValueError(
                f"Unsupported model type: {model_type}"
            )

    # Individual builders
    @staticmethod
    def _build_logistic_regression(params):
        
        return LogisticRegression(**params)

    @staticmethod
    def _build_random_forest(params):

        return RandomForestClassifier(**params)

    @staticmethod
    def _build_adaboost(params):

        return AdaBoostClassifier(**params)

    @staticmethod
    def _build_svc(params):

        return SVC(**params)

    @staticmethod
    def _build_xgboost(params):

        if XGBClassifier is None:
            raise ImportError(
                "xgboost is not installed."
            )

        return XGBClassifier(**params)

    @staticmethod
    def _build_voting_classifier(params):
        """
        Expected config:
            model:
                type: VotingClassifier

                params:
                    voting: soft

                    estimators:
                        rf:
                            type: RandomForestClassifier
                            params: ...

                        lr:
                            type: LogisticRegression
                            params: ...
        """

        estimators = []

        estimator_cfg = params.pop("estimators")

        for name, cfg in estimator_cfg.items():
            model = ModelFactory(cfg).build()
            estimators.append((name, model))

        return VotingClassifier(
            estimators=estimators,
            **params,
        )
    
    @staticmethod
    def _build_stacking_classifier(params):
        """
        Expected config:
            model:
                type: StackingClassifier

                params:
                    final_estimator:
                        type: LogisticRegression
                        params: ...

                    estimators:
                        rf:
                            ...

                        svm:
                            ...
        """

        estimators = []

        estimator_cfg = params.pop("estimators")

        for name, cfg in estimator_cfg.items():
            model = ModelFactory(cfg).build()
            estimators.append((name, model))

        final_cfg = params.pop("final_estimator")

        final_estimator = ModelFactory(
            final_cfg
        ).build()

        return StackingClassifier(
            estimators=estimators,
            final_estimator=final_estimator,
            **params,
        )