import optuna

class SearchSpace:

    @staticmethod
    def logistic_regression(trial):
        return {

            "C": trial.suggest_float(
                "C",
                0.001,
                10,
                log=True,
            ),

            "penalty": trial.suggest_categorical(
                "penalty",
                [
                    "l1",
                    "l2",
                ],
            ),

            "solver": "saga",

            "max_iter": trial.suggest_int(
                "max_iter",
                500,
                3000,
                step=500,
            ),

            "class_weight": trial.suggest_categorical(
                "class_weight",
                [
                    None,
                    "balanced",
                ],
            )
        }

    @staticmethod
    def random_forest(trial):
        return {

            "n_estimators": trial.suggest_int(
                "n_estimators",
                100,
                1000,
                step=100,
            ),

            "max_depth": trial.suggest_int(
                "max_depth",
                3,
                30,
            ),

            "min_samples_split": trial.suggest_int(
                "min_samples_split",
                2,
                20,
            ),

            "min_samples_leaf": trial.suggest_int(
                "min_samples_leaf",
                1,
                10,
            ),

            "class_weight": trial.suggest_categorical(
                "class_weight",
                [
                    None,
                    "balanced",
                ],
            )
        }