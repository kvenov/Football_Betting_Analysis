import logging

from sklearn.metrics import (
    accuracy_score,
    f1_score,
)

logger = logging.getLogger(__name__)

class Objective:
    def __init__(
        self,
        pipeline_builder,
        model_factory,
        X_train,
        X_valid,
        y_train,
        y_valid,
        model_name,
        search_space,
        metric="f1_macro",
    ):
        self.pipeline_builder = pipeline_builder
        self.model_factory = model_factory
        self.X_train = X_train
        self.X_valid = X_valid
        self.y_train = y_train
        self.y_valid = y_valid
        self.model_name = model_name
        self.search_space = search_space
        self.metric = metric



    def __call__(self, trial):
        params = self.search_space(
            trial
        )

        logger.info(
            "Trial parameters: %s",
            params
        )

        model = self.model_factory.create(
            model_name=self.model_name,
            params=params,
        )

        pipeline = self.pipeline_builder.build(
            model=model
        )

        pipeline.fit(
            self.X_train,
            self.y_train,
        )

        predictions = pipeline.predict(
            self.X_valid
        )

        if self.metric == "accuracy":

            score = accuracy_score(
                self.y_valid,
                predictions,
            )

        elif self.metric == "f1_macro":

            score = f1_score(
                self.y_valid,
                predictions,
                average="macro",
            )

        else:

            raise ValueError(
                f"Metric {self.metric} not supported"
            )


        return score