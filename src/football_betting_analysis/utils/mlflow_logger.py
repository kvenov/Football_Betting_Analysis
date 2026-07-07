import mlflow


class MLFlowLogger:

    def __init__(self):
        pass

    def log_dataset(self, cfg, df):
        mlflow.log_param("dataset_path", cfg["path"])
        mlflow.log_param("dataset_revision", cfg["revision"])
        mlflow.log_metric("dataset_rows", df.shape[0])
        mlflow.log_metric("dataset_features", df.shape[1])