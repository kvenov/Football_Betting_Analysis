import yaml
from pathlib import Path


def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


class ConfigLoader:
    def __init__(self, experiment_config_path: str):
        self.experiment_config_path = experiment_config_path

    def load(self):

        exp_cfg = load_yaml(self.experiment_config_path)

        logging_cfg = exp_cfg.get("logging", {})
        global_cfg = load_yaml("configs/global.yaml")
        mlflow_cfg = exp_cfg.get("mlflow", {})
        data_cfg = load_yaml(exp_cfg["data"]["config"])
        split_cfg = load_yaml(exp_cfg["split"]["config"])
        model_cfg = load_yaml(exp_cfg["model"]["config"])
        preprocessing_cfg = load_yaml(exp_cfg["preprocessing"]["config"])
        eval_cfg = load_yaml(exp_cfg["evaluation"]["config"])
        serialization_cfg = exp_cfg.get("serialization", {})
        prediction_cfg = exp_cfg.get("prediction", {})
        
        # fs_cfg = load_yaml(exp_cfg["feature_selection"]["config"])
        # dr_cfg = load_yaml(exp_cfg["dimensionality_reduction"]["config"])
        # opt_cfg = load_yaml(exp_cfg["optimization"]["config"])
        
        # Feature selection save check and safe load
        fs_block = exp_cfg.get("feature_selection", {"enabled": False})
        if fs_block.get("enabled", False) and "config" in fs_block:
            fs_cfg = load_yaml(fs_block["config"])
            fs_cfg["enabled"] = True
        else:
            fs_cfg = {"enabled": False}

        # Dimensionality reduction save check and safe load
        dr_block = exp_cfg.get("dimensionality_reduction", {"enabled": False})
        if dr_block.get("enabled", False) and "config" in dr_block:
            dr_cfg = load_yaml(dr_block["config"])
            dr_cfg["enabled"] = True
        else:
            dr_cfg = {"enabled": False}

        # Optimization save check and safe load
        opt_block = exp_cfg.get("optimization", {"enabled": False})
        if opt_block.get("enabled", False) and "config" in opt_block:
            opt_cfg = load_yaml(opt_block["config"])
            opt_cfg["enabled"] = True
        else:
            opt_cfg = {"enabled": False}

        return {
            "logging": logging_cfg,
            "global": global_cfg,
            "experiment": exp_cfg,
            "mlflow": mlflow_cfg,
            "data": data_cfg,
            "split": split_cfg,
            "model": model_cfg,
            "preprocessing": preprocessing_cfg,
            "feature_selection": fs_cfg,
            "dimensionality_reduction": dr_cfg,
            "optimization": opt_cfg,
            "evaluation": eval_cfg,
            "serialization": serialization_cfg,
            "prediction": prediction_cfg
        }