from pathlib import Path
import yaml

def get_project_root():
    return Path(__file__).resolve().parents[2]


def load_global_config():
    path = get_project_root() / "configs/global.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_data_path(cfg):
    return cfg["dvc"]["path"]