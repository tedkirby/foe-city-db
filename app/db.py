import duckdb
import os
import yaml

def get_db_path():
    config_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "config",
        "settings.yaml"
    )

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    return os.path.expanduser(cfg["db_path"])


def get_connection():
    db_path = get_db_path()
    return duckdb.connect(db_path)
