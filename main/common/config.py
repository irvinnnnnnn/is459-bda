"""Names of the deployed resources. Terraform writes main/config.json on apply; every script and
both apps read it, so nothing has to be typed twice. An environment variable with the same name
in capitals (e.g. BUCKET, STREAM, REGION) overrides a value.
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "..", "config.json")
with open(os.path.join(HERE, "settings.json")) as _fh:      # our_bank and live_start are set there, once
    SETTINGS = {k: v for k, v in json.load(_fh).items() if not k.startswith("_")}
DEFAULTS = {"region": "us-east-1", "stream": "aml-payments", "alerts_table": "aml-q1-alerts", "athena_db": "aml",
            "athena_workgroup": "aml", **SETTINGS}


def load(required=("bucket",)):
    cfg = dict(DEFAULTS)
    if os.path.exists(PATH):
        with open(PATH) as fh:
            cfg.update(json.load(fh))
    cfg.update(SETTINGS)                    # settings.json always wins over a config.json from an older apply
    for k in list(cfg) + list(required):
        if os.getenv(k.upper()):
            cfg[k] = os.environ[k.upper()]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise SystemExit(f"Missing {missing}. Run terraform apply in main/infra (it writes main/config.json), "
                         f"or set {', '.join(k.upper() for k in missing)}.")
    return cfg
