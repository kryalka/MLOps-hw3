import json
import os
from pathlib import Path


MODELS_DIR = Path(os.getenv("SHARED_DIR", "/shared")) / "models"
META_PATH = Path(__file__).with_name("model_meta.json")

with META_PATH.open("r", encoding="utf-8") as metadata_file:
    MODEL_META = json.load(metadata_file)

FEATURE_NAMES = tuple(MODEL_META["feature_names"])
CATEGORICAL_FEATURES = tuple(MODEL_META["categorical_features"])

if len(FEATURE_NAMES) != len(set(FEATURE_NAMES)):
    raise ValueError("model_meta.json contains duplicate feature names")

unknown_categorical_features = set(CATEGORICAL_FEATURES) - set(FEATURE_NAMES)
if unknown_categorical_features:
    raise ValueError(
        "model_meta.json contains categorical features absent from feature_names: "
        f"{sorted(unknown_categorical_features)}"
    )

CATEGORICAL_FEATURE_INDICES = tuple(
    index
    for index, feature_name in enumerate(FEATURE_NAMES)
    if feature_name in CATEGORICAL_FEATURES
)
