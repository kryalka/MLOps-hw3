import logging
import os
import json

import pandas as pd
from catboost import CatBoostClassifier, Pool

from .model_config import (
    CATEGORICAL_FEATURE_INDICES,
    FEATURE_NAMES,
    MODELS_DIR,
)

logger = logging.getLogger(__name__)

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

THRESHOLD = 0.5

model = None
model_version = None


def load_model(path):
    loaded = CatBoostClassifier()
    loaded.load_model(str(path))
    if tuple(loaded.feature_names_) != FEATURE_NAMES:
        raise ValueError("Feature names in model_meta.json do not match model.cbm")
    if tuple(loaded.get_cat_feature_indices()) != CATEGORICAL_FEATURE_INDICES:
        raise ValueError("Categorical features in model_meta.json do not match model.cbm")
    return loaded


def reload_model():
    global model, model_version
    try:
        manifest = json.loads((MODELS_DIR / "latest.json").read_text())
        if manifest["version"] != model_version:
            replacement = load_model(MODELS_DIR / manifest["file"])
            model, model_version = replacement, manifest["version"]
            logger.info("Loaded model_version=%s without restarting service", model_version)
    except Exception:
        if model is None:
            raise
        logger.exception("Model reload failed; continuing with version=%s", model_version)


def _make_prediction_pool(dt):
    ordered_features = dt.loc[:, list(FEATURE_NAMES)]

    return Pool(
        data=ordered_features,
        feature_names=list(FEATURE_NAMES),
        cat_features=list(CATEGORICAL_FEATURE_INDICES),
    )


def make_pred(dt, source_info="kafka"):
    reload_model()
    prediction_pool = _make_prediction_pool(dt)

    y_proba = model.predict_proba(prediction_pool)[:, 1]

    submission = pd.DataFrame({
        "score": y_proba,
        "fraud_flag": (y_proba >= THRESHOLD).astype(int),
        "model_version": model_version,
    })

    logger.info(f'Prediction complete for data from {source_info}')

    return submission, y_proba
