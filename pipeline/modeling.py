import numpy as np
from catboost import CatBoostClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

from fraud_detector.src.model_config import CATEGORICAL_FEATURES, FEATURE_NAMES
from fraud_detector.src.preprocessing import run_preproc
from fraud_detector.src.scorer import load_model


def features(frame):
    return run_preproc(frame).loc[:, list(FEATURE_NAMES)]


def fit_model(x, y, iterations=200):
    model = CatBoostClassifier(
        iterations=iterations, depth=7, learning_rate=0.05,
        loss_function="Logloss", eval_metric="AUC",
        auto_class_weights="Balanced", random_seed=42, thread_count=2,
        task_type="CPU", verbose=False, allow_writing_files=False,
    )
    model.fit(x, y, cat_features=list(CATEGORICAL_FEATURES))
    return model


def metrics(model, x, y):
    prediction = model.predict_proba(x, thread_count=2)[:, 1]
    return {"pr_auc": float(average_precision_score(y, prediction)),
            "roc_auc": float(roc_auc_score(y, prediction))}


def approve(candidate, current):
    return all(np.isfinite(candidate[m]) and np.isfinite(current[m]) and candidate[m] >= current[m]
               for m in ("pr_auc", "roc_auc"))
