import logging

import numpy as np
import pandas as pd

from .model_config import CATEGORICAL_FEATURES

logger = logging.getLogger(__name__)


def haversine(lat1, lon1, lat2, lon2):
    rad = 6371

    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return rad * c


def add_features(df):
    df_ = df.copy()

    coordinate_columns = [
        "lat",
        "lon",
        "merchant_lat",
        "merchant_lon",
    ]

    for col in coordinate_columns:
        df_[col] = pd.to_numeric(df_[col], errors="coerce")

    numeric_columns = ["amount", "population_city"]

    for col in numeric_columns:
        if col in df_.columns:
            df_[col] = pd.to_numeric(df_[col], errors="coerce")

    df_["haversine"] = haversine(
        df_["lat"],
        df_["lon"],
        df_["merchant_lat"],
        df_["merchant_lon"],
    )

    df_["transaction_time"] = pd.to_datetime(df_["transaction_time"], errors="coerce")
    df_["year"] = df_["transaction_time"].dt.year
    df_["month"] = df_["transaction_time"].dt.month
    df_["day"] = df_["transaction_time"].dt.day
    df_["hour"] = df_["transaction_time"].dt.hour
    df_["minute"] = df_["transaction_time"].dt.minute
    df_["weekend"] = (df_["transaction_time"].dt.dayofweek > 4).astype("int")
    df_["amount_log1p"] = np.log1p(df_["amount"].clip(lower=0))
    df_["hour_sin"] = np.sin(2 * np.pi * df_["hour"] / 24)
    df_["hour_cos"] = np.cos(2 * np.pi * df_["hour"] / 24)
    df_["is_night"] = df_["hour"].between(0, 5).astype(int)

    columns_to_drop = ["transaction_time", "name_1", "name_2", "street"]

    return df_.drop(columns=columns_to_drop, errors="ignore")


def run_preproc(input_df):
    output_df = add_features(input_df)

    for col in CATEGORICAL_FEATURES:
        if col in output_df.columns:
            output_df[col] = (
                output_df[col]
                .fillna("__MISSING__")
                .astype(str)
            )

    logger.info(
        "Added new features. Output shape: %s",
        output_df.shape
    )

    return output_df
