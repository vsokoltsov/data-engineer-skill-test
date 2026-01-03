from typing import List

import pandas as pd
import numpy as np
from dataclasses import asdict
from pipelines.services.models import PredictionResponse


def merge_predictions(
    chunk: pd.DataFrame, predictions: List[PredictionResponse]
) -> pd.DataFrame:
    df = chunk.copy()
    pred_map = {
        str(p.transaction_id): p.category
        for p in predictions
    }

    df["category"] = df["id"].map(pred_map)
    # Convert to object dtype and replace NaN/NA with None
    df["category"] = df["category"].astype(object)
    # Use replace to convert NaN/NA to None
    df["category"] = df["category"].replace({pd.NA: None, np.nan: None, pd.NA: None})
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
    return df
