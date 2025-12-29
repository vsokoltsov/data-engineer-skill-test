from typing import List

import pandas as pd
from pipelines.services.models import PredictionResponse


def merge_predictions(
    chunk: pd.DataFrame, predictions: List[PredictionResponse]
) -> pd.DataFrame:
    df_pred = pd.DataFrame(predictions)
    df = chunk.merge(
        df_pred,
        left_on="id",
        right_on="transaction_id",
        how="left",
        validate="one_to_one",
    ).drop(columns=["transaction_id"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
    return df
