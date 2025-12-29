import pandas as pd

def merge_predictions(chunk: pd.DataFrame, predictions: pd.DataFrame) -> pd.DataFrame:
    df_pred = pd.DataFrame(predictions)
    df = chunk.merge(
        df_pred,
        left_on="id",
        right_on="transaction_id",
        how="left",
        validate="one_to_one"
    ).drop(columns=["transaction_id"])
    df['timestamp'] = pd.to_datetime(df["timestamp"], errors="raise")
    return df