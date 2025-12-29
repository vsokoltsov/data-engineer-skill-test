import pandas as pd
from typing import Iterator

def read_transactions(path: str, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
    for chunk in pd.read_csv(
        path,
        sep=";",
        quotechar='"',
        decimal=",",
        parse_dates=["timestamp"],
        chunksize=chunk_size,
    ):
        chunk["id"] = chunk["id"].astype(str)
        yield chunk