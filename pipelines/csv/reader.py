import uuid
from dataclasses import dataclass
import pandas as pd
from typing import Iterator

@dataclass
class CSVReader:
    file_path: str

    def read_batches(self, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
        for chunk in pd.read_csv(
            self.file_path,
            sep=";",
            quotechar='"',
            decimal=",",
            parse_dates=["timestamp"],
            chunksize=chunk_size,
        ):
            chunk["id"] = chunk["id"].map(lambda x: str(uuid.UUID(int=x)))
            chunk['timestamp'] = chunk['timestamp'].map(lambda x: x.isoformat())
            yield chunk    
