from typing import Protocol, List
from pipelines.services.models import TransactionRequest, PredictionResponse


class MLServiceProtocol(Protocol):
    def predict(self, trx: List[TransactionRequest]) -> List[PredictionResponse]: ...
