from dataclasses import dataclass
from uuid import UUID
from datetime import datetime
from typing import List
from urllib.parse import urljoin

import requests
from pydantic import BaseModel

class PredictionResponse(BaseModel):
    transaction_id: UUID
    category: str


class TransactionRequest(BaseModel):
    id: UUID
    description: str
    amount: float
    timestamp: datetime
    merchant: str | None
    operation_type: str
    side: str

@dataclass
class MLPredictService:
    url: str

    def predict(self, trx: List[TransactionRequest]) -> List[PredictionResponse]:
        response = requests.post(urljoin(self.url, "predict"), json=trx)
        return response.json()