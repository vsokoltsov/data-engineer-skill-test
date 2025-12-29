from __future__ import annotations
from dataclasses import dataclass
from uuid import UUID
from datetime import datetime
from urllib.parse import urljoin

import requests
from pydantic import BaseModel
from typing import List

import requests

from pipelines.services.backoff import retry_with_backoff

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

    @retry_with_backoff(
        max_retries=5,
        base_delay_s=0.5,
        max_delay_s=10.0,
        jitter=True,
        retry_statuses=(429, 500, 502, 503, 504),
    )
    def _post(self, path: str, json: object) -> requests.Response:
        return requests.post(urljoin(self.url, path), json=json)

    def predict(self, trx: List[TransactionRequest]) -> List[PredictionResponse]:
        response = self._post("predict", json=trx)
        return response.json()