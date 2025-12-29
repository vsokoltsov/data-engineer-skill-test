from __future__ import annotations
from dataclasses import dataclass
from urllib.parse import urljoin

import requests
from typing import List


from pipelines.services.backoff import retry_with_backoff
from pipelines.services.models import TransactionRequest, PredictionResponse


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
