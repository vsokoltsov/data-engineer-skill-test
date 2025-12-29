from uuid import UUID

from pydantic import BaseModel
from datetime import datetime


class TransactionRequest(BaseModel):
    id: UUID
    description: str
    amount: float
    timestamp: datetime
    merchant: str | None
    operation_type: str
    side: str


class PredictionResponse(BaseModel):
    transaction_id: UUID
    category: str