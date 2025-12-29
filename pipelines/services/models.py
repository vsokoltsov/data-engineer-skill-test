from uuid import UUID
from datetime import datetime
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
