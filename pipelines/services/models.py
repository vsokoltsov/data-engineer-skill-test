from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, field_validator


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

    @field_validator("side")
    @classmethod
    def validate_side(cls, v: str) -> str:
        allowed = {"debit", "credit"}
        if v not in allowed:
            raise ValueError(f"side must be one of {allowed}")
        return v

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount must be non-zero")
        return v
