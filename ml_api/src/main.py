import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from models import PredictionResponse, TransactionRequest

app = FastAPI(
    title="Transaction Category Prediction API",
    description="An API that predicts categories for financial transactions",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CATEGORIES = [
    "Rent",
    "Salary Income",
    "Food",
    "Restaurant",
    "Client Expenses",
    "Transportation",
    "Utilities",
    "Entertainment",
    "Healthcare",
    "Shopping",
    "Travel",
    "Education",
    "Insurance",
    "Investment",
    "Subscriptions"
]


@app.post("/predict")
def predict_post(transactions: list[TransactionRequest])-> list[PredictionResponse]:
    """Predict categories for a transaction based on its details."""

    return [PredictionResponse(
        transaction_id=transaction.id,
        category=CATEGORIES[hash(transaction.id) % len(CATEGORIES)],
    ) for transaction in transactions]


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)