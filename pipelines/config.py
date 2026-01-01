import os
from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA_PATH = os.path.join(ROOT, "data")
TRXS_PATH = os.getenv(
    "TRXS_PATH",
    os.path.join(DATA_PATH, "transactions_fr.csv")
)
DATABASE_URL = "postgresql+asyncpg://app:app@postgres:5432/transactions"
ML_API_URL = "http://ml-api:8000"