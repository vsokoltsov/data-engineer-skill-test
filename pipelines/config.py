import os
from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA_PATH = os.path.join(ROOT, "data")
TRANSACTIONS_PATH = os.getenv(
    "TRXS_PATH", os.path.join(DATA_PATH, "transactions_fr.csv")
)
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+asyncpg://app:app@postgres:5432/transactions"
)
ML_API_URL = "http://ml-api:8000"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "transactions")
GROUP_ID = os.getenv("GROUP_ID", "transactions-consumer")
