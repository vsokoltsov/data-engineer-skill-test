import asyncio
from uuid import UUID
import json
import os
from typing import Any, Dict
from pydantic import BaseModel
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from pipelines.services.ml_api import MLPredictService
from pipelines.db.repository import TransactionRepository
from pipelines.services.csv import KafkaTransactionIngestService
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

DATABASE_URL = "postgresql+asyncpg://app:app@postgres:5432/transactions"

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


class Transaction(BaseModel):
    id: UUID
    description: str
    amount: float
    timestamp: datetime
    merchant: str | None
    operation_type: str
    side: str


def json_deserializer(data: bytes) -> Dict[str, Any]:
    return json.loads(data.decode("utf-8"))


async def handle(
    ml_api: MLPredictService, repo: TransactionRepository, record: Transaction
) -> None:
    pass


async def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.environ.get("TOPIC_NAME", "transactions")
    group_id = os.environ.get("GROUP_ID", "transactions-consumer")

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=json_deserializer,
    )
    ml_api = MLPredictService(url="http://ml-api:8000")
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = KafkaTransactionIngestService(
        session_factory=session_factory, ml_api=ml_api, consumer=consumer
    )

    await consumer.start()
    print("Transactions consumer have started...")
    try:
        while True:
            rows = await service.run()
            print("Rows affected: ", rows)
            await consumer.commit()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
