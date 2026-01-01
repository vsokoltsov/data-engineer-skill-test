import asyncio
from uuid import UUID
import json
from typing import Any, Dict
from pydantic import BaseModel
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from pipelines.services.ml_api import MLPredictService
from pipelines.db.repository import TransactionRepository
from pipelines.services.batch_ingest import KafkaTransactionIngestService
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from pipelines.config import (
    DATABASE_URL,
    ML_API_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
    GROUP_ID,
)

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
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=json_deserializer,
    )
    ml_api = MLPredictService(url=ML_API_URL)
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
