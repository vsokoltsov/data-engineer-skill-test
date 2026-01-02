import os
import time
import asyncio
from uuid import UUID
import json
from typing import Any, Dict
from pydantic import BaseModel
from datetime import datetime

from aiokafka import AIOKafkaConsumer
import structlog
from pipelines.services.ml_api import MLPredictService
from pipelines.db.repository import TransactionRepository
from pipelines.services.batch_ingest import KafkaTransactionIngestService
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from pipelines.observability.http_app import (
    HealthState,
    start_uvicorn,
    create_app,
    stop_uvicorn
)
from pipelines.config import (
    DATABASE_URL,
    ML_API_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
    GROUP_ID,
)
from pipelines.logging import setup_logging

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
    setup_logging("csv-ingest")
    logging = structlog.get_logger().bind(service="kafka")
    state = HealthState(ready=False)
    app = create_app(state)
    obs_host = os.getenv("OBS_HOST", "0.0.0.0")
    obs_port = int(os.getenv("OBS_PORT", "8001"))
    server, server_task = await start_uvicorn(app, obs_host, obs_port)

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
    state.ready = True
    logging.info(f"Consumer started; /ready=200 on :{obs_port}", port=obs_port)
    try:
        while True:
            state.in_progress = True
            state.last_run_ts = time.time()
            rows = await service.run()
            state.last_error = None
            if rows > 0:
                print("Rows affected: ", rows)
                await consumer.commit()
    except Exception as e:
        state.last_error = repr(e)
        logging.exception("kafka consumer exception", error=e)
    finally:
        state.ready = False
        await consumer.stop()
        await engine.dispose()
        logging.info("kafka consumer stopped")
        await stop_uvicorn(server, server_task)


if __name__ == "__main__":
    asyncio.run(main())
