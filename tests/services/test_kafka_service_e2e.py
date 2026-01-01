# tests/e2e/test_kafka_ingest_service.py
import json
from typing import Iterator, List
from sqlalchemy import select, func

import pytest
import pytest_asyncio
import httpx
from testcontainers.postgres import PostgresContainer

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from pipelines.services.batch_ingest import KafkaTransactionIngestService
from pipelines.services.models import TransactionRequest, PredictionResponse
from pipelines.db.models import Base, Transaction


# ---- Kafka container (вариант 1: testcontainers.kafka) ----
try:
    from testcontainers.kafka import KafkaContainer
except Exception:
    KafkaContainer = None


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine") as pg:
        sync_url = pg.get_connection_url()
        async_url = sync_url.replace(
            "postgresql+psycopg2://", "postgresql+asyncpg://"
        ).replace("postgresql://", "postgresql+asyncpg://")
        yield async_url


@pytest_asyncio.fixture(scope="session")
async def session_factory(pg_url: str):
    engine = create_async_engine(pg_url, pool_pre_ping=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield async_sessionmaker(engine, expire_on_commit=False)

    await engine.dispose()


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    if KafkaContainer is None:
        pytest.skip(
            "testcontainers.kafka is not available; install testcontainers[kafka] or add KafkaContainer alternative"
        )

    with KafkaContainer() as kafka:
        yield kafka.get_bootstrap_server()


@pytest_asyncio.fixture
async def fake_ml_api_server():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/predict"):
            payload = json.loads(request.content.decode("utf-8"))
            resp = [{"transaction_id": x["id"], "category": "Food"} for x in payload]
            return httpx.Response(200, json=resp)
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport, base_url="http://ml.local")
    yield client
    client.close()


class HttpxMLPredictService:
    def __init__(self, client: httpx.Client):
        self.client = client

    def predict(self, trx: List[TransactionRequest]) -> List[PredictionResponse]:
        r = self.client.post("/predict", json=trx)
        r.raise_for_status()
        return r.json()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_kafka_ingest_service_inserts_rows(
    session_factory,
    fake_ml_api_server,
    kafka_bootstrap: str,
):
    topic = "transactions"

    # 1) Producer: send messages
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    await producer.start()
    try:
        await producer.send_and_wait(
            topic,
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "description": "hello",
                "amount": 10.5,
                "timestamp": "2024-01-01T00:00:00",
                "merchant": None,
                "operation_type": "payment",
                "side": "debit",
            },
        )
        await producer.send_and_wait(
            topic,
            {
                "id": "00000000-0000-0000-0000-000000000002",
                "description": "world",
                "amount": 20.0,
                "timestamp": "2024-01-02T00:00:00",
                "merchant": "Amazon",
                "operation_type": "transfer",
                "side": "debit",
            },
        )
    finally:
        await producer.stop()

    # 2) Consumer for service
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap,
        group_id="test-transactions-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    await consumer.start()
    try:
        ml_api = HttpxMLPredictService(fake_ml_api_server)

        svc = KafkaTransactionIngestService(
            session_factory=session_factory,
            ml_api=ml_api,
            consumer=consumer,
        )

        affected = await svc.run()

        assert affected == 2

        async with session_factory() as s:
            res = await s.execute(select(func.count()).select_from(Transaction))
            assert res.scalar_one() == 2

    finally:
        await consumer.stop()
