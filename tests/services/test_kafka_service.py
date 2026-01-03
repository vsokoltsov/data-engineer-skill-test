# tests/unit/test_kafka_ingest_read_batches.py
import pandas as pd
import pytest
from unittest.mock import Mock, AsyncMock
from aiokafka.structs import TopicPartition

import pipelines.services.batch_ingest as mod
from pipelines.services.batch_ingest import KafkaTransactionIngestService
from pipelines.services.quality import BatchQuality
from pipelines.kafka.producers.dlq import DLQPublisher


class FakeSession:
    def __init__(self):
        self.commits = 0

    async def commit(self):
        self.commits += 1


class FakeSessionCM:
    def __init__(self, session: FakeSession):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSessionFactory:
    def __init__(self, session: FakeSession):
        self.session = session

    def __call__(self):
        return FakeSessionCM(self.session)


class DummyConsumer:
    def __init__(self, records: list[dict]):
        self.records = records

    async def getmany(self, timeout_ms: int, max_records: int):
        # emulating: {tp: [ConsumerRecord, ...]}
        class R:
            def __init__(self, value):
                self.value = value

        return {("transactions", 0): [R(x) for x in self.records]}


class FakeML:
    def __init__(self):
        self.calls = []

    def predict(self, trx):
        self.calls.append(trx)
        # prediction format like in ml service (transaction_id, category)
        return [{"transaction_id": x["id"], "category": "Food"} for x in trx]


class FakeRepo:
    def __init__(self, session):
        self.session = session
        self.payloads = []

    async def upsert_many(self, payload):
        self.payloads.append(payload)
        return len(payload)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_kafka_read_batches_yields_dataframe():
    class R:
        def __init__(self, value):
            self.value = value

    tp = TopicPartition(topic="transactions", partition=0)

    consumer = type("C", (), {})()

    async def getmany(timeout_ms: int, max_records: int):
        return {
            tp: [
                R({"id": "1", "amount": 10}),
                R({"id": "2", "amount": 20}),
            ]
        }

    setattr(consumer, "getmany", getmany)

    class DummyML:
        def predict(self, trx):
            return []

    async def fake_session_factory():
        raise RuntimeError("not used")

    # Mock metrics to avoid errors
    mock_metrics = {
        "INGEST_INFLIGHT": Mock(),
        "INGEST_BATCHES_TOTAL": Mock(),
        "INGEST_ROWS_TOTAL": Mock(),
        "INGEST_LAST_BATCH_SIZE": Mock(),
        "INGEST_ROWS_WRITTEN_TOTAL": Mock(),
        "INGEST_LAST_SUCCESS_TS": Mock(),
        "INGEST_STAGE_DURATION": Mock(),
    }
    for name, mock_obj in mock_metrics.items():
        mock_obj.labels.return_value = mock_obj
        setattr(mod, name, mock_obj)

    quality_service = BatchQuality(threshold=0.1)
    dlq = Mock(spec=DLQPublisher)
    dlq.publish = AsyncMock()

    svc = KafkaTransactionIngestService(
        session_factory=fake_session_factory,  # type: ignore[arg-type]
        ml_api=DummyML(),  # type: ignore[arg-type]
        quality_service=quality_service,
        dlq=dlq,
        consumer=consumer,  # type: ignore[arg-type]
    )

    chunks = []
    async for df in svc.read_batches(chunk_size=1000):
        chunks.append(df)

    assert len(chunks) == 1
    assert chunks[0].to_dict(orient="records") == [
        {"id": "1", "amount": 10},
        {"id": "2", "amount": 20},
    ]


@pytest.mark.asyncio
@pytest.mark.unit
async def test_kafka_ingest_run_happy_path(monkeypatch):
    # 1) patch TransactionRepository -> FakeRepo
    monkeypatch.setattr(mod, "TransactionRepository", lambda session: FakeRepo(session))

    # 2) patch merge_predictions -> deterministic result
    def fake_merge_predictions(chunk: pd.DataFrame, predictions):
        # добавим column predicted_category
        df = chunk.copy()
        pred_map = {p["transaction_id"]: p["category"] for p in predictions}
        df["predicted_category"] = df["id"].map(pred_map)
        return df

    monkeypatch.setattr(mod, "merge_predictions", fake_merge_predictions)

    # 3) Mock metrics
    mock_metrics = {
        "INGEST_INFLIGHT": Mock(),
        "INGEST_BATCHES_TOTAL": Mock(),
        "INGEST_ROWS_TOTAL": Mock(),
        "INGEST_LAST_BATCH_SIZE": Mock(),
        "INGEST_ROWS_WRITTEN_TOTAL": Mock(),
        "INGEST_LAST_SUCCESS_TS": Mock(),
        "INGEST_STAGE_DURATION": Mock(),
    }
    for name, mock_obj in mock_metrics.items():
        mock_obj.labels.return_value = mock_obj
        monkeypatch.setattr(f"pipelines.services.batch_ingest.{name}", mock_obj)

    # Arrange
    session = FakeSession()
    session_factory = FakeSessionFactory(session)

    # Test data needs required fields for validation
    consumer = DummyConsumer(
        records=[
            {
                "id": "1",
                "description": "Test transaction 1",
                "amount": -10.0,  # Negative for debit
                "side": "debit",
                "timestamp": "2024-01-01T00:00:00",
                "merchant": "Test Merchant",
                "operation_type": "card",
            },
            {
                "id": "2",
                "description": "Test transaction 2",
                "amount": 20.0,  # Positive for credit
                "side": "credit",
                "timestamp": "2024-01-02T00:00:00",
                "merchant": "Test Merchant 2",
                "operation_type": "transfer",
            },
        ]
    )
    ml_api = FakeML()
    quality_service = BatchQuality(threshold=0.1)
    dlq = Mock(spec=DLQPublisher)
    dlq.publish = AsyncMock()

    svc = KafkaTransactionIngestService(
        session_factory=session_factory,  # type: ignore[arg-type]
        ml_api=ml_api,  # type: ignore[arg-type]
        quality_service=quality_service,
        dlq=dlq,
        consumer=consumer,  # type: ignore[arg-type]
    )

    # Act
    affected = await svc.run()

    # Assert
    assert affected == 2
    assert session.commits == 1

    assert len(ml_api.calls) == 1
    # Check that valid transactions were passed to ML API
    assert len(ml_api.calls[0]) == 2
    assert ml_api.calls[0][0]["id"] == "1"
    assert ml_api.calls[0][1]["id"] == "2"
