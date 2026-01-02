from abc import ABC, abstractmethod
import uuid
import time
import pandas as pd
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition
from typing import List, Dict, Any, cast, AsyncIterator
from dataclasses import dataclass, field
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio.session import AsyncSession

from pipelines.db.repository import TransactionRepository
from pipelines.csv.reader import CSVReader
from pipelines.services.utils import merge_predictions
from pipelines.services.protocols import MLServiceProtocol
from pipelines.services.models import TransactionRequest, PredictionResponse
from pipelines.observability.utils import measure_stage, ameasure_stage
from pipelines.observability.metrics import (
    INGEST_BATCHES_TOTAL,
    INGEST_ROWS_TOTAL,
    INGEST_ROWS_WRITTEN_TOTAL,
    INGEST_INFLIGHT,
    INGEST_LAST_SUCCESS_TS,
    INGEST_LAST_BATCH_SIZE,
    INGEST_STAGE_DURATION,
)
import structlog


@dataclass
class AbstractTransactionIngestService(ABC):
    session_factory: async_sessionmaker[AsyncSession]
    ml_api: MLServiceProtocol
    logging: structlog.BoundLogger = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.logging = structlog.get_logger().bind(service="csv-ingest")

    @abstractmethod
    def read_batches(self, chunk_size: int = 1000) -> AsyncIterator[pd.DataFrame]: ...

    @property
    @abstractmethod
    def source(self) -> str: ...

    async def run(self) -> int:
        # Run pipeline

        total_rows = 0

        async with self.session_factory() as session:
            repo = TransactionRepository(session=session)

            # 1. Read source in batches
            async for chunk in self.read_batches(chunk_size=1000):
                batch_id = str(uuid.uuid4())
                INGEST_INFLIGHT.labels(source=self.source).set(1)
                batch_t0 = time.perf_counter()
                try:
                    trx = cast(
                        List[TransactionRequest], chunk.to_dict(orient="records")
                    )
                    n = len(trx)
                    self.logging.info("batch_received", batch_size=n, batch_id=batch_id)
                    INGEST_BATCHES_TOTAL.labels(source=self.source).inc()
                    INGEST_ROWS_TOTAL.labels(source=self.source).inc(n)
                    INGEST_LAST_BATCH_SIZE.labels(source=self.source).set(n)
                    # 2. Retrieve predictions for each batch
                    with measure_stage(self.source, "predict"):
                        predictions: List[PredictionResponse] = self.ml_api.predict(trx)

                    with measure_stage(self.source, "merge"):
                        df = merge_predictions(chunk=chunk, predictions=predictions)

                    async with ameasure_stage(self.source, "db_upsert"):
                        affected = await repo.upsert_many(
                            cast(List[Dict[str, Any]], df.to_dict(orient="records"))
                        )
                    # 3. Save transaction data + predicted category to database
                    async with ameasure_stage(self.source, "commit"):
                        await session.commit()

                    total_rows += affected
                    INGEST_ROWS_WRITTEN_TOTAL.labels(source=self.source).inc(affected)
                    INGEST_LAST_SUCCESS_TS.labels(source=self.source).set(time.time())
                except Exception as e:
                    self.logging.exception(
                        "batch_failed", error=str(e), batch_id=batch_id
                    )
                    raise
                finally:
                    dur = time.perf_counter() - batch_t0
                    INGEST_STAGE_DURATION.labels(
                        source=self.source, stage="batch_total"
                    ).observe(dur)
                    INGEST_INFLIGHT.labels(source=self.source).set(0)
                    self.logging.info(
                        "batch_finished", duration_s=dur, batch_id=batch_id
                    )

        self.logging.info("ingest_run_finished", total_rows=total_rows)
        return total_rows


@dataclass
class CSVTransactionIngestService(AbstractTransactionIngestService):
    csv_reader: CSVReader

    @property
    def source(self) -> str:
        return "csv"

    async def read_batches(self, chunk_size: int = 1000) -> AsyncIterator[pd.DataFrame]:
        for chunk in self.csv_reader.read_batches(chunk_size=chunk_size):
            yield chunk


@dataclass
class KafkaTransactionIngestService(AbstractTransactionIngestService):
    consumer: AIOKafkaConsumer

    @property
    def source(self) -> str:
        return "kafka"

    async def read_batches(self, chunk_size: int = 1000) -> AsyncIterator[pd.DataFrame]:
        batches: Dict[TopicPartition, List[ConsumerRecord]] = (
            await self.consumer.getmany(timeout_ms=1000, max_records=chunk_size)
        )
        for _, records in batches.items():
            if records:
                chunk = pd.DataFrame([r.value for r in records])
                yield chunk
