from abc import ABC, abstractmethod
import asyncio
import uuid
import time
import pandas as pd
from sqlalchemy.exc import DBAPIError
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
from pipelines.services.errors import MLAPIServiceError
from pipelines.services.models import TransactionRequest, PredictionResponse
from pipelines.observability.utils import measure_stage, ameasure_stage
from pipelines.services.validator import TransactionValidator
from pipelines.services.quality import BatchQuality
from pipelines.kafka.producers.dlq import DLQPublisher
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
    quality_service: BatchQuality
    dlq: DLQPublisher
    stop_on_error: bool = True
    max_consecutive_failures: int = 3
    backoff_on_failure_s: float = 0.0
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
        consecutive_failures = 0

        async with self.session_factory() as session:
            repo = TransactionRepository(session=session)

            # 1. Read source in batches
            async for chunk in self.read_batches(chunk_size=1000):
                batch_id = str(uuid.uuid4())
                INGEST_INFLIGHT.labels(source=self.source).set(1)
                batch_t0 = time.perf_counter()
                current_chunk = chunk
                current_stage = "read"
                try:
                    # Validate batch
                    current_stage = "validate"
                    trx_checked = TransactionValidator.validate_rows(rows=chunk)
                    if len(trx_checked["dlqs"]) > 0:
                        self.logging.error(
                            "dlqs_records",
                            batch_id=batch_id,
                            ids=list(trx_checked["dlqs"]["id"]),
                        )
                        # if there are dirty records - put them in DLQ
                        for row in trx_checked["dlqs"].to_dict(orient="records"):
                            self.logging.error(
                                "dlqs_record_error",
                                batch_id=batch_id,
                                err=row["validation_error"],
                            )
                            await self.dlq.publish(
                                original=row,
                                err=row["validation_error"],
                                stage=current_stage,
                                source=self.source,
                                topic="transactions",
                            )
                    current_chunk = trx_checked["valid"]
                    # Verify quality of data
                    current_stage = "quality_check"
                    report = self.quality_service.verify(df=trx_checked["valid"])
                    if report.issues_rate > self.quality_service.threshold:
                        self.logging.error(
                            "data_quality_low",
                            batch_id=batch_id,
                            rate=report.issues_rate,
                            threshold=self.quality_service.threshold,
                        )

                    trx = cast(
                        List[TransactionRequest],
                        trx_checked["valid"].to_dict(orient="records"),
                    )

                    n = len(trx)
                    if n == 0:
                        # Skip batch, if there are no records
                        continue

                    self.logging.info("batch_received", batch_size=n, batch_id=batch_id)
                    INGEST_BATCHES_TOTAL.labels(source=self.source).inc()
                    INGEST_ROWS_TOTAL.labels(source=self.source).inc(n)
                    INGEST_LAST_BATCH_SIZE.labels(source=self.source).set(n)
                    # 2. Retrieve predictions for each batch
                    current_stage = "predict"
                    with measure_stage(self.source, "predict"):
                        predictions: List[PredictionResponse] = self.ml_api.predict(trx)

                    current_stage = "merge"
                    with measure_stage(self.source, "merge"):
                        df = merge_predictions(
                            chunk=trx_checked["valid"], predictions=predictions
                        )

                    current_stage = "db_upsert"
                    async with ameasure_stage(self.source, "db_upsert"):
                        affected = await repo.upsert_many(
                            cast(List[Dict[str, Any]], df.to_dict(orient="records"))
                        )
                    # 3. Save transaction data + predicted category to database
                    current_stage = "commit"
                    async with ameasure_stage(self.source, "commit"):
                        await session.commit()

                    total_rows += affected
                    INGEST_ROWS_WRITTEN_TOTAL.labels(source=self.source).inc(affected)
                    INGEST_LAST_SUCCESS_TS.labels(source=self.source).set(time.time())
                    consecutive_failures = 0
                except Exception as e:
                    consecutive_failures += 1

                    if isinstance(e, DBAPIError):
                        self.logging.exception(
                            "db_upsert_error",
                            statement=getattr(e, "statement", None),
                            params=getattr(e, "params", None),
                            orig=repr(getattr(e, "orig", None)),
                            batch_id=batch_id,
                            stage=current_stage,
                        )
                    elif isinstance(e, MLAPIServiceError):
                        self.logging.exception(
                            "ml_api_service_error",
                            orig=repr(e.orig),
                            batch_id=batch_id,
                            stage=current_stage,
                        )
                    else:
                        self.logging.exception(
                            "batch_failed",
                            error=str(e),
                            batch_id=batch_id,
                            stage=current_stage,
                        )

                    await self._send_chunk_to_dlq(
                        chunk=current_chunk,
                        error=e,
                        stage=current_stage,
                        batch_id=batch_id,
                    )

                    try:
                        await session.rollback()
                    except Exception:
                        self.logging.exception(
                            "session_rollback_failed", batch_id=batch_id
                        )

                    if self.backoff_on_failure_s > 0:
                        await asyncio.sleep(self.backoff_on_failure_s)

                    if self._should_stop(e, current_stage, consecutive_failures):
                        raise
                    continue
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

    async def _send_chunk_to_dlq(
        self,
        chunk: pd.DataFrame,
        error: Exception,
        stage: str,
        batch_id: str,
    ) -> None:
        """Helper method to send all records from a chunk to DLQ."""
        try:
            records = chunk.to_dict(orient="records")
            for record in records:
                await self.dlq.publish(
                    original=record,
                    err=error,
                    stage=stage,
                    source=self.source,
                    topic="transactions",
                )
            self.logging.info(
                "chunk_sent_to_dlq",
                batch_id=batch_id,
                stage=stage,
                records_count=len(records),
                error_type=type(error).__name__,
            )
        except Exception as dlq_error:
            # If DLQ publishing fails, log but don't raise to avoid masking original error
            self.logging.exception(
                "dlq_publish_failed",
                batch_id=batch_id,
                original_error=str(error),
                dlq_error=str(dlq_error),
            )

    def _is_db_data_error(self, e: DBAPIError) -> bool:
        orig = e.orig
        name = orig.__class__.__name__ if orig else ""
        msg = str(orig) if orig else str(e)

        data_markers = (
            "invalid input",
            "DataError",
            "cannot cast",
            "value too long",
            "numeric field overflow",
        )
        return ("DataError" in name) or any(m in msg for m in data_markers)

    def _should_stop(self, e: Exception, stage: str, consecutive_failures: int) -> bool:
        if consecutive_failures >= self.max_consecutive_failures:
            return True

        if isinstance(e, DBAPIError):
            if self._is_db_data_error(e):
                return False
            return True

        if isinstance(e, MLAPIServiceError):
            return self.stop_on_error

        return self.stop_on_error


@dataclass
class CSVTransactionIngestService(AbstractTransactionIngestService):
    csv_reader: CSVReader = field(default=...)  # type: ignore[assignment]

    @property
    def source(self) -> str:
        return "csv"

    async def read_batches(self, chunk_size: int = 1000) -> AsyncIterator[pd.DataFrame]:
        for chunk in self.csv_reader.read_batches(chunk_size=chunk_size):
            yield chunk


@dataclass
class KafkaTransactionIngestService(AbstractTransactionIngestService):
    consumer: AIOKafkaConsumer = field(default=...)  # type: ignore[assignment]

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
