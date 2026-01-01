from abc import ABC, abstractmethod
import pandas as pd
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition
from typing import List, Dict, Any, cast, AsyncIterator
from dataclasses import dataclass
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio.session import AsyncSession

from pipelines.db.repository import TransactionRepository
from pipelines.csv.reader import CSVReader
from pipelines.services.utils import merge_predictions
from pipelines.services.protocols import MLServiceProtocol
from pipelines.services.models import TransactionRequest, PredictionResponse


@dataclass
class AbstractTransactionIngestService(ABC):
    session_factory: async_sessionmaker[AsyncSession]
    ml_api: MLServiceProtocol

    @abstractmethod
    def read_batches(self, chunk_size: int = 1000) -> AsyncIterator[pd.DataFrame]: ...

    async def run(self) -> int:
        # Run pipeline

        total_rows = 0

        async with self.session_factory() as session:
            repo = TransactionRepository(session=session)

            # 1. Read source in batches
            async for chunk in self.read_batches(chunk_size=1000):
                trx = cast(List[TransactionRequest], chunk.to_dict(orient="records"))
                # 2. Retrieve predictions for each batch
                predictions: List[PredictionResponse] = self.ml_api.predict(trx)
                df = merge_predictions(chunk=chunk, predictions=predictions)
                affected = await repo.upsert_many(
                    cast(List[Dict[str, Any]], df.to_dict(orient="records"))
                )
                # 3. Save transaction data + predicted category to database
                await session.commit()
                total_rows += affected
        return total_rows


@dataclass
class CSVTransactionIngestService(AbstractTransactionIngestService):
    csv_reader: CSVReader

    async def read_batches(self, chunk_size: int = 1000) -> AsyncIterator[pd.DataFrame]:
        for chunk in self.csv_reader.read_batches(chunk_size=1000):
            yield chunk


@dataclass
class KafkaTransactionIngestService(AbstractTransactionIngestService):
    consumer: AIOKafkaConsumer

    async def read_batches(self, chunk_size: int = 1000) -> AsyncIterator[pd.DataFrame]:
        batches: Dict[TopicPartition, List[ConsumerRecord]] = (
            await self.consumer.getmany(timeout_ms=1000, max_records=1000)
        )
        for _, records in batches.items():
            if records:
                chunk = pd.DataFrame([r.value for r in records])
                yield chunk
