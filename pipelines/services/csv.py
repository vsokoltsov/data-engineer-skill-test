from dataclasses import dataclass
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio.session import AsyncSession

from pipelines.services.ml_api import MLPredictService
from pipelines.db.repository import TransactionRepository
from pipelines.csv.reader import CSVReader
from pipelines.services.utils import merge_predictions

@dataclass
class CSVTransactionIngestService:
    session_factory: async_sessionmaker[AsyncSession]
    ml_api: MLPredictService
    csv_reader: CSVReader

    async def run(self) -> int:
        total_rows = 0

        async with self.session_factory() as session:
            repo = TransactionRepository(session=session)

            for chunk in self.csv_reader.read_batches(chunk_size=1000):
                trx = chunk.to_dict(orient='records')
                predictions = self.ml_api.predict(trx)
                df = merge_predictions(chunk=chunk, predictions=predictions)
                affected = await repo.upsert_many(df.to_dict(orient="records"))
                await session.commit()
                total_rows += affected
        return total_rows
