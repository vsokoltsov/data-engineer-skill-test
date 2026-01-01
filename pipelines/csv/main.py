import os
import asyncio
from pathlib import Path
from pipelines.csv.reader import CSVReader
from pipelines.services.batch_ingest import CSVTransactionIngestService
from pipelines.services.ml_api import MLPredictService
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

DATABASE_URL = "postgresql+asyncpg://app:app@localhost:5432/transactions"

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


async def main():
    ROOT = Path(__file__).parent.parent.parent
    trx_path = os.path.join(ROOT, "data", "transactions_fr.csv")

    ml_api = MLPredictService(url="http://localhost:8000")
    reader = CSVReader(file_path=trx_path)
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = CSVTransactionIngestService(
        session_factory=session_factory,
        ml_api=ml_api,
        csv_reader=reader,
    )

    rows = await service.run()
    print("Rows inserted: ", rows)


if __name__ == "__main__":
    asyncio.run(main())
