import asyncio
from pipelines.csv.reader import CSVReader
from pipelines.services.batch_ingest import CSVTransactionIngestService
from pipelines.services.ml_api import MLPredictService
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from pipelines.config import DATABASE_URL, TRANSACTIONS_PATH, ML_API_URL

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


async def main():
    ml_api = MLPredictService(url=ML_API_URL)
    reader = CSVReader(file_path=TRANSACTIONS_PATH)
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
