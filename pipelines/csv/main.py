import os
import asyncio
import pandas as pd
from pathlib import Path
from pipelines.csv.reader import read_transactions
from pipelines.external.ml import MLPredictService, TransactionRequest
from pipelines.db.repository import TransactionRepository
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

DATABASE_URL = "postgresql+asyncpg://app:app@localhost:5432/transactions"

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)

async def main():
    ROOT = Path(__file__).parent.parent.parent
    ml_api = MLPredictService(url="http://localhost:8000")
    trx_path = os.path.join(ROOT, "data", "transactions_fr.csv")
    async with SessionFactory() as session:
        repo = TransactionRepository(session)
        for chunk in read_transactions(path=trx_path, chunk_size=1000):
            trx = chunk.to_dict(orient='records')
            predictions = ml_api.predict(trx)
            df_pred = pd.DataFrame(predictions)
            df = chunk.merge(
                df_pred,
                left_on="id",
                right_on="transaction_id",
                how="left",
                validate="one_to_one"
            ).drop(columns=["transaction_id"])
            df['timestamp'] = pd.to_datetime(df["timestamp"], errors="raise")
            rows_affected = await repo.upsert_many(df.to_dict(orient='records'))
            await session.commit()
    print("Rows inserted: ", rows_affected)

if __name__ == '__main__':
    asyncio.run(main())

        