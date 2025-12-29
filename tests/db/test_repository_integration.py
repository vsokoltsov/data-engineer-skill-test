import pytest
import pytest_asyncio
from typing import Iterator
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text

from testcontainers.postgres import PostgresContainer

from pipelines.db.repository import TransactionRepository
from pipelines.db.models import Base


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine") as pg:
        sync_url = pg.get_connection_url()
        async_url = sync_url.replace(
            "postgresql+psycopg2://", "postgresql+asyncpg://"
        ).replace("postgresql://", "postgresql+asyncpg://")
        yield async_url


@pytest_asyncio.fixture(scope="session")
async def engine(postgres_url: str):
    eng = create_async_engine(postgres_url, pool_pre_ping=True)
    try:
        async with eng.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        yield eng
    finally:
        await eng.dispose()


@pytest_asyncio.fixture
async def session(engine):
    SessionFactory = async_sessionmaker(engine, expire_on_commit=False)

    async with SessionFactory() as s:
        await s.execute(text("TRUNCATE TABLE transactions"))
        await s.commit()
        yield s


@pytest.mark.integration
@pytest.mark.asyncio
class TestTransactionRepositoryIntegration:
    async def test_upsert_many_inserts_rows(self, session):
        repo = TransactionRepository(session=session)

        rows = [
            {
                "id": UUID("00000000-0000-0000-0000-000000000001"),
                "description": "first",
                "amount": 10.5,
                "timestamp": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).replace(
                    tzinfo=None
                ),
                "merchant": "EDF",
                "operation_type": "payment",
                "side": "debit",
                "category": "Food",
            },
            {
                "id": UUID("00000000-0000-0000-0000-000000000002"),
                "description": "second",
                "amount": 20.0,
                "timestamp": datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc).replace(
                    tzinfo=None
                ),
                "merchant": "Amazon",
                "operation_type": "transfer",
                "side": "debit",
                "category": "Shopping",
            },
        ]

        affected = await repo.upsert_many(rows)
        await session.commit()

        assert affected == 2

        res = await session.execute(text("SELECT count(*) FROM transactions"))
        assert res.scalar_one() == 2

    async def test_upsert_many_updates_on_conflict(self, session):
        repo = TransactionRepository(session=session)

        trx_id = UUID("00000000-0000-0000-0000-000000000001")

        affected_1 = await repo.upsert_many(
            [
                {
                    "id": trx_id,
                    "description": "old",
                    "amount": 10.0,
                    "timestamp": datetime(2024, 1, 1, 0, 0, 0),
                    "merchant": "EDF",
                    "operation_type": "payment",
                    "side": "debit",
                    "category": "Utilities",
                }
            ]
        )
        await session.commit()
        assert affected_1 == 1

        affected_2 = await repo.upsert_many(
            [
                {
                    "id": trx_id,
                    "description": "new",
                    "amount": 99.99,
                    "timestamp": datetime(2024, 2, 1, 0, 0, 0),
                    "merchant": "EDF",
                    "operation_type": "refund",
                    "side": "credit",
                    "category": "Salary Income",
                }
            ]
        )
        await session.commit()

        assert affected_2 == 1

        row = (
            (
                await session.execute(
                    text(
                        """
                SELECT description, amount, operation_type, side, category
                FROM transactions
                WHERE id = :id
            """
                    ),
                    {"id": str(trx_id)},
                )
            )
            .mappings()
            .one()
        )

        assert row["description"] == "new"
        assert float(row["amount"]) == 99.99
        assert row["operation_type"] == "refund"
        assert row["side"] == "credit"
        assert row["category"] == "Salary Income"

    async def test_upsert_many_empty_returns_0(self, session):
        repo = TransactionRepository(session=session)
        affected = await repo.upsert_many([])
        await session.commit()
        assert affected == 0
