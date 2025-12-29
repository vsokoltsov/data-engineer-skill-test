import pytest
from unittest.mock import AsyncMock, Mock
from sqlalchemy.dialects import postgresql

from pipelines.db.repository import TransactionRepository


class DummyResult:
    def __init__(self, rowcount):
        self.rowcount = rowcount


@pytest.mark.asyncio
@pytest.mark.unit
class TestTransactionRepository:

    async def test_upsert_many_empty_rows_does_not_call_execute(self):
        session = Mock()
        session.execute = AsyncMock()

        repo = TransactionRepository(session=session)
        res = await repo.upsert_many([])

        assert res == 0
        session.execute.assert_not_called()

    async def test_upsert_many_returns_rowcount(self):
        session = Mock()
        session.execute = AsyncMock(return_value=DummyResult(rowcount=123))

        repo = TransactionRepository(session=session)
        res = await repo.upsert_many([{"id": "00000000-0000-0000-0000-000000000001"}])

        assert res == 123
        session.execute.assert_awaited_once()

    async def test_upsert_many_rowcount_none_returns_0(self):
        session = Mock()
        session.execute = AsyncMock(return_value=DummyResult(rowcount=None))

        repo = TransactionRepository(session=session)
        res = await repo.upsert_many([{"id": "00000000-0000-0000-0000-000000000001"}])

        assert res == 0
        session.execute.assert_awaited_once()

    async def test_upsert_many_builds_on_conflict_update_stmt(self):
        session = Mock()
        session.execute = AsyncMock(return_value=DummyResult(rowcount=1))

        repo = TransactionRepository(session=session)

        rows = [
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "description": "x",
                "amount": 1.23,
                "timestamp": "2024-01-01T00:00:00",
                "merchant": None,
                "operation_type": "payment",
                "side": "debit",
                "category": "Food",
            }
        ]

        await repo.upsert_many(rows)

        (stmt,), _kwargs = session.execute.await_args

        compiled = str(
            stmt.compile(
                dialect=postgresql.dialect(),
            )
        ).lower()

        assert "on conflict" in compiled
        assert "do update set" in compiled

        for col in [
            "description",
            "amount",
            "timestamp",
            "merchant",
            "operation_type",
            "side",
            "category",
        ]:
            assert f"{col} = excluded.{col}".lower() in compiled
