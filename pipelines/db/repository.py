from pipelines.db.models import Transaction

from typing import Any, Iterable, Sequence, Dict
from uuid import UUID

from dataclasses import dataclass
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

@dataclass
class TransactionRepository:
    session: AsyncSession

    async def upsert_many(self, rows: Sequence[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        stmt = pg_insert(Transaction).values(list(rows))
        stmt = stmt.on_conflict_do_update(
            index_elements=[Transaction.id],
            set_={
                "description": stmt.excluded.description,
                "amount": stmt.excluded.amount,
                "timestamp": stmt.excluded.timestamp,
                "merchant": stmt.excluded.merchant,
                "operation_type": stmt.excluded.operation_type,
                "side": stmt.excluded.side,
                "category": stmt.excluded.category,
            },
        )
        res = await self.session.execute(stmt)
        return int(res.rowcount or 0)