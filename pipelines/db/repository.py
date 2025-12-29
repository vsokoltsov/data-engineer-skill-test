from pipelines.db.models import Transaction

from typing import Any, Sequence, Dict, cast

from dataclasses import dataclass
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.engine import CursorResult


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
        res = cast(CursorResult[Any], await self.session.execute(stmt))
        return int(res.rowcount or 0)
