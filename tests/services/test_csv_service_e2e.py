import json
from pathlib import Path
from typing import Iterator, List
from sqlalchemy import select, func

import pytest
import pytest_asyncio
import httpx
from testcontainers.postgres import PostgresContainer

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from pipelines.services.csv import CSVTransactionIngestService
from pipelines.csv.reader import CSVReader
from pipelines.services.models import TransactionRequest, PredictionResponse
from pipelines.db.models import Base, Transaction


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine") as pg:
        sync_url = pg.get_connection_url()
        async_url = sync_url.replace(
            "postgresql+psycopg2://", "postgresql+asyncpg://"
        ).replace("postgresql://", "postgresql+asyncpg://")
        yield async_url


@pytest_asyncio.fixture(scope="session")
async def session_factory(pg_url: str):
    engine = create_async_engine(pg_url, pool_pre_ping=True)

    # создаём таблицы (если нет alembic)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield async_sessionmaker(engine, expire_on_commit=False)

    await engine.dispose()


@pytest_asyncio.fixture
async def fake_ml_api_server():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/predict"):
            payload = json.loads(request.content.decode("utf-8"))
            resp = [{"transaction_id": x["id"], "category": "Food"} for x in payload]
            return httpx.Response(200, json=resp)
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport, base_url="http://ml.local")
    yield client
    client.close()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_csv_ingest_service_inserts_rows(
    tmp_path: Path, session_factory, fake_ml_api_server
):
    csv_path = tmp_path / "trx.csv"
    csv_path.write_text(
        "id;description;amount;timestamp;merchant;operation_type;side\n"
        "1;hello;10,50;2024-01-01 00:00:00;;payment;debit\n"
        "2;world;20,00;2024-01-02 00:00:00;Amazon;transfer;debit\n"
    )

    csv_reader = CSVReader(file_path=str(csv_path))

    class HttpxMLPredictService:
        def __init__(self, client: httpx.Client):
            self.client = client

        def predict(self, trx: List[TransactionRequest]) -> List[PredictionResponse]:
            r = self.client.post("/predict", json=trx)
            r.raise_for_status()
            return r.json()

    ml_api = HttpxMLPredictService(fake_ml_api_server)

    svc = CSVTransactionIngestService(
        session_factory=session_factory,
        ml_api=ml_api,
        csv_reader=csv_reader,
    )

    affected = await svc.run()

    assert affected == 2
    async with session_factory() as s:
        res = await s.execute(select(func.count()).select_from(Transaction))
        assert res.scalar_one() == 2
