import os
import asyncio
import time
import structlog
from pipelines.csv.reader import CSVReader
from pipelines.services.batch_ingest import CSVTransactionIngestService
from pipelines.services.ml_api import MLPredictService
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from pipelines.config import (
    DATABASE_URL,
    TRANSACTIONS_PATH,
    ML_API_URL,
    OBS_HOST,
    OBS_PORT
)
from pipelines.observability.http_app import (
    HealthState,
    create_app,
    start_uvicorn,
    stop_uvicorn,
)
from pipelines.logging import setup_logging
from dotenv import load_dotenv

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


async def main():
    load_dotenv()
    setup_logging("csv-ingest")
    logging = structlog.get_logger().bind(service="csv-ingest")

    state = HealthState(ready=False)
    app = create_app(state)
    server, server_task = await start_uvicorn(
        app,
        host=OBS_HOST,
        port=OBS_PORT,
    )

    ml_api = MLPredictService(url=ML_API_URL)
    reader = CSVReader(file_path=TRANSACTIONS_PATH)
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    service = CSVTransactionIngestService(
        session_factory=session_factory,
        ml_api=ml_api,
        csv_reader=reader,
    )

    try:
        state.ready = True
        state.in_progress = True
        state.last_run_ts = time.time()

        await service.run()
        state.last_error = None
    except Exception as e:
        state.last_error = repr(e)
        logging.exception("csv-ingestion script error", error=e)
        raise
    finally:
        state.in_progress = False
        state.ready = False
        await asyncio.sleep(15)
        await engine.dispose()
        logging.info("csv-ingestion finished execution")
        await stop_uvicorn(server, server_task)


if __name__ == "__main__":
    asyncio.run(main())
