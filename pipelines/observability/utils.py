import time
from contextlib import contextmanager, asynccontextmanager
from pipelines.observability.metrics import INGEST_STAGE_DURATION, INGEST_ERRORS_TOTAL


@contextmanager
def measure_stage(source: str, stage: str):
    t0 = time.perf_counter()
    try:
        yield
    except Exception:
        INGEST_ERRORS_TOTAL.labels(source=source, stage=stage).inc()
        raise
    finally:
        INGEST_STAGE_DURATION.labels(source=source, stage=stage).observe(
            time.perf_counter() - t0
        )


@asynccontextmanager
async def ameasure_stage(source: str, stage: str):
    t0 = time.perf_counter()
    try:
        yield
    except Exception:
        INGEST_ERRORS_TOTAL.labels(source=source, stage=stage).inc()
        raise
    finally:
        INGEST_STAGE_DURATION.labels(source=source, stage=stage).observe(
            time.perf_counter() - t0
        )
