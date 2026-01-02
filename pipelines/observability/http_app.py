from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


@dataclass
class HealthState:
    ready: bool = False
    in_progress: bool = False
    last_error: Optional[str] = None
    last_run_ts: Optional[float] = None  # time.time()


def create_app(state: HealthState) -> FastAPI:
    app = FastAPI(title="observability", docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/metrics")
    def metrics() -> Response:
        payload = generate_latest()
        return Response(content=payload, media_type=CONTENT_TYPE_LATEST)

    @app.get("/health")
    def health() -> dict:
        out = {"status": "ok"}
        if state.last_error:
            out["last_error"] = state.last_error
        return out

    @app.get("/ready")
    def ready() -> Response:
        status_code = 200 if state.ready else 503
        body = {
            "ready": state.ready,
            "in_progress": state.in_progress,
            "last_error": state.last_error,
            "last_run_ts": state.last_run_ts,
        }
        return Response(content=_json(body), media_type="application/json", status_code=status_code)

    return app


def _json(obj: dict) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)


async def start_uvicorn(app: FastAPI, host: str, port: int):
    import uvicorn

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)

    task = __import__("asyncio").create_task(server.serve())
    return server, task


async def stop_uvicorn(server, task):
    server.should_exit = True
    try:
        await task
    except Exception:
        pass