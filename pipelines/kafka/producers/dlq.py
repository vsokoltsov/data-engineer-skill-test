import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from aiokafka import AIOKafkaProducer


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _aiokafka_headers(headers: Optional[dict[str, Any]]) -> list[tuple[str, bytes]]:
    """
    aiokafka headers: List[Tuple[str, bytes]]
    """
    if not headers:
        return []
    out: list[tuple[str, bytes]] = []
    for k, v in headers.items():
        if v is None:
            out.append((k, b""))
        elif isinstance(v, (bytes, bytearray)):
            out.append((k, bytes(v)))
        else:
            out.append((k, str(v).encode("utf-8")))
    return out


@dataclass
class DLQPublisher:
    producer: AIOKafkaProducer
    dlq_topic: str
    service: str  # "consumer" / "csv-ingest" и т.п.

    async def publish(
        self,
        *,
        original: Any,
        err: Exception,
        stage: str,
        source: str,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
        headers: dict[str, Any] | None = None,
        key: str | bytes | None = None,
    ) -> None:
        event = {
            "topic": topic or "",
            "partition": partition,
            "offset": offset,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "service": self.service,
            "source": source,  # "kafka" / "csv"
            "stage": stage,  # "validate" / "predict" / "db_upsert" / ...
            "error_type": type(err).__name__,
            "error_message": str(err),
            "payload": original,  # оставляем объектом — сериализуем ниже
            "headers": headers or {},  # то же самое
        }

        value = _json_bytes(event)

        # key в Kafka тоже bytes (если хочешь, чтобы все ошибки одной транзакции шли в одну партицию)
        if isinstance(key, str):
            key_b = key.encode("utf-8")
        elif isinstance(key, (bytes, bytearray)):
            key_b = bytes(key)
        else:
            key_b = None

        await self.producer.send_and_wait(
            self.dlq_topic,
            value=value,
            key=key_b,
            headers=_aiokafka_headers(headers),
        )
