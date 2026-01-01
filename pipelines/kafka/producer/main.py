import asyncio
from pathlib import Path
import json
import os
from typing import Any, Dict

from aiokafka import AIOKafkaProducer
from pipelines.csv.reader import CSVReader


def json_serializer(value: Dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


async def main() -> None:
    ROOT = Path(__file__).parent.parent.parent.parent
    trx_path = os.path.join(ROOT, "data", "transactions_fr.csv")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.environ.get("TOPIC_NAME", "transactions")
    csv_reader = CSVReader(file_path=trx_path)

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=json_serializer,
        # key_serializer=lambda k: k.encode("utf-8"),
    )

    await producer.start()
    try:
        for chunk in csv_reader.read_batches(chunk_size=1000):
            trxs = chunk.to_dict(orient="records")
            for trx in trxs:
                await producer.send_and_wait(topic, value=trx)

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
