import asyncio
import json
from typing import Any, Dict
from dotenv import load_dotenv

from aiokafka import AIOKafkaProducer
from pipelines.csv.reader import CSVReader
from pipelines.config import TRANSACTIONS_PATH, KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME


def json_serializer(value: Dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


async def main() -> None:
    load_dotenv()
    csv_reader = CSVReader(file_path=TRANSACTIONS_PATH)

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
    )

    await producer.start()
    try:
        for chunk in csv_reader.read_batches(chunk_size=1000):
            trxs = chunk.to_dict(orient="records")
            for trx in trxs:
                await producer.send_and_wait(TOPIC_NAME, value=trx)

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
