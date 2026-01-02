import os
import uuid
import json
import time
import subprocess
from pathlib import Path

import pytest
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer


COMPOSE_FILE = os.path.join(Path(__file__).resolve().parents[2], "docker-compose.yml")

TOPIC = "transactions"
BOOTSTRAP = "localhost:29092"
PG_DSN = "dbname=transactions user=app password=app host=localhost port=5432"
DETACHED = os.getenv("DETACHED", False)


def wait_until(predicate, timeout=30, interval=0.5, err="timeout"):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(err)


@pytest.mark.functional
def test_consumer_entrypoint_inserts_rows():
    subprocess.check_call(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "up",
            "-d",
            "--build",
            "postgres",
            "kafka",
            "zookeeper",
            "schema-registry",
            "init-topics",
            "register-schemas",
            "consumer",
            "ml-api",
        ]
    )

    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

        def pg_ready():
            try:
                conn = psycopg2.connect(PG_DSN)
                conn.close()
                return True
            except Exception:
                return False

        wait_until(pg_ready, timeout=60, err="Postgres not ready")

        def table_exists() -> bool:
            with psycopg2.connect(PG_DSN) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass('public.transactions')")
                    row = cur.fetchone()
                    if not row:
                        return False
                    return row[0] is not None

        wait_until(
            table_exists,
            timeout=90,
            err="transactions table not created (alembic not applied)",
        )

        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE transactions")
            conn.commit()

        trx1_id = uuid.uuid4()
        trx2_id = uuid.uuid4()
        expected = {
            str(trx1_id): {
                "description": "hello",
                "amount": 10.5,
                "merchant": "EDF",
                "operation_type": "payment",
                "side": "debit",
            },
            str(trx2_id): {
                "description": "world",
                "amount": 20.0,
                "merchant": "Amazon",
                "operation_type": "transfer",
                "side": "debit",
            },
        }
        payloads = [
            {
                "id": str(trx1_id),
                "description": "hello",
                "amount": 10.5,
                "timestamp": "2024-01-01T00:00:00",
                "merchant": "EDF",
                "operation_type": "payment",
                "side": "debit",
            },
            {
                "id": str(trx2_id),
                "description": "world",
                "amount": 20.0,
                "timestamp": "2024-01-02T00:00:00",
                "merchant": "Amazon",
                "operation_type": "transfer",
                "side": "debit",
            },
        ]

        for msg in payloads:
            producer.send(
                TOPIC, value=json.dumps(msg).encode("utf-8")
            )  # <-- см. ниже про JSON
        producer.flush()

        def rows_inserted():
            with psycopg2.connect(PG_DSN) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT count(*) AS c FROM transactions")
                    row = cur.fetchone()
                    if not row:
                        return False
                    qresult = row.get("c", 0)
                    return qresult >= 2

        wait_until(rows_inserted, timeout=60, err="Rows were not inserted")

        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id::text, description, amount, merchant, operation_type, side
                    FROM transactions
                    WHERE id = ANY(%s::uuid[])
                    ORDER BY id
                    """,
                    (list(expected.keys()),),
                )
                rows = cur.fetchall()

        assert len(rows) == len(expected)

        for r in rows:
            exp = expected[r["id"]]
            assert r["description"] == exp["description"]
            assert float(r["amount"]) == exp["amount"]
            assert r["merchant"] == exp["merchant"]
            assert r["operation_type"] == exp["operation_type"]
            assert r["side"] == exp["side"]

    finally:
        if not DETACHED:
            subprocess.call(
                ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"]
            )
