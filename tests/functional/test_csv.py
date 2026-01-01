import os
import uuid
import json
import time
import subprocess
import pandas as pd
from pathlib import Path

import pytest
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
from pipelines.config import DATA_PATH


COMPOSE_FILE = os.path.join(Path(__file__).resolve().parents[2], "docker-compose.yml")

TOPIC = "transactions"
BOOTSTRAP = "localhost:29092"
PG_DSN = "dbname=transactions user=app password=app host=localhost port=5432"
DETACHED = os.getenv('DETACHED', False)


def wait_until(predicate, timeout=30, interval=0.5, err="timeout"):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(err)


@pytest.mark.e2e
def test_csv_entrypoint_inserts_rows():
    with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE transactions")
            conn.commit()

    trx_file_path = os.path.join(DATA_PATH, "dumb_csv.csv")
    trx_container_path = os.path.join("/app", "data", "dumb_csv.csv")
    trx1_id = 1
    trx2_id = 2
    trx1_uuid = uuid.UUID(int=trx1_id)
    trx2_uuid = uuid.UUID(int=trx2_id)
    trxs = [
        {
            "id": 1,
            "description": "hello",
            "amount": 10.5,
            "timestamp": "2024-01-01T00:00:00",
            "merchant": None,
            "operation_type": "payment",
            "side": "debit",
        },
        {
            "id": 2,
            "description": "world",
            "amount": 20.0,
            "timestamp": "2024-01-02T00:00:00",
            "merchant": "Amazon",
            "operation_type": "transfer",
            "side": "debit",
        },
    ]
    df = pd.DataFrame(trxs)
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    cols = ["id", "description", "amount", "timestamp", "merchant", "operation_type", "side"]
    df[cols].to_csv(
        trx_file_path,
        sep=";",
        index=False,
        decimal=",",
        quotechar='"'
    )
    subprocess.check_call([
        "docker", 
        "compose", 
        "-f",
        str(COMPOSE_FILE),
        "run", 
        "--rm",
        "-e", f"TRXS_PATH={trx_container_path}",
        "csv-ingest",
        "python", "-m", "pipelines.csv.main",
    ])

    try:
        def table_exists() -> bool:
            with psycopg2.connect(PG_DSN) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass('public.transactions')")
                    row = cur.fetchone()
                    if not row:
                        return False
                    return row[0] is not None

        wait_until(table_exists, timeout=90, err="transactions table not created (alembic not applied)")

        expected = {
            str(trx1_uuid): {
                "description": "hello",
                "amount": 10.5,
                "merchant": None,
                "operation_type": "payment",
                "side": "debit",
            },
            str(trx2_uuid): {
                "description": "world",
                "amount": 20.0,
                "merchant": "Amazon",
                "operation_type": "transfer",
                "side": "debit",
            },
        }

        def rows_inserted():
            with psycopg2.connect(PG_DSN) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT count(*) AS c FROM transactions")
                    row = cur.fetchone()
                    print("ROW IS", row)
                    if not row:
                        return False
                    qresult = row.get('c', 0)
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
            os.remove(trx_file_path)
            subprocess.call(["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"])