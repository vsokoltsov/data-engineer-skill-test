from prometheus_client import Counter, Histogram, Gauge

INGEST_BATCHES_TOTAL = Counter(
    "ingest_batches_total",
    "Number of processed batches",
    ["source"],  # kafka|csv
)

INGEST_ROWS_TOTAL = Counter(
    "ingest_rows_total",
    "Number of processed rows (input)",
    ["source"],
)

INGEST_ROWS_WRITTEN_TOTAL = Counter(
    "ingest_rows_written_total",
    "Number of rows written to DB (affected)",
    ["source"],
)

INGEST_ERRORS_TOTAL = Counter(
    "ingest_errors_total",
    "Number of errors by stage",
    ["source", "stage"],  # read|predict|merge|db|commit|other
)

INGEST_STAGE_DURATION = Histogram(
    "ingest_stage_duration_seconds",
    "Duration of ingest stages in seconds",
    ["source", "stage"],  # read|predict|merge|db_upsert|commit|batch_total
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)

INGEST_INFLIGHT = Gauge(
    "ingest_inflight",
    "Whether ingest is currently processing a batch (0/1)",
    ["source"],
)

INGEST_LAST_SUCCESS_TS = Gauge(
    "ingest_last_success_timestamp_seconds",
    "Unix timestamp of last successful batch",
    ["source"],
)

INGEST_LAST_BATCH_SIZE = Gauge(
    "ingest_last_batch_size",
    "Size of last batch",
    ["source"],
)

# Kafka
KAFKA_RECORDS_POLLED_TOTAL = Counter(
    "kafka_records_polled_total",
    "Number of Kafka records polled",
    ["topic", "group_id"],
)

KAFKA_POLL_EMPTY_TOTAL = Counter(
    "kafka_poll_empty_total",
    "Number of empty polls (no records)",
    ["topic", "group_id"],
)

# CSV
CSV_CHUNKS_TOTAL = Counter(
    "csv_chunks_total",
    "Number of CSV chunks read",
    [],
)

CSV_ROWS_READ_TOTAL = Counter(
    "csv_rows_read_total",
    "Number of CSV rows read",
    [],
)
