import pytest
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4
from sqlalchemy.exc import DBAPIError
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition

from pipelines.services.batch_ingest import (
    AbstractTransactionIngestService,
    CSVTransactionIngestService,
    KafkaTransactionIngestService,
)
from pipelines.services.quality import BatchQuality
from pipelines.services.validator import TransactionValidator
from pipelines.services.errors import MLAPIServiceError
from pipelines.csv.reader import CSVReader


class ConcreteIngestService(AbstractTransactionIngestService):
    """Concrete implementation for testing abstract class."""

    def __init__(
        self, session_factory, ml_api, quality_service, dlq, read_batches_func
    ):
        super().__init__(session_factory, ml_api, quality_service, dlq)
        self._read_batches_func = read_batches_func

    @property
    def source(self) -> str:
        return "test"

    async def read_batches(self, chunk_size: int = 1000):
        async for chunk in self._read_batches_func(chunk_size):
            yield chunk


@pytest.mark.asyncio
@pytest.mark.unit
class TestHelperMethods:
    """Unit tests for helper methods."""

    def test_is_db_data_error_with_dataerror_class(self):
        """Test _is_db_data_error returns True for DataError class."""
        # Arrange
        orig_error = Mock()
        orig_error.__class__.__name__ = "DataError"
        db_error = DBAPIError("SELECT", None, orig_error)
        db_error.orig = orig_error

        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )

        # Act
        result = service._is_db_data_error(db_error)

        # Assert
        assert result is True

    def test_is_db_data_error_with_data_marker_in_message(self):
        """Test _is_db_data_error returns True for messages with data markers."""
        # Arrange
        orig_error = Exception("invalid input syntax")
        db_error = DBAPIError("SELECT", None, orig_error)
        db_error.orig = orig_error

        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )

        # Act
        result = service._is_db_data_error(db_error)

        # Assert
        assert result is True

    def test_is_db_data_error_with_no_orig(self):
        """Test _is_db_data_error handles missing orig attribute."""
        # Arrange
        db_error = DBAPIError("SELECT", None, Exception("dummy"))
        db_error.orig = None  # type: ignore[assignment]

        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )

        # Act
        result = service._is_db_data_error(db_error)

        # Assert
        assert result is False

    def test_is_db_data_error_with_non_data_error(self):
        """Test _is_db_data_error returns False for non-data errors."""
        # Arrange
        orig_error = Exception("Connection timeout")
        db_error = DBAPIError("SELECT", None, orig_error)
        db_error.orig = orig_error

        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )

        # Act
        result = service._is_db_data_error(db_error)

        # Assert
        assert result is False

    def test_should_stop_with_max_consecutive_failures(self):
        """Test _should_stop returns True when max consecutive failures reached."""
        # Arrange
        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )
        service.max_consecutive_failures = 3

        # Act
        result = service._should_stop(Exception("Error"), "stage", 3)

        # Assert
        assert result is True

    def test_should_stop_with_dbapi_data_error(self):
        """Test _should_stop returns False for data errors."""
        # Arrange
        orig_error = Exception("invalid input")
        db_error = DBAPIError("SELECT", None, orig_error)
        db_error.orig = orig_error

        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )

        # Act
        result = service._should_stop(db_error, "stage", 1)

        # Assert
        assert result is False

    def test_should_stop_with_dbapi_non_data_error(self):
        """Test _should_stop returns True for non-data DBAPI errors."""
        # Arrange
        orig_error = Exception("Connection error")
        db_error = DBAPIError("SELECT", None, orig_error)
        db_error.orig = orig_error

        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )

        # Act
        result = service._should_stop(db_error, "stage", 1)

        # Assert
        assert result is True

    def test_should_stop_with_ml_api_error_and_stop_on_error_true(self):
        """Test _should_stop respects stop_on_error for MLAPI errors."""
        # Arrange
        ml_error = MLAPIServiceError(Exception("API error"))
        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )
        service.stop_on_error = True

        # Act
        result = service._should_stop(ml_error, "stage", 1)

        # Assert
        assert result is True

    def test_should_stop_with_ml_api_error_and_stop_on_error_false(self):
        """Test _should_stop respects stop_on_error=False for MLAPI errors."""
        # Arrange
        ml_error = MLAPIServiceError(Exception("API error"))
        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )
        service.stop_on_error = False

        # Act
        result = service._should_stop(ml_error, "stage", 1)

        # Assert
        assert result is False

    def test_should_stop_with_generic_error_and_stop_on_error_true(self):
        """Test _should_stop respects stop_on_error for generic errors."""
        # Arrange
        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), Mock()
        )
        service.stop_on_error = True

        # Act
        result = service._should_stop(RuntimeError("Error"), "stage", 1)

        # Assert
        assert result is True

    async def test_send_chunk_to_dlq_success(self):
        """Test _send_chunk_to_dlq successfully publishes all records."""
        # Arrange
        chunk = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4())],
                "description": ["Test 1", "Test 2"],
                "amount": [-100.0, 200.0],
            }
        )
        error = Exception("Test error")
        dlq = Mock()
        dlq.publish = AsyncMock()

        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), dlq, Mock()
        )

        # Act
        await service._send_chunk_to_dlq(chunk, error, "test_stage", "batch-123")

        # Assert
        assert dlq.publish.await_count == 2
        dlq.publish.assert_any_await(
            original=chunk.iloc[0].to_dict(),
            err=error,
            stage="test_stage",
            source="test",
            topic="transactions",
        )

    async def test_send_chunk_to_dlq_handles_publish_failure(self):
        """Test _send_chunk_to_dlq handles DLQ publish failures gracefully."""
        # Arrange
        chunk = pd.DataFrame({"id": [str(uuid4())], "description": ["Test"]})
        error = Exception("Test error")
        dlq = Mock()
        dlq.publish = AsyncMock(side_effect=Exception("DLQ error"))

        mock_logger = Mock()
        mock_logger.exception = Mock()
        mock_logger.bind.return_value = mock_logger

        service = ConcreteIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), dlq, Mock()
        )
        service.logging = mock_logger

        # Act
        await service._send_chunk_to_dlq(chunk, error, "test_stage", "batch-123")

        # Assert
        # Should not raise, but log the error
        mock_logger.exception.assert_called_once()
        call_args = mock_logger.exception.call_args
        assert call_args[0][0] == "dlq_publish_failed"


@pytest.mark.asyncio
@pytest.mark.unit
class TestErrorHandlingWithRetries:
    """Unit tests for error handling with consecutive failures and backoff."""

    async def test_run_continues_after_data_error(self, monkeypatch):
        """Test that processing continues after a data error."""
        # Arrange
        chunk1 = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Valid"],
                "amount": [-100.0],
                "side": ["debit"],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["M1"],
                "operation_type": ["card"],
            }
        )
        chunk2 = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Valid 2"],
                "amount": [200.0],
                "side": ["credit"],
                "timestamp": ["2024-01-15T11:00:00"],
                "merchant": ["M2"],
                "operation_type": ["card"],
            }
        )

        async def read_batches(chunk_size):
            yield chunk1
            yield chunk2

        ml_api = Mock()
        ml_api.predict.return_value = []

        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        # First batch fails with data error, second succeeds
        orig_error = Exception("invalid input syntax")
        db_error = DBAPIError("INSERT", None, orig_error)
        db_error.orig = orig_error

        validator_result = {
            "valid": chunk1,
            "dlqs": pd.DataFrame(columns=chunk1.columns),
        }
        validate_mock = Mock(
            side_effect=[
                validator_result,
                {"valid": chunk2, "dlqs": pd.DataFrame(columns=chunk2.columns)},
            ]
        )
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        merged_df = Mock()
        merged_df.to_dict.return_value = [{"id": "1"}]
        merge_mock = Mock(return_value=merged_df)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.merge_predictions", merge_mock
        )

        repo = Mock()
        repo.upsert_many = AsyncMock(side_effect=[db_error, 1])
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.TransactionRepository",
            Mock(return_value=repo),
        )

        session = Mock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        mock_metrics = {
            "INGEST_INFLIGHT": Mock(),
            "INGEST_BATCHES_TOTAL": Mock(),
            "INGEST_ROWS_TOTAL": Mock(),
            "INGEST_LAST_BATCH_SIZE": Mock(),
            "INGEST_ROWS_WRITTEN_TOTAL": Mock(),
            "INGEST_LAST_SUCCESS_TS": Mock(),
            "INGEST_STAGE_DURATION": Mock(),
        }
        for name, mock_obj in mock_metrics.items():
            mock_obj.labels.return_value = mock_obj
            monkeypatch.setattr(f"pipelines.services.batch_ingest.{name}", mock_obj)

        mock_logger = Mock()
        mock_logger.error = Mock()
        mock_logger.info = Mock()
        mock_logger.exception = Mock()
        mock_logger.bind.return_value = mock_logger
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.structlog.get_logger",
            Mock(return_value=mock_logger),
        )

        service = ConcreteIngestService(
            session_factory, ml_api, quality_service, dlq, read_batches
        )

        # Act
        result = await service.run()

        # Assert
        assert result == 1  # Only second batch succeeded
        assert session.rollback.await_count == 1  # Rolled back after first error
        assert repo.upsert_many.await_count == 2  # Both batches attempted

    async def test_run_stops_after_max_consecutive_failures(self, monkeypatch):
        """Test that processing stops after max consecutive failures."""
        # Arrange
        chunk = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Valid"],
                "amount": [-100.0],
                "side": ["debit"],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["M1"],
                "operation_type": ["card"],
            }
        )

        async def read_batches(chunk_size):
            yield chunk
            yield chunk
            yield chunk

        ml_api = Mock()
        ml_api.predict.side_effect = RuntimeError("API error")

        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        validator_result = {"valid": chunk, "dlqs": pd.DataFrame(columns=chunk.columns)}
        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        session = Mock()
        session.rollback = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        mock_metrics = {
            "INGEST_INFLIGHT": Mock(),
            "INGEST_BATCHES_TOTAL": Mock(),
            "INGEST_ROWS_TOTAL": Mock(),
            "INGEST_LAST_BATCH_SIZE": Mock(),
            "INGEST_ROWS_WRITTEN_TOTAL": Mock(),
            "INGEST_LAST_SUCCESS_TS": Mock(),
            "INGEST_STAGE_DURATION": Mock(),
        }
        for name, mock_obj in mock_metrics.items():
            mock_obj.labels.return_value = mock_obj
            monkeypatch.setattr(f"pipelines.services.batch_ingest.{name}", mock_obj)

        mock_logger = Mock()
        mock_logger.error = Mock()
        mock_logger.info = Mock()
        mock_logger.exception = Mock()
        mock_logger.bind.return_value = mock_logger
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.structlog.get_logger",
            Mock(return_value=mock_logger),
        )

        service = ConcreteIngestService(
            session_factory, ml_api, quality_service, dlq, read_batches
        )
        service.max_consecutive_failures = 2
        service.stop_on_error = True

        # Act / Assert
        with pytest.raises(RuntimeError):
            await service.run()

        # Should have failed after 1 failure because stop_on_error=True
        # (not waiting for max_consecutive_failures when stop_on_error is True)
        assert ml_api.predict.call_count == 1

    async def test_run_applies_backoff_on_failure(self, monkeypatch):
        """Test that backoff is applied when backoff_on_failure_s is set."""
        # Arrange
        chunk = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Valid"],
                "amount": [-100.0],
                "side": ["debit"],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["M1"],
                "operation_type": ["card"],
            }
        )

        async def read_batches(chunk_size):
            yield chunk
            yield chunk

        ml_api = Mock()
        ml_api.predict.side_effect = [RuntimeError("Error"), []]

        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        validator_result = {"valid": chunk, "dlqs": pd.DataFrame(columns=chunk.columns)}
        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        merged_df = Mock()
        merged_df.to_dict.return_value = [{"id": "1"}]
        merge_mock = Mock(return_value=merged_df)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.merge_predictions", merge_mock
        )

        repo = Mock()
        repo.upsert_many = AsyncMock(return_value=1)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.TransactionRepository",
            Mock(return_value=repo),
        )

        session = Mock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        mock_metrics = {
            "INGEST_INFLIGHT": Mock(),
            "INGEST_BATCHES_TOTAL": Mock(),
            "INGEST_ROWS_TOTAL": Mock(),
            "INGEST_LAST_BATCH_SIZE": Mock(),
            "INGEST_ROWS_WRITTEN_TOTAL": Mock(),
            "INGEST_LAST_SUCCESS_TS": Mock(),
            "INGEST_STAGE_DURATION": Mock(),
        }
        for name, mock_obj in mock_metrics.items():
            mock_obj.labels.return_value = mock_obj
            monkeypatch.setattr(f"pipelines.services.batch_ingest.{name}", mock_obj)

        mock_logger = Mock()
        mock_logger.error = Mock()
        mock_logger.info = Mock()
        mock_logger.exception = Mock()
        mock_logger.bind.return_value = mock_logger
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.structlog.get_logger",
            Mock(return_value=mock_logger),
        )

        service = ConcreteIngestService(
            session_factory, ml_api, quality_service, dlq, read_batches
        )
        service.backoff_on_failure_s = 0.1
        service.max_consecutive_failures = 10
        service.stop_on_error = False

        # Act
        with patch("asyncio.sleep", new_callable=AsyncMock) as sleep_mock:
            await service.run()
            # Verify backoff was called after first failure
            sleep_mock.assert_awaited_once_with(0.1)


@pytest.mark.asyncio
@pytest.mark.unit
class TestCSVTransactionIngestService:
    """Unit tests for CSVTransactionIngestService."""

    def test_source_property(self):
        """Test that source property returns 'csv'."""
        # Arrange
        csv_reader = Mock(spec=CSVReader)
        service = CSVTransactionIngestService(
            Mock(), Mock(), BatchQuality(threshold=0.1), Mock(), csv_reader
        )

        # Act
        result = service.source

        # Assert
        assert result == "csv"

    async def test_read_batches_delegates_to_csv_reader(self):
        """Test that read_batches delegates to CSVReader."""
        # Arrange
        chunk1 = pd.DataFrame({"id": ["1"], "amount": [100.0]})
        chunk2 = pd.DataFrame({"id": ["2"], "amount": [200.0]})

        csv_reader = Mock(spec=CSVReader)
        csv_reader.read_batches = Mock(return_value=iter([chunk1, chunk2]))

        service = CSVTransactionIngestService(
            session_factory=Mock(),
            ml_api=Mock(),
            quality_service=BatchQuality(threshold=0.1),
            dlq=Mock(),
            csv_reader=csv_reader,
        )

        # Act
        batches = []
        async for batch in service.read_batches(chunk_size=1000):
            batches.append(batch)

        # Assert
        assert len(batches) == 2
        assert batches[0].equals(chunk1)
        assert batches[1].equals(chunk2)
        csv_reader.read_batches.assert_called_once_with(chunk_size=1000)


@pytest.mark.asyncio
@pytest.mark.unit
class TestKafkaTransactionIngestService:
    """Unit tests for KafkaTransactionIngestService."""

    def test_source_property(self):
        """Test that source property returns 'kafka'."""
        # Arrange
        consumer = Mock(spec=AIOKafkaConsumer)
        service = KafkaTransactionIngestService(
            session_factory=Mock(),
            ml_api=Mock(),
            quality_service=BatchQuality(threshold=0.1),
            dlq=Mock(),
            consumer=consumer,
        )

        # Act
        result = service.source

        # Assert
        assert result == "kafka"

    async def test_read_batches_reads_from_kafka_consumer(self):
        """Test that read_batches reads from Kafka consumer."""
        # Arrange
        consumer = Mock(spec=AIOKafkaConsumer)

        # Create mock consumer records
        record1 = Mock(spec=ConsumerRecord)
        record1.value = {"id": "1", "amount": 100.0}
        record2 = Mock(spec=ConsumerRecord)
        record2.value = {"id": "2", "amount": 200.0}

        topic_partition = Mock(spec=TopicPartition)
        batches = {topic_partition: [record1, record2]}

        consumer.getmany = AsyncMock(return_value=batches)

        service = KafkaTransactionIngestService(
            session_factory=Mock(),
            ml_api=Mock(),
            quality_service=BatchQuality(threshold=0.1),
            dlq=Mock(),
            consumer=consumer,
        )

        # Act
        result_batches = []
        async for batch in service.read_batches(chunk_size=1000):
            result_batches.append(batch)

        # Assert
        assert len(result_batches) == 1
        assert len(result_batches[0]) == 2
        consumer.getmany.assert_awaited_once_with(timeout_ms=1000, max_records=1000)

    async def test_read_batches_handles_empty_batches(self):
        """Test that read_batches handles empty Kafka batches."""
        # Arrange
        consumer = Mock(spec=AIOKafkaConsumer)
        consumer.getmany = AsyncMock(return_value={})

        service = KafkaTransactionIngestService(
            session_factory=Mock(),
            ml_api=Mock(),
            quality_service=BatchQuality(threshold=0.1),
            dlq=Mock(),
            consumer=consumer,
        )

        # Act
        result_batches = []
        async for batch in service.read_batches(chunk_size=1000):
            result_batches.append(batch)

        # Assert
        assert len(result_batches) == 0

    async def test_read_batches_handles_multiple_partitions(self):
        """Test that read_batches handles multiple topic partitions."""
        # Arrange
        consumer = Mock(spec=AIOKafkaConsumer)

        record1 = Mock(spec=ConsumerRecord)
        record1.value = {"id": "1", "amount": 100.0}
        record2 = Mock(spec=ConsumerRecord)
        record2.value = {"id": "2", "amount": 200.0}

        tp1 = Mock(spec=TopicPartition)
        tp2 = Mock(spec=TopicPartition)
        batches = {
            tp1: [record1],
            tp2: [record2],
        }

        consumer.getmany = AsyncMock(return_value=batches)

        service = KafkaTransactionIngestService(
            session_factory=Mock(),
            ml_api=Mock(),
            quality_service=BatchQuality(threshold=0.1),
            dlq=Mock(),
            consumer=consumer,
        )

        # Act
        result_batches = []
        async for batch in service.read_batches(chunk_size=1000):
            result_batches.append(batch)

        # Assert
        assert len(result_batches) == 2
        assert len(result_batches[0]) == 1
        assert len(result_batches[1]) == 1
