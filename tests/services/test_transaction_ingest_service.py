import pytest
import pandas as pd
from unittest.mock import Mock, AsyncMock
from uuid import uuid4
from sqlalchemy.exc import DBAPIError

from pipelines.services.batch_ingest import AbstractTransactionIngestService
from pipelines.services.quality import BatchQuality
from pipelines.services.validator import TransactionValidator
from pipelines.services.errors import MLAPIServiceError


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
class TestAbstractTransactionIngestService:
    """Unit tests for AbstractTransactionIngestService class."""

    async def test_run_happy_path_single_batch(self, monkeypatch):
        """Test successful processing of a single batch."""
        # Arrange
        chunk = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4())],
                "description": ["Valid 1", "Valid 2"],
                "amount": [-100.0, 200.0],
                "side": ["debit", "credit"],
                "timestamp": ["2024-01-15T10:30:00"] * 2,
                "merchant": ["M1", "M2"],
                "operation_type": ["card", "transfer"],
            }
        )

        async def read_batches(chunk_size):
            yield chunk

        ml_api = Mock()
        predictions = [
            Mock(transaction_id=chunk.iloc[0]["id"], category="Food"),
            Mock(transaction_id=chunk.iloc[1]["id"], category="Travel"),
        ]
        ml_api.predict.return_value = predictions

        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        # Mock validator - all valid
        validator_result = {"valid": chunk, "dlqs": pd.DataFrame(columns=chunk.columns)}
        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        # Mock merge_predictions
        merged_df = Mock()
        merged_df.to_dict.return_value = [
            {"id": chunk.iloc[0]["id"], "category": "Food"},
            {"id": chunk.iloc[1]["id"], "category": "Travel"},
        ]
        merge_mock = Mock(return_value=merged_df)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.merge_predictions", merge_mock
        )

        # Mock repository
        repo = Mock()
        repo.upsert_many = AsyncMock(return_value=2)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.TransactionRepository",
            Mock(return_value=repo),
        )

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        service = ConcreteIngestService(
            session_factory, ml_api, quality_service, dlq, read_batches
        )

        # Act
        result = await service.run()

        # Assert
        assert result == 2
        validate_mock.assert_called_once()
        ml_api.predict.assert_called_once()
        repo.upsert_many.assert_awaited_once()
        session.commit.assert_awaited_once()
        dlq.publish.assert_not_called()  # No DLQs

    async def test_run_publishes_dlqs_when_invalid_rows_present(self, monkeypatch):
        """Test that invalid rows are published to DLQ."""
        # Arrange
        chunk = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4()), str(uuid4())],
                "description": ["Valid 1", "Valid 2", "Invalid"],
                "amount": [-100.0, 200.0, 0.0],  # Last one invalid
                "side": ["debit", "credit", "debit"],
                "timestamp": ["2024-01-15T10:30:00"] * 3,
                "merchant": ["M1", "M2", "M3"],
                "operation_type": ["card", "card", "card"],
            }
        )

        async def read_batches(chunk_size):
            yield chunk

        ml_api = Mock()
        ml_api.predict.return_value = []

        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        # Mock validator with DLQs
        valid_df = chunk.iloc[:2].copy()
        dlqs_df = chunk.iloc[2:].copy()
        dlqs_df["validation_error"] = "amount and side mismatch"
        validator_result = {"valid": valid_df, "dlqs": dlqs_df}

        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        # Mock merge_predictions
        merged_df = Mock()
        merged_df.to_dict.return_value = [{"id": "1"}, {"id": "2"}]
        merge_mock = Mock(return_value=merged_df)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.merge_predictions", merge_mock
        )

        # Mock repository
        repo = Mock()
        repo.upsert_many = AsyncMock(return_value=2)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.TransactionRepository",
            Mock(return_value=repo),
        )

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        # Mock logger
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
        assert result == 2
        # Verify DLQ was called for invalid row
        assert dlq.publish.await_count == 1
        dlq.publish.assert_awaited_with(
            original=dlqs_df.iloc[0].to_dict(),
            err="amount and side mismatch",
            stage="validate",
            source="test",
            topic="transactions",
        )
        # Verify error was logged
        error_calls = [
            c
            for c in mock_logger.error.call_args_list
            if len(c[0]) > 0 and c[0][0] == "dlqs_records"
        ]
        assert len(error_calls) == 1

    async def test_run_skips_batch_when_all_rows_invalid(self, monkeypatch):
        """Test that batch is skipped when all rows are invalid."""
        # Arrange
        chunk = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4())],
                "description": ["Invalid 1", "Invalid 2"],
                "amount": [0.0, 100.0],  # Both invalid
                "side": ["debit", "debit"],
                "timestamp": ["2024-01-15T10:30:00"] * 2,
                "merchant": ["M1", "M2"],
                "operation_type": ["card", "card"],
            }
        )

        async def read_batches(chunk_size):
            yield chunk

        ml_api = Mock()
        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        # Mock validator - all invalid
        dlqs_df = chunk.copy()
        dlqs_df["validation_error"] = "amount and side mismatch"
        validator_result = {
            "valid": pd.DataFrame(columns=chunk.columns),
            "dlqs": dlqs_df,
        }

        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        # Mock logger
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
        assert result == 0
        # DLQ should be called for both invalid rows
        assert dlq.publish.await_count == 2
        # ML API should not be called since no valid rows
        ml_api.predict.assert_not_called()

    async def test_run_logs_quality_issues_when_threshold_exceeded(self, monkeypatch):
        """Test that quality issues are logged when threshold is exceeded."""
        # Arrange
        chunk = pd.DataFrame(
            {
                "id": [str(uuid4()) for _ in range(10)],
                "description": [""] * 5 + ["Valid"] * 5,  # 5 missing descriptions
                "amount": [-100.0] * 10,
                "side": ["debit"] * 10,
                "timestamp": ["2024-01-15T10:30:00"] * 10,
                "merchant": ["M"] * 10,
                "operation_type": ["card"] * 10,
            }
        )

        async def read_batches(chunk_size):
            yield chunk

        ml_api = Mock()
        ml_api.predict.return_value = []

        quality_service = BatchQuality(threshold=0.3)  # 0.5 rate will exceed this
        dlq = Mock()
        dlq.publish = AsyncMock()

        # Mock validator - all valid
        validator_result = {"valid": chunk, "dlqs": pd.DataFrame(columns=chunk.columns)}
        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        # Mock merge_predictions
        merged_df = Mock()
        merged_df.to_dict.return_value = [{"id": str(i)} for i in range(10)]
        merge_mock = Mock(return_value=merged_df)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.merge_predictions", merge_mock
        )

        # Mock repository
        repo = Mock()
        repo.upsert_many = AsyncMock(return_value=10)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.TransactionRepository",
            Mock(return_value=repo),
        )

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        # Mock logger
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
        await service.run()

        # Assert
        error_calls = [
            c
            for c in mock_logger.error.call_args_list
            if len(c[0]) > 0 and c[0][0] == "data_quality_low"
        ]
        assert len(error_calls) == 1
        assert error_calls[0][1]["rate"] > quality_service.threshold

    async def test_run_handles_multiple_batches(self, monkeypatch):
        """Test processing of multiple batches."""
        # Arrange
        chunk1 = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Valid 1"],
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

        # Mock validator
        validator_result1 = {
            "valid": chunk1,
            "dlqs": pd.DataFrame(columns=chunk1.columns),
        }
        validator_result2 = {
            "valid": chunk2,
            "dlqs": pd.DataFrame(columns=chunk2.columns),
        }
        validate_mock = Mock(side_effect=[validator_result1, validator_result2])
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        # Mock merge_predictions
        merged_df = Mock()
        merged_df.to_dict.return_value = [{"id": "1"}]
        merge_mock = Mock(return_value=merged_df)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.merge_predictions", merge_mock
        )

        # Mock repository
        repo = Mock()
        repo.upsert_many = AsyncMock(side_effect=[1, 1])
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.TransactionRepository",
            Mock(return_value=repo),
        )

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        service = ConcreteIngestService(
            session_factory, ml_api, quality_service, dlq, read_batches
        )

        # Act
        result = await service.run()

        # Assert
        assert result == 2
        assert validate_mock.call_count == 2
        assert ml_api.predict.call_count == 2
        assert repo.upsert_many.await_count == 2
        assert session.commit.await_count == 2

    async def test_run_handles_empty_batches(self, monkeypatch):
        """Test that empty batches are handled gracefully."""

        # Arrange
        async def read_batches(chunk_size):
            return
            yield  # Empty generator

        ml_api = Mock()
        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        service = ConcreteIngestService(
            session_factory, ml_api, quality_service, dlq, read_batches
        )

        # Act
        result = await service.run()

        # Assert
        assert result == 0
        ml_api.predict.assert_not_called()

    async def test_run_raises_dbapi_error(self, monkeypatch):
        """Test that DBAPIError is properly handled and re-raised."""
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

        ml_api = Mock()
        ml_api.predict.return_value = []

        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        # Mock validator
        validator_result = {"valid": chunk, "dlqs": pd.DataFrame(columns=chunk.columns)}
        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        # Mock merge_predictions
        merged_df = Mock()
        merged_df.to_dict.return_value = [{"id": "1"}]
        merge_mock = Mock(return_value=merged_df)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.merge_predictions", merge_mock
        )

        # Mock repository to raise DBAPIError
        db_error = DBAPIError("SELECT", None, Exception("Database error"))
        db_error.statement = "SELECT * FROM transactions"
        db_error.params = {}
        db_error.orig = Exception("Database error")

        repo = Mock()
        repo.upsert_many = AsyncMock(side_effect=db_error)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.TransactionRepository",
            Mock(return_value=repo),
        )

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        # Mock logger
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

        # Act / Assert
        with pytest.raises(DBAPIError):
            await service.run()

        # Verify error was logged
        exception_calls = [
            c
            for c in mock_logger.exception.call_args_list
            if len(c[0]) > 0 and c[0][0] == "db_upsert_error"
        ]
        assert len(exception_calls) == 1

    async def test_run_raises_ml_api_service_error(self, monkeypatch):
        """Test that MLAPIServiceError is properly handled and re-raised."""
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

        ml_api = Mock()
        ml_error = MLAPIServiceError(Exception("Connection error"))
        ml_error.orig = Exception("Connection error")
        ml_api.predict.side_effect = ml_error

        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        # Mock validator
        validator_result = {"valid": chunk, "dlqs": pd.DataFrame(columns=chunk.columns)}
        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        # Mock logger
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

        # Act / Assert
        with pytest.raises(MLAPIServiceError):
            await service.run()

        # Verify error was logged
        exception_calls = [
            c
            for c in mock_logger.exception.call_args_list
            if len(c[0]) > 0 and c[0][0] == "ml_api_service_error"
        ]
        assert len(exception_calls) == 1

    async def test_run_raises_generic_exception(self, monkeypatch):
        """Test that generic exceptions are properly handled and re-raised."""
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

        ml_api = Mock()
        ml_api.predict.side_effect = RuntimeError("Unexpected error")

        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        # Mock validator
        validator_result = {"valid": chunk, "dlqs": pd.DataFrame(columns=chunk.columns)}
        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        # Mock logger
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

        # Act / Assert
        with pytest.raises(RuntimeError, match="Unexpected error"):
            await service.run()

        # Verify error was logged
        exception_calls = [
            c
            for c in mock_logger.exception.call_args_list
            if len(c[0]) > 0 and c[0][0] == "batch_failed"
        ]
        assert len(exception_calls) == 1

    async def test_run_updates_metrics_correctly(self, monkeypatch):
        """Test that metrics are updated correctly during processing."""
        # Arrange
        chunk = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4())],
                "description": ["Valid 1", "Valid 2"],
                "amount": [-100.0, 200.0],
                "side": ["debit", "credit"],
                "timestamp": ["2024-01-15T10:30:00"] * 2,
                "merchant": ["M1", "M2"],
                "operation_type": ["card", "card"],
            }
        )

        async def read_batches(chunk_size):
            yield chunk

        ml_api = Mock()
        ml_api.predict.return_value = []

        quality_service = BatchQuality(threshold=0.1)
        dlq = Mock()
        dlq.publish = AsyncMock()

        # Mock validator
        validator_result = {"valid": chunk, "dlqs": pd.DataFrame(columns=chunk.columns)}
        validate_mock = Mock(return_value=validator_result)
        monkeypatch.setattr(TransactionValidator, "validate_rows", validate_mock)

        # Mock merge_predictions
        merged_df = Mock()
        merged_df.to_dict.return_value = [{"id": "1"}, {"id": "2"}]
        merge_mock = Mock(return_value=merged_df)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.merge_predictions", merge_mock
        )

        # Mock repository
        repo = Mock()
        repo.upsert_many = AsyncMock(return_value=2)
        monkeypatch.setattr(
            "pipelines.services.batch_ingest.TransactionRepository",
            Mock(return_value=repo),
        )

        # Mock session
        session = Mock()
        session.commit = AsyncMock()
        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)
        session_factory = Mock(return_value=cm)

        # Mock metrics
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

        service = ConcreteIngestService(
            session_factory, ml_api, quality_service, dlq, read_batches
        )

        # Act
        await service.run()

        # Assert
        # Verify metrics were called
        mock_metrics["INGEST_INFLIGHT"].labels.assert_called_with(source="test")
        mock_metrics["INGEST_BATCHES_TOTAL"].labels.assert_called_with(source="test")
        mock_metrics["INGEST_ROWS_TOTAL"].labels.assert_called_with(source="test")
        mock_metrics["INGEST_LAST_BATCH_SIZE"].labels.assert_called_with(source="test")
        mock_metrics["INGEST_ROWS_WRITTEN_TOTAL"].labels.assert_called_with(
            source="test"
        )
        mock_metrics["INGEST_LAST_SUCCESS_TS"].labels.assert_called_with(source="test")
        mock_metrics["INGEST_STAGE_DURATION"].labels.assert_called_with(
            source="test", stage="batch_total"
        )

        # Verify inflight was set to 1 and then 0
        assert mock_metrics["INGEST_INFLIGHT"].set.call_count == 2
        assert mock_metrics["INGEST_BATCHES_TOTAL"].inc.call_count == 1
        assert mock_metrics["INGEST_ROWS_TOTAL"].inc.call_count == 1
        assert mock_metrics["INGEST_ROWS_WRITTEN_TOTAL"].inc.call_count == 1
