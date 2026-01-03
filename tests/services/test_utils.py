import pytest
import pandas as pd
from uuid import uuid4

from pipelines.services.utils import merge_predictions
from pipelines.services.models import PredictionResponse


@pytest.mark.unit
class TestMergePredictions:
    """Unit tests for merge_predictions function."""

    def test_merge_predictions_all_matched(self):
        """Test merging predictions when all transactions have predictions."""
        # Arrange
        id1 = uuid4()
        id2 = uuid4()
        id3 = uuid4()

        chunk = pd.DataFrame(
            {
                "id": [str(id1), str(id2), str(id3)],
                "description": ["Food purchase", "Travel expense", "Rent payment"],
                "amount": [-50.0, -200.0, -1000.0],
                "timestamp": [
                    "2024-01-15T10:30:00",
                    "2024-01-16T14:20:00",
                    "2024-01-17T09:00:00",
                ],
                "merchant": ["Restaurant", "Airline", "Landlord"],
                "operation_type": ["card", "card", "transfer"],
                "side": ["debit", "debit", "debit"],
            }
        )

        predictions = [
            PredictionResponse(transaction_id=id1, category="Food"),
            PredictionResponse(transaction_id=id2, category="Travel"),
            PredictionResponse(transaction_id=id3, category="Rent"),
        ]

        # Act
        result = merge_predictions(chunk=chunk.copy(), predictions=predictions)

        # Assert
        assert len(result) == 3
        assert result["category"].tolist() == ["Food", "Travel", "Rent"]
        assert result["category"].dtype == object
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])
        assert result["id"].tolist() == [str(id1), str(id2), str(id3)]

    def test_merge_predictions_some_missing(self):
        """Test merging when some transactions don't have predictions."""
        # Arrange
        id1 = uuid4()
        id2 = uuid4()
        id3 = uuid4()

        chunk = pd.DataFrame(
            {
                "id": [str(id1), str(id2), str(id3)],
                "description": ["Food purchase", "Travel expense", "Unknown"],
                "amount": [-50.0, -200.0, -100.0],
                "timestamp": [
                    "2024-01-15T10:30:00",
                    "2024-01-16T14:20:00",
                    "2024-01-17T09:00:00",
                ],
                "merchant": ["Restaurant", "Airline", "Store"],
                "operation_type": ["card", "card", "card"],
                "side": ["debit", "debit", "debit"],
            }
        )

        # Only predictions for id1 and id2
        predictions = [
            PredictionResponse(transaction_id=id1, category="Food"),
            PredictionResponse(transaction_id=id2, category="Travel"),
        ]

        # Act
        result = merge_predictions(chunk=chunk.copy(), predictions=predictions)

        # Assert
        assert len(result) == 3
        assert result["category"].tolist() == ["Food", "Travel", None]
        assert result["category"].dtype == object
        # Check that None values are actually None, not NaN
        assert result["category"].iloc[2] is None

    def test_merge_predictions_all_missing(self):
        """Test merging when no transactions have predictions."""
        # Arrange
        id1 = uuid4()
        id2 = uuid4()

        chunk = pd.DataFrame(
            {
                "id": [str(id1), str(id2)],
                "description": ["Food purchase", "Travel expense"],
                "amount": [-50.0, -200.0],
                "timestamp": ["2024-01-15T10:30:00", "2024-01-16T14:20:00"],
                "merchant": ["Restaurant", "Airline"],
                "operation_type": ["card", "card"],
                "side": ["debit", "debit"],
            }
        )

        predictions = []

        # Act
        result = merge_predictions(chunk=chunk.copy(), predictions=predictions)

        # Assert
        assert len(result) == 2
        assert result["category"].tolist() == [None, None]
        assert result["category"].dtype == object
        assert all(cat is None for cat in result["category"])

    def test_merge_predictions_empty_chunk(self):
        """Test merging with empty chunk DataFrame."""
        # Arrange
        chunk = pd.DataFrame(
            columns=[
                "id",
                "description",
                "amount",
                "timestamp",
                "merchant",
                "operation_type",
                "side",
            ]
        )
        predictions = [
            PredictionResponse(transaction_id=uuid4(), category="Food"),
        ]

        # Act
        result = merge_predictions(chunk=chunk.copy(), predictions=predictions)

        # Assert
        assert len(result) == 0
        assert "category" in result.columns
        assert result["category"].dtype == object

    def test_merge_predictions_timestamp_conversion(self):
        """Test that timestamp is correctly converted to datetime."""
        # Arrange
        id1 = uuid4()

        chunk = pd.DataFrame(
            {
                "id": [str(id1)],
                "description": ["Test"],
                "amount": [-50.0],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Store"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )

        predictions = [
            PredictionResponse(transaction_id=id1, category="Food"),
        ]

        # Act
        result = merge_predictions(chunk=chunk.copy(), predictions=predictions)

        # Assert
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])
        assert result["timestamp"].iloc[0] == pd.Timestamp("2024-01-15T10:30:00")

    def test_merge_predictions_timestamp_conversion_raises_on_invalid(self):
        """Test that invalid timestamp raises an error."""
        # Arrange
        id1 = uuid4()

        chunk = pd.DataFrame(
            {
                "id": [str(id1)],
                "description": ["Test"],
                "amount": [-50.0],
                "timestamp": ["invalid-timestamp"],
                "merchant": ["Store"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )

        predictions = [
            PredictionResponse(transaction_id=id1, category="Food"),
        ]

        # Act & Assert
        with pytest.raises((ValueError, pd.errors.ParserError)):
            merge_predictions(chunk=chunk.copy(), predictions=predictions)

    def test_merge_predictions_preserves_original_columns(self):
        """Test that original columns are preserved in the result."""
        # Arrange
        id1 = uuid4()

        chunk = pd.DataFrame(
            {
                "id": [str(id1)],
                "description": ["Food purchase"],
                "amount": [-50.0],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Restaurant"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )

        predictions = [
            PredictionResponse(transaction_id=id1, category="Food"),
        ]

        # Act
        result = merge_predictions(chunk=chunk.copy(), predictions=predictions)

        # Assert
        expected_columns = {
            "id",
            "description",
            "amount",
            "timestamp",
            "merchant",
            "operation_type",
            "side",
            "category",
        }
        assert set(result.columns) == expected_columns

    def test_merge_predictions_does_not_modify_original_dataframe(self):
        """Test that the original DataFrame is not modified in place."""
        # Arrange
        id1 = uuid4()

        chunk = pd.DataFrame(
            {
                "id": [str(id1)],
                "description": ["Food purchase"],
                "amount": [-50.0],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Restaurant"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )

        predictions = [
            PredictionResponse(transaction_id=id1, category="Food"),
        ]

        # Act
        result = merge_predictions(chunk=chunk, predictions=predictions)
        # Assert
        # Original chunk should not have category column
        assert "category" not in chunk.columns
        # Result should have category column
        assert "category" in result.columns
        # Original chunk timestamp should still be string
        assert chunk["timestamp"].dtype == object
        # Result timestamp should be datetime
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

    def test_merge_predictions_handles_duplicate_ids_in_chunk(self):
        """Test merging when chunk has duplicate transaction IDs."""
        # Arrange
        id1 = uuid4()

        chunk = pd.DataFrame(
            {
                "id": [str(id1), str(id1)],
                "description": ["Food purchase 1", "Food purchase 2"],
                "amount": [-50.0, -75.0],
                "timestamp": ["2024-01-15T10:30:00", "2024-01-15T11:00:00"],
                "merchant": ["Restaurant", "Cafe"],
                "operation_type": ["card", "card"],
                "side": ["debit", "debit"],
            }
        )

        predictions = [
            PredictionResponse(transaction_id=id1, category="Food"),
        ]

        # Act
        result = merge_predictions(chunk=chunk.copy(), predictions=predictions)

        # Assert
        assert len(result) == 2
        # Both rows should get the same category since they map to the same prediction
        assert result["category"].tolist() == ["Food", "Food"]

    def test_merge_predictions_handles_extra_predictions(self):
        """Test merging when predictions contain IDs not in chunk."""
        # Arrange
        id1 = uuid4()
        id2 = uuid4()
        id3 = uuid4()  # Not in chunk

        chunk = pd.DataFrame(
            {
                "id": [str(id1), str(id2)],
                "description": ["Food purchase", "Travel expense"],
                "amount": [-50.0, -200.0],
                "timestamp": ["2024-01-15T10:30:00", "2024-01-16T14:20:00"],
                "merchant": ["Restaurant", "Airline"],
                "operation_type": ["card", "card"],
                "side": ["debit", "debit"],
            }
        )

        # Predictions include id3 which is not in chunk
        predictions = [
            PredictionResponse(transaction_id=id1, category="Food"),
            PredictionResponse(transaction_id=id2, category="Travel"),
            PredictionResponse(transaction_id=id3, category="Rent"),
        ]

        # Act
        result = merge_predictions(chunk=chunk.copy(), predictions=predictions)

        # Assert
        assert len(result) == 2
        assert result["category"].tolist() == ["Food", "Travel"]
        # Extra prediction should be ignored (not cause issues)
