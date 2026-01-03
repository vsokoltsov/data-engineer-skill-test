import pytest
import pandas as pd
from uuid import uuid4

from pipelines.services.validator import TransactionValidator


@pytest.mark.unit
class TestTransactionValidator:
    """Unit tests for TransactionValidator class."""

    def test_validate_rows_valid_debit_with_negative_amount(self):
        """Test that debit transactions with negative amounts are valid."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Debit transaction"],
                "amount": [-100.50],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 1
        assert len(result["dlqs"]) == 0
        assert result["valid"].iloc[0]["amount"] == -100.50
        assert result["valid"].iloc[0]["side"] == "debit"

    def test_validate_rows_valid_credit_with_positive_amount(self):
        """Test that credit transactions with positive amounts are valid."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Credit transaction"],
                "amount": [200.75],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["transfer"],
                "side": ["credit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 1
        assert len(result["dlqs"]) == 0
        assert result["valid"].iloc[0]["amount"] == 200.75
        assert result["valid"].iloc[0]["side"] == "credit"

    def test_validate_rows_debit_with_positive_amount_is_invalid(self):
        """Test that debit transactions with positive amounts are invalid."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Invalid debit"],
                "amount": [100.0],  # Positive amount for debit is invalid
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 1
        assert "amount and side mismatch" in result["dlqs"].iloc[0]["validation_error"]

    def test_validate_rows_credit_with_negative_amount_is_invalid(self):
        """Test that credit transactions with negative amounts are invalid."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Invalid credit"],
                "amount": [-50.0],  # Negative amount for credit is invalid
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["transfer"],
                "side": ["credit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 1
        assert "amount and side mismatch" in result["dlqs"].iloc[0]["validation_error"]

    def test_validate_rows_zero_amount_is_invalid_for_debit(self):
        """Test that zero amount is invalid for debit transactions."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Zero amount debit"],
                "amount": [0.0],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 1
        assert "amount and side mismatch" in result["dlqs"].iloc[0]["validation_error"]

    def test_validate_rows_zero_amount_is_invalid_for_credit(self):
        """Test that zero amount is invalid for credit transactions."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Zero amount credit"],
                "amount": [0.0],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["transfer"],
                "side": ["credit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 1
        assert "amount and side mismatch" in result["dlqs"].iloc[0]["validation_error"]

    def test_validate_rows_null_amount_is_invalid(self):
        """Test that null amount is invalid."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Null amount"],
                "amount": [None],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 1
        assert "amount is null" in result["dlqs"].iloc[0]["validation_error"]

    def test_validate_rows_null_side_is_invalid(self):
        """Test that null side is invalid."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Null side"],
                "amount": [-100.0],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["card"],
                "side": [None],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 1
        assert "side is null" in result["dlqs"].iloc[0]["validation_error"]

    def test_validate_rows_invalid_side_value_is_invalid(self):
        """Test that invalid side values are rejected."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4())],
                "description": ["Invalid side 1", "Invalid side 2"],
                "amount": [-100.0, 200.0],
                "timestamp": ["2024-01-15T10:30:00"] * 2,
                "merchant": ["Merchant 1"] * 2,
                "operation_type": ["card"] * 2,
                "side": ["invalid", "deposit"],  # Invalid values
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 2
        assert all(
            "side is not valid" in err for err in result["dlqs"]["validation_error"]
        )

    def test_validate_rows_multiple_validation_errors_combined(self):
        """Test that transactions with multiple errors have combined error messages."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Multiple errors"],
                "amount": [None],  # Null amount
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["card"],
                "side": ["invalid"],  # Invalid side
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 1
        error = result["dlqs"].iloc[0]["validation_error"]
        assert "amount is null" in error
        assert "side is not valid" in error
        assert ";" in error  # Should have separator

    def test_validate_rows_mixed_valid_and_invalid_transactions(self):
        """Test separation of valid and invalid transactions."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4()) for _ in range(5)],
                "description": [f"Transaction {i}" for i in range(5)],
                "amount": [
                    -100.0,
                    200.0,
                    50.0,
                    -25.0,
                    0.0,
                ],  # Valid, valid, invalid (credit+pos), valid, invalid
                "timestamp": ["2024-01-15T10:30:00"] * 5,
                "merchant": ["Merchant"] * 5,
                "operation_type": ["card"] * 5,
                "side": ["debit", "credit", "debit", "credit", "debit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        # Index 0: debit with -100 (valid)
        # Index 1: credit with 200 (valid)
        # Index 2: debit with 50 (invalid - positive for debit)
        # Index 3: credit with -25 (invalid - negative for credit)
        # Index 4: debit with 0 (invalid - zero)
        assert len(result["valid"]) == 2
        assert len(result["dlqs"]) == 3
        assert result["valid"].iloc[0]["amount"] == -100.0
        assert result["valid"].iloc[1]["amount"] == 200.0

    def test_validate_rows_empty_dataframe(self):
        """Test that empty DataFrame returns empty valid and dlqs."""
        # Arrange
        df = pd.DataFrame(
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

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 0
        assert isinstance(result["valid"], pd.DataFrame)
        assert isinstance(result["dlqs"], pd.DataFrame)

    def test_validate_rows_all_valid_transactions(self):
        """Test that all valid transactions are returned in 'valid'."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4()), str(uuid4())],
                "description": ["Debit 1", "Credit 1", "Debit 2"],
                "amount": [-50.0, 100.0, -200.0],
                "timestamp": ["2024-01-15T10:30:00"] * 3,
                "merchant": ["Merchant 1", "Merchant 2", "Merchant 3"],
                "operation_type": ["card", "transfer", "card"],
                "side": ["debit", "credit", "debit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 3
        assert len(result["dlqs"]) == 0
        assert all(result["valid"]["side"].isin(["debit", "credit"]))

    def test_validate_rows_all_invalid_transactions(self):
        """Test that when all transactions are invalid, they all go to dlqs."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4())],
                "description": ["Invalid 1", "Invalid 2"],
                "amount": [100.0, -50.0],  # Debit with positive, credit with negative
                "timestamp": ["2024-01-15T10:30:00"] * 2,
                "merchant": ["Merchant 1", "Merchant 2"],
                "operation_type": ["card", "transfer"],
                "side": ["debit", "credit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 0
        assert len(result["dlqs"]) == 2
        assert all(
            "amount and side mismatch" in err
            for err in result["dlqs"]["validation_error"]
        )

    def test_validate_rows_preserves_dataframe_structure(self):
        """Test that the returned DataFrames preserve the original structure."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4())],
                "description": ["Valid", "Invalid"],
                "amount": [-100.0, 100.0],  # Valid debit, invalid debit (positive)
                "timestamp": ["2024-01-15T10:30:00"] * 2,
                "merchant": ["Merchant 1", "Merchant 2"],
                "operation_type": ["card", "card"],
                "side": ["debit", "debit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        # Valid DataFrame should have original columns (validation_error removed)
        assert list(result["valid"].columns) == list(df.columns)
        # DLQs DataFrame should have original columns plus validation_error
        assert "validation_error" in result["dlqs"].columns
        assert len(result["dlqs"].columns) == len(df.columns) + 1

    def test_validate_rows_validation_error_column_format(self):
        """Test that validation_error column has correct format."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4()), str(uuid4())],
                "description": [
                    "Invalid amount-side",
                    "Invalid side",
                    "Multiple errors",
                ],
                "amount": [
                    100.0,
                    -50.0,
                    None,
                ],  # Invalid for debit, valid for debit, null
                "timestamp": ["2024-01-15T10:30:00"] * 3,
                "merchant": ["M"] * 3,
                "operation_type": ["card"] * 3,
                "side": ["debit", "invalid", "invalid"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["dlqs"]) == 3

        errors = result["dlqs"]["validation_error"].tolist()

        # First row: amount-side mismatch
        assert "amount and side mismatch" in errors[0]

        # Second row: invalid side
        assert "side is not valid" in errors[1]

        # Third row: multiple errors (null amount + invalid side)
        assert "amount is null" in errors[2]
        assert "side is not valid" in errors[2]
        assert ";" in errors[2]

    def test_validate_rows_edge_case_very_small_negative_amount(self):
        """Test that very small negative amounts are valid for debit."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Small debit"],
                "amount": [-0.01],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 1
        assert len(result["dlqs"]) == 0

    def test_validate_rows_edge_case_very_small_positive_amount(self):
        """Test that very small positive amounts are valid for credit."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Small credit"],
                "amount": [0.01],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["transfer"],
                "side": ["credit"],
            }
        )

        # Act
        result = TransactionValidator.validate_rows(df)

        # Assert
        assert len(result["valid"]) == 1
        assert len(result["dlqs"]) == 0

    def test_validate_rows_does_not_modify_original_dataframe(self):
        """Test that the original DataFrame is not modified."""
        # Arrange
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Test"],
                "amount": [-100.0],
                "timestamp": ["2024-01-15T10:30:00"],
                "merchant": ["Merchant 1"],
                "operation_type": ["card"],
                "side": ["debit"],
            }
        )
        original_columns = list(df.columns)

        # Act
        TransactionValidator.validate_rows(df)

        # Assert
        assert list(df.columns) == original_columns
        assert "validation_error" not in df.columns
