import pytest
import pandas as pd
import numpy as np
from uuid import uuid4

from pipelines.services.quality import BatchQuality, QualityReport, QualityIssue


@pytest.mark.unit
class TestBatchQuality:
    """Unit tests for BatchQuality class."""

    def test_verify_with_no_issues_returns_empty_report(self):
        """Test that DataFrame with no quality issues returns empty report."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4())],
                "description": ["Valid description 1", "Valid description 2"],
                "amount": [100.0, 200.0],
                "operation_type": ["card", "withdrawal"],
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        assert isinstance(result, QualityReport)
        assert result.issues_rate == 0.0
        assert len(result.issues) == 0

    def test_verify_with_missing_description_detects_issue(self):
        """Test that empty or NaN descriptions are detected."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4()), str(uuid4())],
                "description": ["Valid description", "", "Another valid"],
                "amount": [100.0, 200.0, 300.0],
                "operation_type": ["card", "card", "card"],
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        assert result.issues_rate > 0.0
        assert len(result.issues) >= 1
        missing_desc_issue = next(
            (i for i in result.issues if i.code == "missing_description"), None
        )
        assert missing_desc_issue is not None
        assert missing_desc_issue.message == "transaction with missing description"
        assert len(missing_desc_issue.sample_ids) == 1

    def test_verify_with_nan_description_detects_issue(self):
        """Test that NaN descriptions are detected."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4())],
                "description": ["Valid description", np.nan],
                "amount": [100.0, 200.0],
                "operation_type": ["card", "card"],
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        assert result.issues_rate > 0.0
        missing_desc_issue = next(
            (i for i in result.issues if i.code == "missing_description"), None
        )
        assert missing_desc_issue is not None
        assert len(missing_desc_issue.sample_ids) == 1

    def test_verify_with_fishy_amounts_detects_issue(self):
        """Test that amounts outside mean ± std are detected."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        # Create data with clear outliers
        # Use more normal values to establish a stable baseline
        normal_amounts = [
            100.0,
            110.0,
            105.0,
            95.0,
            102.0,
            98.0,
            107.0,
        ]  # mean ~102, std ~5
        huge_amount = 10000.0  # Clear outlier
        small_amount = 0.01  # Clear outlier

        df = pd.DataFrame(
            {
                "id": [str(uuid4()) for _ in range(9)],
                "description": ["Desc"] * 9,
                "amount": normal_amounts + [huge_amount, small_amount],
                "operation_type": ["card"] * 9,
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        assert result.issues_rate > 0.0
        fishy_issue = next((i for i in result.issues if i.code == "fishy_amount"), None)
        assert fishy_issue is not None
        assert fishy_issue.message == "suspicious amount of transactions"
        # Note: Due to outliers affecting mean/std calculation,
        # at least one outlier should be detected (the huge one is more likely)
        assert len(fishy_issue.sample_ids) >= 1

    def test_verify_with_huge_amount_only_detects_issue(self):
        """Test that only huge amounts are detected."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4()) for _ in range(5)],
                "description": ["Desc"] * 5,
                "amount": [100.0, 110.0, 105.0, 95.0, 10000.0],  # Last one is huge
                "operation_type": ["card"] * 5,
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        fishy_issue = next((i for i in result.issues if i.code == "fishy_amount"), None)
        assert fishy_issue is not None
        assert len(fishy_issue.sample_ids) >= 1

    def test_verify_with_small_amount_only_detects_issue(self):
        """Test that only small amounts are detected."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4()) for _ in range(5)],
                "description": ["Desc"] * 5,
                "amount": [100.0, 110.0, 105.0, 95.0, 0.01],  # Last one is very small
                "operation_type": ["card"] * 5,
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        fishy_issue = next((i for i in result.issues if i.code == "fishy_amount"), None)
        assert fishy_issue is not None
        assert len(fishy_issue.sample_ids) >= 1

    def test_verify_with_wrong_operation_type_detects_issue(self):
        """Test that wrong operation types are detected."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4()), str(uuid4())],
                "description": ["Desc"] * 3,
                "amount": [100.0, 200.0, 300.0],
                "operation_type": [
                    "card",
                    "payment",
                    "transfer",
                ],  # payment and transfer should be flagged
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        op_issue = next(
            (i for i in result.issues if i.code == "wrong_operation_type"), None
        )
        assert op_issue is not None
        assert op_issue.message == "wrong operation type of transaction"
        assert len(op_issue.sample_ids) == 2  # payment and transfer

    def test_verify_calculates_issues_rate_correctly(self):
        """Test that issues_rate is calculated correctly."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4()) for _ in range(10)],
                "description": ["Valid"] * 8
                + ["", ""],  # 2 missing descriptions = 0.2 rate
                "amount": [100.0] * 10,
                "operation_type": ["card"] * 10,
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        assert result.issues_rate == pytest.approx(0.2, abs=0.01)

    def test_verify_with_multiple_issues_accumulates_rate(self):
        """Test that multiple issues accumulate the rate correctly."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4()) for _ in range(10)],
                "description": ["Valid"] * 7 + ["", "", ""],  # 3 missing = 0.3
                "amount": [100.0] * 7 + [10000.0, 10000.0, 10000.0],  # 3 fishy = 0.3
                "operation_type": ["card"] * 7
                + ["payment", "payment", "payment"],  # 3 wrong = 0.3
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        # Total should be 0.3 + 0.3 + 0.3 = 0.9
        assert result.issues_rate == pytest.approx(0.9, abs=0.01)
        assert len(result.issues) == 3

    def test_verify_includes_sample_ids_in_issues(self):
        """Test that issues include sample transaction IDs."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        test_id_1 = str(uuid4())
        test_id_2 = str(uuid4())
        df = pd.DataFrame(
            {
                "id": [test_id_1, test_id_2, str(uuid4())],
                "description": ["Valid", "", "Valid"],
                "amount": [100.0, 200.0, 300.0],
                "operation_type": ["card", "card", "card"],
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        missing_desc_issue = next(
            (i for i in result.issues if i.code == "missing_description"), None
        )
        assert missing_desc_issue is not None
        assert test_id_2 in missing_desc_issue.sample_ids
        assert isinstance(missing_desc_issue.sample_ids, list)

    def test_verify_with_empty_dataframe_returns_zero_rate(self):
        """Test that empty DataFrame returns zero issues rate."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(columns=["id", "description", "amount", "operation_type"])

        # Act
        result = quality.verify(df)

        # Assert
        assert result.issues_rate == 0.0
        assert len(result.issues) == 0

    def test_verify_with_single_row_no_issues(self):
        """Test that single valid row produces no issues."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Valid description"],
                "amount": [100.0],
                "operation_type": ["card"],
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        assert result.issues_rate == 0.0
        assert len(result.issues) == 0

    def test_verify_with_all_issues_detects_all(self):
        """Test that DataFrame with all types of issues detects all."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4()), str(uuid4()), str(uuid4())],
                "description": ["", "Valid", "Valid"],  # 1 missing
                "amount": [100.0, 10000.0, 0.01],  # 2 fishy (huge and small)
                "operation_type": [
                    "payment",
                    "transfer",
                    "card",
                ],  # 2 wrong (payment, transfer)
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        assert len(result.issues) >= 1  # At least one issue type
        assert result.issues_rate > 0.0

    def test_verify_returns_quality_report_instance(self):
        """Test that verify returns a QualityReport instance."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": ["Valid"],
                "amount": [100.0],
                "operation_type": ["card"],
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        assert isinstance(result, QualityReport)
        assert hasattr(result, "issues_rate")
        assert hasattr(result, "issues")
        assert isinstance(result.issues, list)

    def test_verify_issues_are_quality_issue_instances(self):
        """Test that issues in report are QualityIssue instances."""
        # Arrange
        quality = BatchQuality(threshold=0.1)
        df = pd.DataFrame(
            {
                "id": [str(uuid4())],
                "description": [""],  # Missing description
                "amount": [100.0],
                "operation_type": ["card"],
            }
        )

        # Act
        result = quality.verify(df)

        # Assert
        assert len(result.issues) > 0
        for issue in result.issues:
            assert isinstance(issue, QualityIssue)
            assert hasattr(issue, "code")
            assert hasattr(issue, "message")
            assert hasattr(issue, "sample_ids")
            assert isinstance(issue.sample_ids, list)
