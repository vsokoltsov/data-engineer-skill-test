import pytest
from unittest.mock import Mock, AsyncMock, call

from pipelines.services.csv import CSVTransactionIngestService


@pytest.mark.asyncio
@pytest.mark.unit
class TestCSVTransactionIngestService:
    async def test_run_happy_path_multiple_chunks_sums_rows_and_commits_each_chunk(
        self, monkeypatch
    ):
        chunk1 = Mock(name="chunk1")
        chunk2 = Mock(name="chunk2")

        trx1 = [{"id": "1"}, {"id": "2"}]
        trx2 = [{"id": "3"}]

        chunk1.to_dict.return_value = trx1
        chunk2.to_dict.return_value = trx2

        csv_reader = Mock()
        csv_reader.read_batches.return_value = [chunk1, chunk2]

        # ML predictions
        ml_api = Mock()
        pred1 = [{"transaction_id": "1", "category": "Food"}]
        pred2 = [{"transaction_id": "3", "category": "Travel"}]
        ml_api.predict.side_effect = [pred1, pred2]

        # merge_predictions -> returns df-like with .to_dict(...)
        merged_df1 = Mock(name="merged_df1")
        merged_df2 = Mock(name="merged_df2")

        rows_for_upsert1 = [{"id": "1", "category": "Food"}]
        rows_for_upsert2 = [{"id": "3", "category": "Travel"}]

        merged_df1.to_dict.return_value = rows_for_upsert1
        merged_df2.to_dict.return_value = rows_for_upsert2

        merge_predictions_mock = Mock(side_effect=[merged_df1, merged_df2])
        monkeypatch.setattr(
            "pipelines.services.csv.merge_predictions",
            merge_predictions_mock,
        )

        # repo mock inside TransactionRepository(...)
        repo = Mock()
        repo.upsert_many = AsyncMock(side_effect=[2, 1])  # affected per chunk

        # Patch TransactionRepository constructor to return our repo mock
        monkeypatch.setattr(
            "pipelines.services.csv.TransactionRepository",
            Mock(return_value=repo),
        )

        # session + factory (async context manager)
        session = Mock()
        session.commit = AsyncMock()

        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)

        session_factory = Mock(return_value=cm)

        svc = CSVTransactionIngestService(
            session_factory=session_factory,
            ml_api=ml_api,
            csv_reader=csv_reader,
        )

        # --- act
        total = await svc.run()

        # --- assert
        assert total == 3

        csv_reader.read_batches.assert_called_once_with(chunk_size=1000)

        assert ml_api.predict.call_args_list == [call(trx1), call(trx2)]

        assert merge_predictions_mock.call_args_list == [
            call(chunk=chunk1, predictions=pred1),
            call(chunk=chunk2, predictions=pred2),
        ]

        assert repo.upsert_many.await_args_list == [
            call(rows_for_upsert1),
            call(rows_for_upsert2),
        ]

        assert session.commit.await_count == 2

    async def test_run_no_chunks_returns_0_and_does_not_call_ml_or_repo(
        self, monkeypatch
    ):
        # --- arrange
        csv_reader = Mock()
        csv_reader.read_batches.return_value = []

        ml_api = Mock()

        merge_predictions_mock = Mock()
        monkeypatch.setattr(
            "pipelines.services.utils.merge_predictions",
            merge_predictions_mock,
        )

        repo = Mock()
        repo.upsert_many = AsyncMock()

        monkeypatch.setattr(
            "pipelines.services.csv.TransactionRepository",
            Mock(return_value=repo),
        )

        session = Mock()
        session.commit = AsyncMock()

        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)

        session_factory = Mock(return_value=cm)

        svc = CSVTransactionIngestService(session_factory, ml_api, csv_reader)

        # --- act
        total = await svc.run()

        # --- assert
        assert total == 0
        ml_api.predict.assert_not_called()
        merge_predictions_mock.assert_not_called()
        repo.upsert_many.assert_not_awaited()
        session.commit.assert_not_awaited()

    async def test_run_propagates_ml_error_and_does_not_commit(self, monkeypatch):
        # --- arrange
        chunk = Mock()
        chunk.to_dict.return_value = [{"id": "1"}]

        csv_reader = Mock()
        csv_reader.read_batches.return_value = [chunk]

        ml_api = Mock()
        ml_api.predict.side_effect = RuntimeError("ml down")

        # merge should not be called due to predict error
        merge_predictions_mock = Mock()
        monkeypatch.setattr(
            "pipelines.services.csv.merge_predictions",
            merge_predictions_mock,
        )

        repo = Mock()
        repo.upsert_many = AsyncMock()

        monkeypatch.setattr(
            "pipelines.services.csv.TransactionRepository",
            Mock(return_value=repo),
        )

        session = Mock()
        session.commit = AsyncMock()

        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)

        session_factory = Mock(return_value=cm)

        svc = CSVTransactionIngestService(session_factory, ml_api, csv_reader)

        # --- act / assert
        with pytest.raises(RuntimeError, match="ml down"):
            await svc.run()

        merge_predictions_mock.assert_not_called()
        repo.upsert_many.assert_not_awaited()
        session.commit.assert_not_awaited()

    async def test_run_propagates_repo_error_and_does_not_commit_failed_chunk(
        self, monkeypatch
    ):
        # --- arrange
        chunk = Mock()
        chunk.to_dict.return_value = [{"id": "1"}]

        csv_reader = Mock()
        csv_reader.read_batches.return_value = [chunk]

        ml_api = Mock()
        pred = [{"transaction_id": "1", "category": "Food"}]
        ml_api.predict.return_value = pred

        merged_df = Mock()
        merged_df.to_dict.return_value = [{"id": "1", "category": "Food"}]

        merge_predictions_mock = Mock(return_value=merged_df)
        monkeypatch.setattr(
            "pipelines.services.csv.merge_predictions",
            merge_predictions_mock,
        )

        repo = Mock()
        repo.upsert_many = AsyncMock(side_effect=RuntimeError("db error"))

        monkeypatch.setattr(
            "pipelines.services.csv.TransactionRepository",
            Mock(return_value=repo),
        )

        session = Mock()
        session.commit = AsyncMock()

        cm = Mock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=None)

        session_factory = Mock(return_value=cm)

        svc = CSVTransactionIngestService(session_factory, ml_api, csv_reader)

        # --- act / assert
        with pytest.raises(RuntimeError, match="db error"):
            await svc.run()

        session.commit.assert_not_awaited()
