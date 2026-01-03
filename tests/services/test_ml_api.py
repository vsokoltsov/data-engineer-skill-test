import pytest
import requests
from uuid import uuid4
from datetime import datetime

from pipelines.services.ml_api import MLPredictService
from pipelines.services.models import TransactionRequest
from pipelines.services.errors import MLAPIServiceError


class DummyResponse:
    def __init__(self, status_code=200, json_data=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else []
        self.headers = headers or {}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            raise requests.HTTPError(f"HTTP {self.status_code}")


@pytest.mark.unit
class TestMLPredictService:
    """Unit tests for MLPredictService class."""

    def test_predict_success_first_try(self, monkeypatch):
        """Test successful prediction on first attempt."""
        calls = {"n": 0}

        def fake_post(url, json):
            calls["n"] += 1
            return DummyResponse(
                status_code=200,
                json_data=[
                    {
                        "transaction_id": str(uuid4()),
                        "category": "Food",
                    }
                ],
            )

        monkeypatch.setattr(requests, "post", fake_post)

        svc = MLPredictService(url="http://localhost:8000/")
        trx = [
            TransactionRequest(
                id=uuid4(),
                description="Test transaction",
                amount=100.0,
                timestamp=datetime.now(),
                merchant="Test Merchant",
                operation_type="card",
                side="debit",
            )
        ]
        out = svc.predict(trx=trx)

        assert calls["n"] == 1
        assert len(out) == 1
        assert out[0].category == "Food"

    def test_predict_retries_on_503_then_success(self, monkeypatch):
        """Test retry on 503 status code then success."""
        seq = [
            DummyResponse(status_code=503),
            DummyResponse(status_code=503),
            DummyResponse(
                status_code=200,
                json_data=[
                    {
                        "transaction_id": str(uuid4()),
                        "category": "Travel",
                    }
                ],
            ),
        ]
        calls = {"n": 0}

        def fake_post(url, json):
            calls["n"] += 1
            return seq.pop(0)

        sleeps = []

        def fake_sleep(seconds):
            sleeps.append(seconds)

        monkeypatch.setattr(requests, "post", fake_post)
        monkeypatch.setattr("time.sleep", fake_sleep)

        svc = MLPredictService(url="http://localhost:8000/")
        out = svc.predict(trx=[])

        assert calls["n"] == 3
        assert len(sleeps) == 2
        assert out[0].category == "Travel"

    def test_predict_retries_on_429_then_success(self, monkeypatch):
        """Test retry on 429 status code then success."""
        seq = [
            DummyResponse(status_code=429),
            DummyResponse(
                status_code=200,
                json_data=[
                    {
                        "transaction_id": str(uuid4()),
                        "category": "Rent",
                    }
                ],
            ),
        ]

        sleeps = []

        def fake_post(url, json):
            return seq.pop(0)

        def fake_sleep(seconds):
            sleeps.append(seconds)

        monkeypatch.setattr(requests, "post", fake_post)
        monkeypatch.setattr("time.sleep", fake_sleep)

        svc = MLPredictService(url="http://localhost:8000/")
        out = svc.predict(trx=[])

        assert len(sleeps) == 1
        assert out[0].category == "Rent"

    def test_predict_uses_retry_after_header(self, monkeypatch):
        """Test that Retry-After header is used for retry delay."""
        seq = [
            DummyResponse(status_code=429, headers={"Retry-After": "2"}),
            DummyResponse(
                status_code=200,
                json_data=[
                    {
                        "transaction_id": str(uuid4()),
                        "category": "Rent",
                    }
                ],
            ),
        ]

        sleeps = []

        def fake_post(url, json):
            return seq.pop(0)

        def fake_sleep(seconds):
            sleeps.append(seconds)

        monkeypatch.setattr(requests, "post", fake_post)
        monkeypatch.setattr("time.sleep", fake_sleep)

        svc = MLPredictService(url="http://localhost:8000/")
        out = svc.predict(trx=[])

        assert sleeps == [2.0]  # Used Retry-After value
        assert out[0].category == "Rent"

    def test_predict_raises_mlapi_error_on_non_200_status(self, monkeypatch):
        """Test that non-200 status codes raise MLAPIServiceError."""

        def fake_post(url, json):
            response = DummyResponse(status_code=400)
            # Mock raise_for_status to not raise, so predict can check status_code
            response.raise_for_status = lambda: None
            return response

        monkeypatch.setattr(requests, "post", fake_post)

        svc = MLPredictService(url="http://localhost:8000/")

        with pytest.raises(MLAPIServiceError) as exc_info:
            svc.predict(trx=[])

        assert isinstance(exc_info.value.orig, ValueError)
        assert "http status is 400" in str(exc_info.value.orig)

    def test_predict_raises_after_max_retries_on_status(self, monkeypatch):
        """Test that service raises after max retries on retryable status."""

        def fake_post(url, json):
            return DummyResponse(status_code=503)

        monkeypatch.setattr(requests, "post", fake_post)
        monkeypatch.setattr("time.sleep", lambda _: None)

        svc = MLPredictService(url="http://localhost:8000/")

        with pytest.raises(requests.HTTPError):
            svc.predict(trx=[])

    def test_predict_retries_on_timeout_then_success(self, monkeypatch):
        """Test retry on timeout exception then success."""
        seq = [
            requests.Timeout("timeout"),
            DummyResponse(
                status_code=200,
                json_data=[
                    {
                        "transaction_id": str(uuid4()),
                        "category": "Food",
                    }
                ],
            ),
        ]
        calls = {"n": 0}
        sleeps = []

        def fake_post(url, json):
            calls["n"] += 1
            item = seq.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

        monkeypatch.setattr(requests, "post", fake_post)
        monkeypatch.setattr("time.sleep", lambda s: sleeps.append(s))

        svc = MLPredictService(url="http://localhost:8000/")
        out = svc.predict(trx=[])

        assert calls["n"] == 2
        assert len(sleeps) == 1
        assert out[0].category == "Food"

    def test_predict_raises_mlapi_error_on_timeout_after_max_retries(self, monkeypatch):
        """Test that timeout after max retries raises MLAPIServiceError."""

        def fake_post(url, json):
            raise requests.Timeout("timeout")

        monkeypatch.setattr(requests, "post", fake_post)
        monkeypatch.setattr("time.sleep", lambda _: None)

        svc = MLPredictService(url="http://localhost:8000/")

        with pytest.raises(MLAPIServiceError) as exc_info:
            svc.predict(trx=[])

        assert isinstance(exc_info.value.orig, requests.Timeout)

    def test_predict_raises_mlapi_error_on_connection_error(self, monkeypatch):
        """Test that connection error raises MLAPIServiceError."""

        def fake_post(url, json):
            raise requests.ConnectionError("Connection failed")

        monkeypatch.setattr(requests, "post", fake_post)
        monkeypatch.setattr("time.sleep", lambda _: None)

        svc = MLPredictService(url="http://localhost:8000/")

        with pytest.raises(MLAPIServiceError) as exc_info:
            svc.predict(trx=[])

        assert isinstance(exc_info.value.orig, Exception)
        assert "unable to connect to host" in str(exc_info.value.orig)

    def test_predict_raises_mlapi_error_on_json_decode_error(self, monkeypatch):
        """Test that JSON decode error raises MLAPIServiceError."""

        class BadJsonResponse:
            def __init__(self):
                self.status_code = 200
                self.headers = {}

            def json(self):
                raise requests.JSONDecodeError("Invalid JSON", "", 0)

            def raise_for_status(self):
                # Status is 200, so no error
                pass

        def fake_post(url, json):
            return BadJsonResponse()

        monkeypatch.setattr(requests, "post", fake_post)

        svc = MLPredictService(url="http://localhost:8000/")

        with pytest.raises(MLAPIServiceError) as exc_info:
            svc.predict(trx=[])

        assert isinstance(exc_info.value.orig, requests.JSONDecodeError)

    def test_predict_builds_correct_url(self, monkeypatch):
        """Test that URL is correctly built using urljoin."""
        captured = {}

        def fake_post(url, json):
            captured["url"] = url
            captured["json"] = json
            return DummyResponse(status_code=200, json_data=[])

        monkeypatch.setattr(requests, "post", fake_post)

        svc = MLPredictService(url="http://localhost:8000/")
        svc.predict(trx=[])

        # urljoin("http://localhost:8000/", "predict") -> "http://localhost:8000/predict"
        assert captured["url"] == "http://localhost:8000/predict"

    def test_predict_builds_correct_url_without_trailing_slash(self, monkeypatch):
        """Test URL building when base URL doesn't have trailing slash."""
        captured = {}

        def fake_post(url, json):
            captured["url"] = url
            return DummyResponse(status_code=200, json_data=[])

        monkeypatch.setattr(requests, "post", fake_post)

        svc = MLPredictService(url="http://localhost:8000")
        svc.predict(trx=[])

        assert captured["url"] == "http://localhost:8000/predict"

    def test_predict_passes_transaction_data_correctly(self, monkeypatch):
        """Test that transaction data is passed correctly to the API."""
        captured = {}

        def fake_post(url, json):
            captured["json"] = json
            return DummyResponse(status_code=200, json_data=[])

        monkeypatch.setattr(requests, "post", fake_post)

        svc = MLPredictService(url="http://localhost:8000/")
        trx_id = uuid4()
        trx = [
            TransactionRequest(
                id=trx_id,
                description="Test transaction",
                amount=100.0,
                timestamp=datetime(2024, 1, 1, 12, 0, 0),
                merchant="Test Merchant",
                operation_type="card",
                side="debit",
            )
        ]
        svc.predict(trx=trx)

        # TransactionRequest objects are passed, need to check their attributes
        assert len(captured["json"]) == 1
        assert captured["json"][0].id == trx_id
        assert captured["json"][0].description == "Test transaction"
        assert captured["json"][0].amount == 100.0

    def test_predict_handles_empty_transaction_list(self, monkeypatch):
        """Test that empty transaction list is handled correctly."""

        def fake_post(url, json):
            return DummyResponse(status_code=200, json_data=[])

        monkeypatch.setattr(requests, "post", fake_post)

        svc = MLPredictService(url="http://localhost:8000/")
        out = svc.predict(trx=[])

        assert out == []

    def test_predict_handles_multiple_transactions(self, monkeypatch):
        """Test prediction with multiple transactions."""

        def fake_post(url, json):
            return DummyResponse(
                status_code=200,
                json_data=[
                    {"transaction_id": str(uuid4()), "category": "Food"},
                    {"transaction_id": str(uuid4()), "category": "Travel"},
                    {"transaction_id": str(uuid4()), "category": "Rent"},
                ],
            )

        monkeypatch.setattr(requests, "post", fake_post)

        svc = MLPredictService(url="http://localhost:8000/")
        trx = [
            TransactionRequest(
                id=uuid4(),
                description=f"Transaction {i}",
                amount=100.0 * (i + 1),
                timestamp=datetime.now(),
                merchant=f"Merchant {i}",
                operation_type="card",
                side="debit",
            )
            for i in range(3)
        ]
        out = svc.predict(trx=trx)

        assert len(out) == 3
        assert out[0].category == "Food"
        assert out[1].category == "Travel"
        assert out[2].category == "Rent"

    def test_predict_retries_on_all_retryable_status_codes(self, monkeypatch):
        """Test retry on all configured retryable status codes."""
        retryable_statuses = [429, 500, 502, 503, 504]

        for status in retryable_statuses:
            seq = [
                DummyResponse(status_code=status),
                DummyResponse(
                    status_code=200,
                    json_data=[{"transaction_id": str(uuid4()), "category": "Food"}],
                ),
            ]

            def fake_post(url, json):
                return seq.pop(0)

            monkeypatch.setattr(requests, "post", fake_post)
            monkeypatch.setattr("time.sleep", lambda _: None)

            svc = MLPredictService(url="http://localhost:8000/")
            out = svc.predict(trx=[])

            assert out[0].category == "Food"
