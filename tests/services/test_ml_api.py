import pytest
import requests

from pipelines.services.ml_api import MLPredictService


class DummyResponse:
    def __init__(self, status_code=200, json_data=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else []
        self.headers = headers or {}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if 400 <= self.status_code:
            raise requests.HTTPError(f"HTTP {self.status_code}")


@pytest.mark.unit
class TestMLPredictService:
    def test_predict_success_first_try(self, monkeypatch):
        calls = {"n": 0}

        def fake_post(url, json):
            calls["n"] += 1
            return DummyResponse(
                status_code=200,
                json_data=[
                    {
                        "transaction_id": "00000000-0000-0000-0000-000000000001",
                        "category": "Food",
                    }
                ],
            )

        monkeypatch.setattr(requests, "post", fake_post)

        svc = MLPredictService(url="http://localhost:8000/")
        out = svc.predict(trx=[])

        assert calls["n"] == 1
        assert out == [
            {
                "transaction_id": "00000000-0000-0000-0000-000000000001",
                "category": "Food",
            }
        ]

    def test_predict_retries_on_503_then_success(self, monkeypatch):
        # 503 -> 503 -> 200
        seq = [
            DummyResponse(status_code=503),
            DummyResponse(status_code=503),
            DummyResponse(
                status_code=200,
                json_data=[
                    {
                        "transaction_id": "00000000-0000-0000-0000-000000000002",
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
        # Было 2 ретрая => 2 sleep
        assert len(sleeps) == 2
        assert out == [
            {
                "transaction_id": "00000000-0000-0000-0000-000000000002",
                "category": "Travel",
            }
        ]

    def test_predict_uses_retry_after_header(self, monkeypatch):
        # 429 с Retry-After -> потом успех
        seq = [
            DummyResponse(status_code=429, headers={"Retry-After": "2"}),
            DummyResponse(
                status_code=200,
                json_data=[
                    {
                        "transaction_id": "00000000-0000-0000-0000-000000000003",
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

        assert sleeps == [2.0]  # использовали Retry-After
        assert out[0]["category"] == "Rent"

    def test_predict_raises_after_max_retries_on_status(self, monkeypatch):
        # Всегда 503 -> должно упасть после max_retries
        def fake_post(url, json):
            return DummyResponse(status_code=503)

        monkeypatch.setattr(requests, "post", fake_post)
        monkeypatch.setattr("time.sleep", lambda _: None)

        svc = MLPredictService(url="http://localhost:8000/")

        with pytest.raises(requests.HTTPError):
            svc.predict(trx=[])

    def test_predict_retries_on_timeout_then_success(self, monkeypatch):
        seq = [
            requests.Timeout("timeout"),
            DummyResponse(
                status_code=200,
                json_data=[
                    {
                        "transaction_id": "00000000-0000-0000-0000-000000000004",
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
        assert out[0]["category"] == "Food"

    def test_post_builds_correct_url(self, monkeypatch):
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
