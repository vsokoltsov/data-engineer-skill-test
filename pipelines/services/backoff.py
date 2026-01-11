from __future__ import annotations

import random
import time
from functools import wraps
from typing import Callable, TypeVar, ParamSpec, Iterable

import requests

P = ParamSpec("P")
R = TypeVar("R")


def retry_with_backoff(
    *,
    max_retries: int = 5,
    base_delay_s: float = 0.5,
    max_delay_s: float = 10.0,
    jitter: bool = True,
    retry_statuses: Iterable[int] = (429, 500, 502, 503, 504),
    retry_exceptions: tuple[type[BaseException], ...] = (
        requests.Timeout,
        requests.ConnectionError,
    ),
) -> Callable[[Callable[P, requests.Response]], Callable[P, requests.Response]]:
    retry_statuses_set = set(retry_statuses)

    def decorator(fn: Callable[P, requests.Response]) -> Callable[P, requests.Response]:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> requests.Response:
            last_exc: BaseException | None = None

            for attempt in range(max_retries + 1):
                try:
                    resp = fn(*args, **kwargs)

                    if resp.status_code in retry_statuses_set:
                        if attempt == max_retries:
                            resp.raise_for_status()

                        retry_after = resp.headers.get("Retry-After")
                        sleep_s: float | None = None
                        if retry_after:
                            try:
                                sleep_s = min(float(retry_after), max_delay_s)
                            except ValueError:
                                sleep_s = None

                        _sleep_backoff(
                            attempt=attempt,
                            base_delay_s=base_delay_s,
                            max_delay_s=max_delay_s,
                            jitter=jitter,
                            override=sleep_s,
                        )
                        continue

                    resp.raise_for_status()
                    return resp

                except retry_exceptions as e:
                    last_exc = e
                    if attempt == max_retries:
                        raise
                    _sleep_backoff(
                        attempt=attempt,
                        base_delay_s=base_delay_s,
                        max_delay_s=max_delay_s,
                        jitter=jitter,
                        override=None,
                    )

            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator


def _sleep_backoff(
    *,
    attempt: int,
    base_delay_s: float,
    max_delay_s: float,
    jitter: bool,
    override: float | None,
) -> None:
    if override is not None:
        time.sleep(override)
        return

    delay = min(base_delay_s * (2**attempt), max_delay_s)
    if jitter:
        delay *= random.uniform(0.7, 1.3)
    time.sleep(delay)
