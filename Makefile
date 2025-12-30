mypy:
	uv run mypy pipelines/ tests/

black:
	black --check pipelines/ tests/

black-fix:
	black pipelines/ tests/

ruff:
	ruff check pipelines/ tests/ --fix

unit-tests:
	uv run pytest -m unit

integration-tests:
	uv run pytest -m integration

e2e-tests:
	uv run pytest -m e2e

test-coverage:
	uv run pytest -q --cov=pipelines --cov-report=term-missing