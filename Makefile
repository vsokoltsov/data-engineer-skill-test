mypy:
	uv run mypy pipelines/ tests/

black:
	black --check pipelines/ tests/

black-fix:
	black pipelines/ tests/

ruff:
	ruff check pipelines/ tests/ --fix

lint:
	make mypy & make black-fix & make ruff

unit-tests:
	uv run pytest -m unit

integration-tests:
	uv run pytest -m integration

e2e-tests:
	uv run pytest -m e2e

test-coverage:
	uv run pytest -q --cov=pipelines --cov-report=term-missing -m "not functional"

csv-ingestion:
	docker-compose up postgres ml-api csv-ingestion

kafka-consumer:
	docker-compose up postgres ml-api init-topics register-schemas zookeeper kafka schema-registry console consumer

kafka-producer:
	docker-compose up init-topics register-schemas zookeeper kafka schema-registry console producer

postgres:
	docker-compose up postgres pgadmin

kafka:
	docker-compose up kafka zookeeper schema-registry init-topics register-schemas schema-registry console producer consumer

observability:
	docker-compose up prometheus grafana loki promtail