.PHONY: test integration

test:
pytest -q

integration:
docker-compose up -d
pytest -q
docker-compose down
