.PHONY: test integration

test:
pytest -q

integration:
docker-compose up -d
NOCO_TEST_ONLINE=1 pytest -q
docker-compose down
