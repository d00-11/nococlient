import os
import importlib.util
import pytest

if importlib.util.find_spec("requests") is None:
    pytest.skip("requests package not installed", allow_module_level=True)

from src import NocoDBClient

@pytest.fixture(scope="module")
def client():
    return NocoDBClient()


def test_validate_connection(client):
    if not os.environ.get("NOCO_TEST_ONLINE"):
        pytest.skip("NOCO_TEST_ONLINE not set; skipping integration test")
    assert client.validate_connection()
