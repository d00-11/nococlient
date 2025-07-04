import os
import importlib.util
import pytest

if importlib.util.find_spec("requests") is None:
    pytest.skip("requests package not installed", allow_module_level=True)
if importlib.util.find_spec("pytest_docker") is None:
    pytest.skip("pytest-docker package not installed", allow_module_level=True)
if not os.environ.get("NOCO_TEST_ONLINE"):
    pytest.skip("NOCO_TEST_ONLINE not set; skipping integration test", allow_module_level=True)

from src import NocoDBClient

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(pytestconfig.rootdir, "docker-compose.yml")


@pytest.fixture(scope="module")
def client(docker_ip, docker_services):
    port = docker_services.port_for("nocodb", 8080)
    os.environ["NOCODB_BASE_URL"] = f"http://{docker_ip}:{port}/api/v2"
    nc = NocoDBClient()
    docker_services.wait_until_responsive(
        timeout=60.0,
        pause=1.0,
        check=nc.validate_connection,
    )
    return nc


def test_validate_connection(client):
    assert client.validate_connection()
