import os
import pytest
import requests
from src import NocoDBClient

def _wait_for_nocodb(base):
    def check():
        try:
            r = requests.get(base)     # root returns 200 when UI is ready
            return r.ok
        except requests.RequestException:
            return False
    return check
@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(pytestconfig.rootdir, "docker-compose.yml")


@pytest.fixture(scope="session")
def api_token(docker_ip, docker_services):
    port = docker_services.port_for("nocodb", 8080)
    base = f"http://{docker_ip}:{port}"
    docker_services.wait_until_responsive(
        timeout=30.0,
        pause=0.1,
        check=_wait_for_nocodb(base)
    )
    requests.post(
        f"{base}/api/v1/auth/user/signup",
        json={
            "email": os.getenv("NC_USER", "admin@example.com"),
            "password": os.getenv("NC_PASS", "password"),
            "firstname": "Admin",
            "lastname": "Smith",
        }
    )
    r = requests.post(
        f"{base}/api/v1/auth/user/signin",
        json={"email": os.getenv("NC_USER", "admin@example.com"),
              "password": os.getenv("NC_PASS", "password")}
    )
    r.raise_for_status()
    jwt = r.json()["token"]

    projects = requests.get(f"{base}/api/v1/db/meta/projects",
                        headers={"xc-auth": jwt}).json()["list"]
    project_id = projects[0]["id"]

    create = requests.post(
        f"{base}/api/v1/db/meta/projects/{project_id}/api-tokens",
        headers={"xc-auth": jwt},
        json={"name": "my-long-term-token"}
    )
    create.raise_for_status()
    api_token = create.json()["token"]
    os.environ["NOCODB_API_KEY"] = api_token


@pytest.fixture(scope="session")
def client(docker_ip, docker_services, api_token):
    port = docker_services.port_for("nocodb", 8080)
    base = f"http://{docker_ip}:{port}/api/v2"
    os.environ["NOCODB_BASE_URL"] = base
    return NocoDBClient()