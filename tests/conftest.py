import os
import pytest
import requests
from nococlient.client import NocoClient


@pytest.fixture(scope="session")
def api_token(docker_ip, docker_services):
    port = docker_services.wait_for_service("nocodb", 8080)
    base = f"http://{docker_ip}:{port}"

    r = requests.post(
        f"{base}/api/v1/auth/signin",
        json={"email": os.getenv("NC_USER", "admin@example.com"),
              "password": os.getenv("NC_PASS", "password")}
    )
    r.raise_for_status()
    jwt = r.json()["jwt"]

    headers = {"xc-auth": jwt}
    r = requests.get(
        f"{base}/api/v1/db/meta/projects/{os.getenv('NC_PROJECT_ID', '1')}/api-tokens",
        headers=headers
    )
    r.raise_for_status()
    tokens = r.json()
    if tokens:
        return tokens[0]["token"]

    r = requests.post(
        f"{base}/api/v1/db/meta/projects/{os.getenv('NC_PROJECT_ID', '1')}/api-tokens",
        headers=headers
    )
    r.raise_for_status()
    return r.json()["token"]


@pytest.fixture(scope="session")
def client(docker_ip, docker_services, api_token):
    port = docker_services.port_for("nocodb", 8080)
    base = f"http://{docker_ip}:{port}"
    return NocoClient(base_url=base, headers={"xc-token": api_token})

