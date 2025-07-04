import requests
from . import config


class NocoClient:
    def __init__(self, base_url: str | None = None, headers: dict | None = None):
        self.base_url = (base_url or config.NOCO_URL).rstrip('/')
        self.session = requests.Session()
        self.headers = headers or {}

    def validate_connection(self) -> bool:
        try:
            r = self.session.get(
                f"{self.base_url}/api/v1/meta/bases",
                headers=self.headers,
                timeout=5,
            )
            r.raise_for_status()
            return True
        except requests.RequestException:
            return False

