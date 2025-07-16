import pytest

from nococlient import NocoDBClient, NocoDBConfig


def test_init_with_explicit_params(monkeypatch):
    monkeypatch.delenv("NOCODB_BASE_URL", raising=False)
    monkeypatch.delenv("NOCODB_API_KEY", raising=False)
    client = NocoDBClient(base_url="http://example.com", api_key="abc")
    assert client.config.base_url == "http://example.com"
    assert client.config.api_key == "abc"


def test_init_with_config(monkeypatch):
    monkeypatch.delenv("NOCODB_BASE_URL", raising=False)
    monkeypatch.delenv("NOCODB_API_KEY", raising=False)
    cfg = NocoDBConfig(base_url="http://cfg", api_key="key")
    client = NocoDBClient(config=cfg)
    assert client.config.base_url == "http://cfg"
    assert client.config.api_key == "key"


def test_init_missing(monkeypatch):
    monkeypatch.delenv("NOCODB_BASE_URL", raising=False)
    monkeypatch.delenv("NOCODB_API_KEY", raising=False)
    with pytest.raises(ValueError):
        NocoDBClient()
