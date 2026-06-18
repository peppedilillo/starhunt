import importlib
import sys

import pytest

from starhunt.exceptions import MissingEnvironmentVariable


def reload_queue(
    monkeypatch,
    *,
    redis_host="redis.example.test",
    redis_port="6380",
    redis_db="2",
):
    sys.modules.pop("starhunt.queue", None)
    for name, value in {
        "REDIS_HOST": redis_host,
        "REDIS_PORT": redis_port,
        "REDIS_DB": redis_db,
    }.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    return importlib.import_module("starhunt.queue")


@pytest.mark.parametrize("missing", ["REDIS_HOST", "REDIS_PORT", "REDIS_DB"])
def test_queue_requires_redis_configuration(monkeypatch, missing):
    values = {
        "redis_host": "localhost",
        "redis_port": "6379",
        "redis_db": "0",
        missing.lower(): None
    }

    with pytest.raises(MissingEnvironmentVariable, match=missing):
        reload_queue(monkeypatch, **values)


def test_queue_uses_configured_redis_connection(monkeypatch):
    queue_module = reload_queue(monkeypatch)

    connection_kwargs = queue_module.redis_connection.connection_pool.connection_kwargs
    assert connection_kwargs["host"] == "redis.example.test"
    assert connection_kwargs["port"] == 6380
    assert connection_kwargs["db"] == 2
