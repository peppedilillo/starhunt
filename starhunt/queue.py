import os

from redis import Redis
from rq import Queue as RQQueue

from starhunt.exceptions import MissingEnvironmentVariable


def required_env(name: str) -> str:
    if (value := os.environ.get(name)) is None:
        raise MissingEnvironmentVariable(name)
    return value


redis_connection = Redis(
    host=required_env("REDIS_HOST"),
    port=int(required_env("REDIS_PORT")),
    db=int(required_env("REDIS_DB")),
)
queue = RQQueue("default", connection=redis_connection)
