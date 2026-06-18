# Starhunt

## Queue

Copy `.env.sample` to `.env`, then load it before starting workers:

```shell
set -a
source .env
set +a
docker compose up -d redis
uv run rq worker --url "redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}" --with-scheduler
uv run rq-dashboard --redis-url "redis://${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}"
```

## Tests

```shell
docker compose --profile test up -d postgres-test
uv run pytest
```

## Linter rules
```shell
uv run black -l 120 . --target-version py313 & uv run isort --profile google .
```
