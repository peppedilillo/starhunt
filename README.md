# Starhunt

Tools for consuming GCN notice and to monitor alerts from wide-field telescopes in real-time.

## Run

```shell
cp .env.sample .env
${EDITOR:-vi} .env
mkdir -p artifacts
docker compose --env-file .env build postgres consumer worker
docker compose --env-file .env up -d postgres consumer worker
docker compose --env-file .env ps
```

## Tests

```shell
docker compose --profile test up -d postgres-test
uv run pytest
# to perform smoke test against live endpoints
uv run pytest --smoke
```

## Linter rules
```shell
uv run black -l 120 . --target-version py313 & uv run isort --profile google .
```
