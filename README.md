# Starhunt

Tools for searching optical counterparts to high-energy astrophysical transients.

## Run

```shell
cp .env.sample .env
${EDITOR:-vi} .env
mkdir -p artifacts # stores raw notices and non-empty cone-search responses
docker compose --env-file .env build
docker compose --env-file .env up -d
docker compose --env-file .env ps
docker compose --env-file .env logs consumer worker api | uv run pretty
```

To run the API locally: 

```shell
uv run uvicorn starhunt.api:app
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
uv run black -l 120 . --target-version py313 && uv run isort --profile google .
```
