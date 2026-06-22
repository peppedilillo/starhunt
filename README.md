# Starhunt

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
