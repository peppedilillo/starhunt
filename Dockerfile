FROM ghcr.io/astral-sh/uv:python3.13-bookworm

WORKDIR /app

ENV PATH="/app/.venv/bin:${PATH}" \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
COPY scripts ./scripts
COPY starhunt ./starhunt

RUN uv sync --frozen --no-dev

CMD ["work", "/data/artifacts"]
