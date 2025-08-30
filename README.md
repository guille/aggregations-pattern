# CDC Aggregations pattern

Small demo app to demonstrate the CDC pattern for serving aggregated data.

## Technologies used

Web app

- Python back-end: FastAPI server
- Front-end: htmx + Tailwind

Infrastructure:

- PostgreSQL
- Debezium
- Apache Pulsar
- Docker Compose

## Getting started

Installing [mise](https://mise.jdx.dev/) is recommended but not required.

With mise:

```sh
docker compose up -d
mise run server
mise run worker
```

Without:

```sh
docker compose up -d
uv run -- uvicorn aggregations.webapp.app:app --reload
uv run -- python -m aggregations.aggregator.worker
```

A git hook is also provided to ensure committed code is well formatted and checked, it can be installed with `mise run hooks` or by copying the files in the [hooks directory](/hooks) to `.git/hooks`.
