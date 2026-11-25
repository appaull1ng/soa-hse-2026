import json
import logging
import os
from datetime import UTC, date, datetime
from pathlib import Path
from threading import Lock
from time import monotonic, sleep

import httpx
import psycopg

LOG = logging.getLogger(__name__)
SQL = Path(__file__).with_name("sql")
CALCULATE = (SQL / "calculate.sql").read_text()
SCALAR_METRICS = ("dau", "avg_watch_seconds", "view_conversion", "retention_d1", "retention_d7")
UPSERT_METRIC = """
    INSERT INTO daily_metrics (metric_date, metric_name, movie_id, value, computed_at)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (metric_date, metric_name, movie_id)
    DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
"""


class AggregationError(RuntimeError):
    pass


class MetricsService:
    def __init__(self):
        self.clickhouse = httpx.Client(
            base_url=os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123"),
            headers={
                "X-ClickHouse-User": os.getenv("CLICKHOUSE_USER", "analytics"),
                "X-ClickHouse-Key": os.getenv("CLICKHOUSE_PASSWORD", "local-analytics"),
            },
            timeout=30,
            trust_env=False,
        )
        self.postgres_dsn = os.getenv(
            "POSTGRES_DSN", "postgresql://analytics:local-analytics@postgres:5432/movie_analytics"
        )
        self.lock = Lock()

    def query_clickhouse(self, query: str, **parameters) -> str:
        params = {f"param_{name}": str(value) for name, value in parameters.items()}
        params.update(wait_end_of_query="1", output_format_json_quote_64bit_integers="0")
        response = self.clickhouse.post("/", params=params, content=query.encode())
        response.raise_for_status()
        return response.text

    def migrate(self) -> None:
        for migration in sorted(SQL.glob("clickhouse_*.sql")):
            self.query_clickhouse(migration.read_text())
        with psycopg.connect(self.postgres_dsn, connect_timeout=5) as connection:
            connection.execute((SQL / "postgres.sql").read_text())

    def recalculate(self, day: date) -> dict:
        with self.lock:
            start = monotonic()
            LOG.info("Starting aggregation date=%s", day)
            try:
                data = json.loads(self.query_clickhouse(
                    CALCULATE, metric_day=day, today=datetime.now(UTC).date()
                ))
                computed_at = datetime.now(UTC)
                row = {"metric_date": day.isoformat(), **data}
                snapshot = row | {"computed_at": computed_at.strftime("%Y-%m-%d %H:%M:%S.%f")}
                self.query_clickhouse(
                    "INSERT INTO movie_analytics.daily_metric_versions FORMAT JSONEachRow\n"
                    + json.dumps(snapshot)
                )
                self._save_postgres(day, computed_at, data)
            except (httpx.HTTPError, psycopg.Error, ValueError) as error:
                LOG.exception("Aggregation failed date=%s elapsed_seconds=%.3f", day, monotonic() - start)
                raise AggregationError(f"Could not recalculate {day}") from error

            LOG.info(
                "Finished aggregation date=%s events_processed=%s elapsed_seconds=%.3f",
                day, data["events_processed"], monotonic() - start,
            )
            return row | {"computed_at": computed_at}

    def _save_postgres(self, day: date, computed_at: datetime, data: dict) -> None:
        rows = [(day, name, "", data[name], computed_at) for name in SCALAR_METRICS]
        rows.extend((day, "movie_views", movie, views, computed_at) for movie, views in data["top_movies"])

        for attempt in range(1, 4):
            try:
                with psycopg.connect(self.postgres_dsn, connect_timeout=5) as connection:
                    with connection.cursor() as cursor:
                        cursor.executemany(UPSERT_METRIC, rows)
                    connection.execute(
                        "DELETE FROM daily_metrics WHERE metric_date = %s AND computed_at <> %s",
                        (day, computed_at),
                    )
                return
            except psycopg.Error as error:
                LOG.warning("PostgreSQL write failed date=%s attempt=%s/3 error=%s", day, attempt, error)
                if attempt == 3:
                    raise
                sleep(attempt)
