import json
import logging
import os
from datetime import date
from threading import Lock

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

LOG = logging.getLogger(__name__)
BUCKET = "movie-analytics"


class NoMetricsError(RuntimeError):
    pass


class ExportError(RuntimeError):
    pass


class S3Exporter:
    def __init__(self, postgres_dsn: str):
        self.postgres_dsn = postgres_dsn
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://rustfs:9000"),
            region_name="us-east-1",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "analytics"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "local-analytics"),
            config=Config(connect_timeout=3, read_timeout=10, retries={"total_max_attempts": 1},
                          s3={"addressing_style": "path"}),
        )
        self.lock = Lock()

    def _ensure_bucket(self):
        try:
            self.s3.head_bucket(Bucket=BUCKET)
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            self.s3.create_bucket(Bucket=BUCKET)

    def export(self, day: date) -> str:
        key = f"daily/{day}/aggregates.json"
        with self.lock:
            try:
                with psycopg.connect(self.postgres_dsn, connect_timeout=5) as connection:
                    rows = connection.execute(
                        """
                        SELECT metric_name, movie_id, value, computed_at
                        FROM daily_metrics WHERE metric_date = %s
                        ORDER BY metric_name, value DESC, movie_id
                        """,
                        (day,),
                    ).fetchall()
                if not rows:
                    raise NoMetricsError(f"No aggregates for {day}; recalculate this date first")
                document = {
                    "date": day.isoformat(),
                    "computed_at": rows[0][3].isoformat(),
                    "metrics": {name: value for name, movie, value, _ in rows if not movie},
                    "top_movies": [
                        {"movie_id": movie, "views": int(value)} for name, movie, value, _ in rows
                        if name == "movie_views"
                    ],
                }
                self._ensure_bucket()
                self.s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(document).encode(),
                                   ContentType="application/json")
            except (psycopg.Error, BotoCoreError, ClientError) as error:
                LOG.exception("S3 export failed date=%s", day)
                raise ExportError(f"Could not export {day}") from error

        path = f"s3://{BUCKET}/{key}"
        LOG.info("Exported metrics date=%s path=%s", day, path)
        return path
