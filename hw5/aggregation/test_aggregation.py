import json
import unittest
from datetime import date, datetime, timedelta
from uuid import uuid4

import httpx
import psycopg

from metrics import MetricsService
from s3_export import BUCKET, S3Exporter


class AggregationTest(unittest.TestCase):
    def setUp(self):
        self.service = MetricsService()
        self.addCleanup(self.service.clickhouse.close)
        self.user_prefix = str(uuid4())
        self.day = date(1960, 1, 15)
        # Use an unused date range so reruns see only this test's events.
        while int(self.service.query_clickhouse(
            """
            SELECT count() FROM movie_analytics.raw_events
            WHERE toDate32(timestamp) BETWEEN subtractDays({day:Date32}, 1) AND addDays({day:Date32}, 7)
            """,
            day=self.day,
        )):
            self.day += timedelta(days=10)

    def event(self, day_offset, user, movie, kind, progress=0):
        return {
            "event_id": str(uuid4()),
            "user_id": f"{self.user_prefix}-{user}",
            "movie_id": movie,
            "event_type": kind,
            "timestamp": f"{self.day + timedelta(days=day_offset)} 12:00:00.000",
            "device_type": "TV",
            "session_id": str(uuid4()),
            "progress_seconds": progress,
        }

    def insert_events(self, rows):
        self.service.query_clickhouse(
            "INSERT INTO movie_analytics.raw_events FORMAT JSONEachRow\n"
            + "\n".join(json.dumps(row) for row in rows)
        )

    def recalculate(self):
        response = httpx.post(f"http://aggregation-service:8001/recalculate/{self.day}", timeout=30)
        response.raise_for_status()
        return response.json()

    def test_calculate_export_and_recalculate(self):
        start = self.event(0, "alice", "a", "VIEW_STARTED")
        self.insert_events([
            self.event(-1, "existing", "b", "VIEW_STARTED"),
            start, start,  # Kafka can replay the same event ID.
            self.event(0, "alice", "a", "VIEW_FINISHED", 120),
            self.event(0, "bob", "a", "VIEW_STARTED"),
            self.event(0, "bob", "a", "VIEW_FINISHED", 240),
            self.event(0, "cara", "b", "VIEW_STARTED"),
            self.event(0, "existing", "b", "VIEW_STARTED"),
            self.event(0, "searcher", "b", "SEARCHED", None),
            self.event(1, "alice", "b", "VIEW_STARTED"),
            self.event(1, "alice", "b", "VIEW_STARTED"),
            self.event(1, "bob", "c", "LIKED", None),
            self.event(1, "existing", "c", "VIEW_STARTED"),
            self.event(7, "bob", "a", "VIEW_STARTED"),
            self.event(7, "cara", "b", "VIEW_STARTED"),
        ])

        expected = {
            "events_processed": 7,
            "dau": 5,
            "avg_watch_seconds": 180.0,
            "view_conversion": 0.5,
            "cohort_size": 3,
            "retention_d1": 1 / 3,
            "retention_d7": 2 / 3,
            "retention_days": [1.0, 1 / 3, 0.0, 0.0, 0.0, 0.0, 0.0, 2 / 3],
            "top_movies": [["a", 2], ["b", 2]],
        }
        first = self.recalculate()
        self.assertEqual({key: first[key] for key in expected}, expected)
        first_export = self.export_report()
        self.assertEqual(first_export["metrics"]["view_conversion"], 0.5)

        self.insert_events([
            self.event(0, "cara", "b", "VIEW_FINISHED", 360),
            self.event(0, "alice", "a", "VIEW_STARTED"),
        ])
        second = self.recalculate()
        expected.update(events_processed=9, avg_watch_seconds=240.0, view_conversion=0.6,
                        top_movies=[["a", 3], ["b", 2]])
        self.assertEqual({key: second[key] for key in expected}, expected)
        computed_at = self.assert_saved_snapshot(second, expected)
        self.assertGreater(computed_at, datetime.fromisoformat(first["computed_at"]))
        second_export = self.export_report()
        self.assertEqual(second_export["computed_at"], computed_at.isoformat())
        self.assertEqual(second_export["metrics"], {
            "dau": 5.0, "avg_watch_seconds": 240.0, "view_conversion": 0.6,
            "retention_d1": 1 / 3, "retention_d7": 2 / 3,
        })
        self.assertEqual(second_export["top_movies"], [
            {"movie_id": "a", "views": 3}, {"movie_id": "b", "views": 2},
        ])

    def export_report(self):
        response = httpx.post(f"http://aggregation-service:8001/export/{self.day}", timeout=30)
        response.raise_for_status()
        key = f"daily/{self.day}/aggregates.json"
        self.assertEqual(response.json(), {"path": f"s3://{BUCKET}/{key}"})
        exporter = S3Exporter(self.service.postgres_dsn)
        try:
            objects = exporter.s3.list_objects_v2(Bucket=BUCKET, Prefix=f"daily/{self.day}/")
            self.assertEqual([item["Key"] for item in objects["Contents"]], [key])
            with exporter.s3.get_object(Bucket=BUCKET, Key=key)["Body"] as body:
                document = json.load(body)
                self.assertEqual(document["date"], self.day.isoformat())
                return document
        finally:
            exporter.s3.close()

    def assert_saved_snapshot(self, result, expected):
        stored_rows = self.service.query_clickhouse(
            """
            SELECT * FROM movie_analytics.daily_metrics
            WHERE metric_date = {day:Date32} FORMAT JSONEachRow
            """,
            day=self.day,
        ).splitlines()
        self.assertEqual(len(stored_rows), 1)
        row = json.loads(stored_rows[0])
        self.assertEqual({key: row[key] for key in expected}, expected)

        with psycopg.connect(self.service.postgres_dsn) as connection:
            rows = connection.execute(
                "SELECT metric_name, movie_id, value, computed_at FROM daily_metrics WHERE metric_date = %s",
                (self.day,),
            ).fetchall()
        values = {
            ("dau", ""): 5.0,
            ("avg_watch_seconds", ""): 240.0,
            ("view_conversion", ""): 0.6,
            ("retention_d1", ""): 1 / 3,
            ("retention_d7", ""): 2 / 3,
            ("movie_views", "a"): 3.0,
            ("movie_views", "b"): 2.0,
        }
        self.assertEqual(len(rows), len(values))
        self.assertEqual({(name, movie): value for name, movie, value, _ in rows}, values)
        computed_at = datetime.fromisoformat(result["computed_at"])
        self.assertEqual({saved_at for _, _, _, saved_at in rows}, {computed_at})
        return computed_at
