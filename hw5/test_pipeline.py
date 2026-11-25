import json
import unittest
from datetime import datetime
from time import monotonic, sleep
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4


def example_event(event_type: str, progress: int | None) -> dict:
    return {
        "event_id": str(uuid4()),
        "user_id": f"test-user-{uuid4()}",
        "movie_id": "test-movie",
        "event_type": event_type,
        "timestamp": "2026-11-18T21:00:00.123+03:00",
        "device_type": "TV",
        "session_id": str(uuid4()),
        "progress_seconds": progress,
    }


def post_event(event: dict):
    return urlopen(Request(
        "http://movie-service:8000/events",
        data=json.dumps(event).encode(),
        headers={"Content-Type": "application/json"},
    ), timeout=20)


def stored_event(event_id: str) -> list[dict]:
    query = """
        SELECT event_id, user_id, movie_id, event_type, device_type, session_id,
               progress_seconds, toUnixTimestamp64Milli(timestamp) AS timestamp_ms
        FROM movie_analytics.raw_events
        WHERE event_id = {event_id:UUID}
        FORMAT JSONEachRow
    """
    params = urlencode({"param_event_id": event_id, "output_format_json_quote_64bit_integers": 0})
    request = Request(f"http://clickhouse:8123/?{params}", data=query.encode(), headers={
        "X-ClickHouse-User": "analytics",
        "X-ClickHouse-Key": "local-analytics",
    })
    with urlopen(request, timeout=5) as response:
        return [json.loads(line) for line in response]


class PipelineTest(unittest.TestCase):
    def test_publish_and_read_from_clickhouse(self):
        for event_type, progress in (("VIEW_PAUSED", 600), ("LIKED", None)):
            with self.subTest(event_type=event_type):
                event = example_event(event_type, progress)
                with post_event(event) as response:
                    self.assertEqual(response.status, 201)
                    self.assertEqual(json.load(response), {"event_id": event["event_id"]})

                timestamp = datetime.fromisoformat(event.pop("timestamp"))
                event["timestamp_ms"] = int(timestamp.timestamp() * 1000)
                deadline = monotonic() + 45
                while monotonic() < deadline:
                    rows = stored_event(event["event_id"])
                    if rows:
                        self.assertEqual(rows, [event])
                        break
                    sleep(0.5)
                else:
                    self.fail(f"Event {event['event_id']} did not reach ClickHouse")

    def test_view_event_requires_progress(self):
        with self.assertRaises(HTTPError) as error:
            with post_event(example_event("VIEW_PAUSED", None)):
                pass
        self.assertEqual(error.exception.code, 422)
        error.exception.close()
