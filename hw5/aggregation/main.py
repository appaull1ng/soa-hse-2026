import logging
import os
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from threading import Event, Thread

from fastapi import FastAPI, HTTPException, Request

from metrics import AggregationError, MetricsService
from s3_export import ExportError, NoMetricsError, S3Exporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)


def run_schedule(service: MetricsService, exporter: S3Exporter, stop: Event, interval: int) -> None:
    pending_exports = set()
    scheduled_on = None
    while not stop.is_set():
        today = datetime.now(UTC).date()
        if scheduled_on != today:
            # Export yesterday and refresh older files when D1 and D7 become final.
            pending_exports.update(today - timedelta(days=offset) for offset in (1, 2, 8))
            scheduled_on = today
        days = pending_exports | {today - timedelta(days=offset) for offset in range(9)}
        for day in sorted(days):
            if stop.is_set():
                return
            try:
                service.recalculate(day)
            except AggregationError:
                continue  # The failed date is logged and retried next cycle.
            if day in pending_exports and not stop.is_set():
                try:
                    exporter.export(day)
                except (ExportError, NoMetricsError):
                    continue
                pending_exports.remove(day)
        stop.wait(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    interval = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "300"))
    if interval <= 0:
        raise ValueError("AGGREGATION_INTERVAL_SECONDS must be positive")
    service = MetricsService()
    exporter = S3Exporter(service.postgres_dsn)
    stop = Event()
    scheduler = Thread(target=run_schedule, args=(service, exporter, stop, interval), daemon=True)
    try:
        service.migrate()
        app.state.metrics = service
        app.state.exporter = exporter
        scheduler.start()
        yield
    finally:
        stop.set()
        if scheduler.is_alive():
            scheduler.join()
        service.clickhouse.close()
        exporter.s3.close()


app = FastAPI(title="Aggregation Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/recalculate/{metric_date}")
def recalculate(metric_date: date, request: Request):
    if not date(1900, 1, 1) <= metric_date <= datetime.now(UTC).date():
        raise HTTPException(status_code=422, detail="Date must be between 1900-01-01 and today (UTC)")
    try:
        return request.app.state.metrics.recalculate(metric_date)
    except AggregationError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@app.post("/export/{metric_date}")
def export(metric_date: date, request: Request):
    try:
        return {"path": request.app.state.exporter.export(metric_date)}
    except NoMetricsError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ExportError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
