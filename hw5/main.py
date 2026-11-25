import logging
import os
from contextlib import asynccontextmanager
from threading import Event, Thread

from fastapi import FastAPI, HTTPException, Request

from events import MovieEvent, sample_session
from producer import MoviePublisher, PublishError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)


def generate_events(publisher: MoviePublisher, stop: Event) -> None:
    while not stop.is_set():
        try:
            for event in sample_session():
                if stop.is_set():
                    return
                publisher.publish(event)
                if stop.wait(1):
                    return
        except PublishError:
            LOG.exception("Could not publish a generated event")
            stop.wait(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = MoviePublisher(
        brokers=os.getenv("KAFKA_BROKERS", "kafka:29092"),
        registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
    )
    app.state.publisher = publisher

    stop = Event()
    generator = None
    if os.getenv("GENERATE_EVENTS", "0") == "1":
        generator = Thread(target=generate_events, args=(publisher, stop), daemon=True)
        generator.start()
    try:
        yield
    finally:
        stop.set()
        if generator is not None:
            generator.join()


app = FastAPI(title="Movie Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", status_code=201)
def create_event(event: MovieEvent, request: Request):
    try:
        request.app.state.publisher.publish(event)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except PublishError as error:
        LOG.exception("Could not publish API event")
        raise HTTPException(status_code=503, detail="Event publication failed") from error
    return {"event_id": event.event_id}
