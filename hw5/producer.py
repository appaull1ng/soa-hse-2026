import json
import logging
from pathlib import Path
from threading import Lock

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext
from fastavro import parse_schema
from fastavro.validation import validate

from events import MovieEvent

LOG = logging.getLogger(__name__)
TOPIC = "movie-events"
PARTITIONS = 3
REPLICAS = 3


class PublishError(RuntimeError):
    pass


class MoviePublisher:
    def __init__(self, brokers: str, registry_url: str):
        self._create_topic(brokers)
        schema_text = Path(__file__).with_name("movie_event.avsc").read_text()
        self.avro_schema = parse_schema(json.loads(schema_text))

        registry = SchemaRegistryClient({"url": registry_url})
        schema = Schema(schema_text, "AVRO")
        subject = f"{TOPIC}-value"
        registry.register_schema(subject, schema)
        registered = registry.lookup_schema(subject, schema)
        LOG.info(
            "Registered %s version=%s schema_id=%s", subject, registered.version, registered.schema_id
        )

        self.serialize = AvroSerializer(registry, schema_text, conf={"auto.register.schemas": False})
        self.producer = Producer({
            "bootstrap.servers": brokers,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 100,
            "retry.backoff.max.ms": 1000,
            "message.timeout.ms": 10_000,
        })
        self.lock = Lock()

    @staticmethod
    def _create_topic(brokers: str) -> None:
        admin = AdminClient({"bootstrap.servers": brokers})
        topic = NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=REPLICAS,
                         config={"min.insync.replicas": "1"})
        try:
            admin.create_topics([topic], request_timeout=10)[TOPIC].result()
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            metadata = admin.list_topics(TOPIC, timeout=10).topics[TOPIC]
            if metadata.error or len(metadata.partitions) < PARTITIONS:
                raise RuntimeError(f"{TOPIC} must have at least {PARTITIONS} partitions") from error
            if any(len(partition.replicas) < REPLICAS for partition in metadata.partitions.values()):
                raise RuntimeError(f"{TOPIC} must have {REPLICAS} replicas per partition") from error

    def publish(self, event: MovieEvent) -> None:
        data = event.model_dump()
        data["event_id"] = str(event.event_id)
        if not validate(data, self.avro_schema, raise_errors=False):
            raise ValueError("Event does not match the Avro schema")

        delivery_error = None

        def on_delivery(error, _message):
            nonlocal delivery_error
            delivery_error = error

        try:
            with self.lock:
                value = self.serialize(data, SerializationContext(TOPIC, MessageField.VALUE))
                self.producer.produce(
                    TOPIC, key=event.user_id.encode(), value=value, on_delivery=on_delivery
                )
                self.producer.flush()
        except (KafkaException, SchemaRegistryError, BufferError) as error:
            raise PublishError(f"Could not send event {event.event_id}") from error

        if delivery_error is not None:
            raise PublishError(f"Delivery failed for {event.event_id}: {delivery_error}")

        LOG.info(
            "Published event_id=%s event_type=%s timestamp=%s",
            event.event_id, event.event_type, event.timestamp,
        )
