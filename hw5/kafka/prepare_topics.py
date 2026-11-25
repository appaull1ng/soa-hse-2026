import json
import re
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep

BROKERS = "kafka:29092,kafka-2:29092,kafka-3:29092"
BROKER_IDS = (1, 2, 3)
TOPICS = {"movie-events", "_schemas", "__consumer_offsets", "__transaction_state"}
PARTITION = re.compile(
    r"Topic:\s+(\S+)\s+Partition:\s+(\d+)\s+Leader:\s+-?\d+"
    r"\s+Replicas:\s+([\d,]+)\s+Isr:\s*([\d,]*)"
)


def kafka(command, *arguments):
    result = subprocess.run(
        [command, "--bootstrap-server", BROKERS, *arguments],
        text=True, capture_output=True, timeout=45,
    )
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def partitions():
    description = kafka("kafka-topics", "--describe")
    for topic, partition, replicas, isr in PARTITION.findall(description):
        if topic in TOPICS:
            yield topic, int(partition), [int(node) for node in replicas.split(",")], isr


def prepare_topics():
    kafka("kafka-topics", "--create", "--if-not-exists", "--topic", "movie-events",
          "--partitions", "3", "--replication-factor", "3", "--config", "min.insync.replicas=1")
    kafka("kafka-configs", "--alter", "--entity-type", "topics", "--entity-name", "movie-events",
          "--add-config", "min.insync.replicas=1")

    changes = [
        {"topic": topic, "partition": partition,
         "replicas": replicas + [node for node in BROKER_IDS if node not in replicas]}
        for topic, partition, replicas, _ in partitions() if len(replicas) < len(BROKER_IDS)
    ]
    if changes:
        with TemporaryDirectory() as directory:
            plan = Path(directory) / "replicas.json"
            plan.write_text(json.dumps({"version": 1, "partitions": changes}))
            print(kafka("kafka-reassign-partitions", "--reassignment-json-file", str(plan), "--execute"))

    deadline = monotonic() + 90
    while monotonic() < deadline:
        current = list(partitions())
        if any(topic == "movie-events" for topic, *_ in current) and all(
            len(replicas) == len(BROKER_IDS) and len(isr.split(",")) == len(BROKER_IDS)
            for _, _, replicas, isr in current
        ):
            print("Kafka topics are ready with three in-sync replicas")
            return
        sleep(2)
    raise RuntimeError("Kafka topic replicas did not synchronize in time")


if __name__ == "__main__":
    prepare_topics()
