CREATE TABLE IF NOT EXISTS movie_analytics.raw_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Nullable(Int32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate32(timestamp), user_id, timestamp, event_id);

-- Reattach the reader with the new broker list and the same Kafka consumer group.
DROP TABLE IF EXISTS movie_analytics.save_movie_events SYNC;
DROP TABLE IF EXISTS movie_analytics.kafka_events SYNC;

CREATE TABLE IF NOT EXISTS movie_analytics.kafka_events AS movie_analytics.raw_events
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092,kafka-2:29092,kafka-3:29092',
         kafka_topic_list = 'movie-events',
         kafka_group_name = 'clickhouse-movie-events',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081',
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS movie_analytics.save_movie_events
TO movie_analytics.raw_events
AS SELECT * FROM movie_analytics.kafka_events;
