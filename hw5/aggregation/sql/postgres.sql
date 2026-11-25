CREATE TABLE IF NOT EXISTS daily_metrics
(
    metric_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    movie_id TEXT NOT NULL DEFAULT '',
    value DOUBLE PRECISION,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, metric_name, movie_id)
);
