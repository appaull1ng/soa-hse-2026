ALTER TABLE movie_analytics.daily_metric_versions
ADD COLUMN IF NOT EXISTS retention_days Array(Nullable(Float64))
DEFAULT [if(cohort_size > 0, 1.0, NULL), retention_d1, NULL, NULL, NULL, NULL, NULL, retention_d7];
