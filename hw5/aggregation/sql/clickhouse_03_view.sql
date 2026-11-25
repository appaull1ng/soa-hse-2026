CREATE OR REPLACE VIEW movie_analytics.daily_metrics AS
SELECT * FROM movie_analytics.daily_metric_versions FINAL;
