WITH
    {metric_day:Date32} AS report_day,
    {today:Date32} AS current_day,
    day_events AS (
        SELECT event_id,
               any(user_id) AS viewer,
               any(movie_id) AS movie,
               any(event_type) AS kind,
               any(progress_seconds) AS progress
        FROM movie_analytics.raw_events
        WHERE toDate32(timestamp) = report_day
        GROUP BY event_id
    ),
    daily AS (
        SELECT count() AS events_processed,
               uniqExact(viewer) AS dau,
               avgOrNullIf(progress, kind = 'VIEW_FINISHED') AS avg_watch_seconds,
               countIf(kind = 'VIEW_FINISHED') / nullIf(countIf(kind = 'VIEW_STARTED'), 0)
                   AS view_conversion
        FROM day_events
    ),
    movie_counts AS (
        SELECT movie, count() AS views
        FROM day_events
        WHERE kind = 'VIEW_STARTED'
        GROUP BY movie
    ),
    cohort AS (
        SELECT user_id,
               groupUniqArray(dateDiff('day', report_day, toDate32(timestamp))) AS active_days
        FROM movie_analytics.raw_events
        WHERE event_type = 'VIEW_STARTED'
          AND toDate32(timestamp) <= addDays(report_day, 7)
        GROUP BY user_id
        HAVING min(toDate32(timestamp)) = report_day
    ),
    retention_counts AS (
        SELECT count() AS cohort_size,
               sumForEach(arrayMap(day -> toUInt64(has(active_days, day)), range(8))) AS counts
        FROM cohort
    ),
    retention AS (
        SELECT cohort_size,
               arrayMap(day -> if(day = 0 OR addDays(report_day, day) < current_day,
                                  counts[day + 1] / nullIf(cohort_size, 0), NULL),
                        range(8)) AS retention_days
        FROM retention_counts
    )
SELECT events_processed, dau, avg_watch_seconds, view_conversion,
       cohort_size, retention_days, retention_days[2] AS retention_d1, retention_days[8] AS retention_d7,
       (
           SELECT arraySort(item -> (-toInt128(item.2), item.1), groupArray((movie, views)))
           FROM movie_counts
       ) AS top_movies
FROM daily CROSS JOIN retention
FORMAT JSONEachRow
