SELECT
    update_runs.update_id,
    update_runs.update_time,
    AVG(delays.avg_delay) AS avg_delay_all_stations
FROM update_runs
JOIN delays
  ON delays.update_id = update_runs.update_id
WHERE delays.avg_delay <> 0
GROUP BY
    update_runs.update_id,
    update_runs.update_time
ORDER BY
    update_runs.update_time;
