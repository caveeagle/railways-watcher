SELECT
    stations.station_id,
    delays.avg_delay,
    delays.share_delayed,
    delays.update_id
FROM delays
JOIN stations
  ON stations.ID = delays.station_ID
WHERE delays.update_id = 5
ORDER BY stations.station_id
