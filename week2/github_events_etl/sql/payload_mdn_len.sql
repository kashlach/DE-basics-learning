SELECT
  e.type,
  COUNT(*) AS cnt,
  ROUND(
     PERCENTILE_CONT(0.5)
     WITHIN GROUP (ORDER BY LENGTH(r.raw_json ->> 'payload'))
    ) AS mdn_size
FROM github_events_raw r
JOIN github_events e ON r.event_id = e.event_id
GROUP BY e.type
ORDER BY e.type
