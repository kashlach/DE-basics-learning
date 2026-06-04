SELECT
  raw_json -> 'payload' ->> 'action' AS action,
  COUNT(*) AS cnt
FROM github_events_raw
WHERE raw_json ->> 'type' = 'IssuesEvent'
GROUP BY action
