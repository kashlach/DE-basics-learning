SELECT
  type,
  COUNT(*) AS type_cnt,
  ROUND(
     COUNT(*) * 100 / SUM(COUNT(*)) OVER(),
     2) AS pct
FROM github_events
GROUP BY type
ORDER BY pct DESC, type
