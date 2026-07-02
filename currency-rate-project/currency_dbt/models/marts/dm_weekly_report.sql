/*
Витрина для еженедельного отчета
*/
SELECT
  requested_date,
  char_code,
  currency_name,
  rate,
  prev_date_rate,
  daily_change_pct,
  avg_rate_7d,
  MIN(rate) OVER(PARTITION BY char_code) AS week_min_rate,
  MAX(rate) OVER(PARTITION BY char_code) AS week_max_rate
FROM {{ ref('int_daily_rates') }}
WHERE requested_date >= CURRENT_DATE - INTERVAL '7' DAY
  AND char_code IN ('USD', 'EUR', 'EGP', 'THB', 'TRY', 'VND')
ORDER BY requested_date DESC, char_code ASC