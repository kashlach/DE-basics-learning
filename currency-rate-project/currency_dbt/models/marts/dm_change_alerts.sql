/*
Витрина для алертов по USD. Триггер на изменение более чем на 2%
*/
SELECT
  char_code,
  requested_date,
  rate AS current_rate,
  prev_date_rate,
  daily_change_pct,
  CASE
	WHEN ABS(daily_change_pct) > 2 THEN TRUE
	ELSE FALSE
  END AS alert_triggered
FROM {{ ref('int_daily_rates') }}
WHERE char_code = 'USD'
  AND daily_change_pct IS NOT NULL
ORDER BY requested_date DESC
