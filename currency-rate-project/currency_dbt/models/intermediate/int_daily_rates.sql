/*
 - Курс за предыдущий день prev_date_rate
 - Изменение курса по сравнению с вчерашним в процентах daily_change_pct
 - Скользящее среднее за 7 дней avg_rate_7d
*/
WITH prev_val AS (
  SELECT
    requested_date,
	received_date,
	char_code,
	nominal,
	currency_name,
	value,
	rate,
	LAG(rate) OVER(
	   PARTITION BY char_code 
	   ORDER BY requested_date
	) AS prev_date_rate
  FROM {{ ref('stg_currency_rates') }}
)
SELECT 
*,
CASE
  WHEN prev_date_rate IS NOT NULL 
  THEN ROUND((rate - prev_date_rate) / prev_date_rate * 100, 2)
  ELSE NULL
END AS daily_change_pct,
ROUND(
  AVG(rate) OVER(
	PARTITION BY char_code
	ORDER BY requested_date DESC
	ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING
  ),
4)	AS avg_rate_7d
FROM prev_val