/*
raw_json = {"Date": "...", "Valute": {"AUD": {"CharCode": "AUD", "Value": 51.444, "Nominal": 1, "Name": "Австралийский доллар"}, "MMK": {"CharCode": "MMK", "Value": 34.971, "Nominal": 1000, "Name": "Кьятов"}}}

raw_json -> 'Valute' = {"AUD": {"CharCode": "AUD", "Value": 51.444, "Nominal": 1, "Name": "Австралийский доллар"}, "MMK": {"CharCode": "MMK", "Value": 34.971, "Nominal": 1000, "Name": "Кьятов"}}

jsonb_each(raw_json -> 'Valute')
key | value
---------
"AUD" | {"CharCode": "AUD", "Value": 51.444, "Nominal": 1, "Name": "Австралийский доллар"}
"MMK" | {"CharCode": "MMK", "Value": 34.971, "Nominal": 1000, "Name": "Кьятов"}
*/
WITH source AS (
  SELECT request_date, raw_json
  FROM {{ source ('raw', 'currency_rates_raw') }}
),
parse AS (
  SELECT 
    request_date AS requested_date,
	(source.raw_json ->> 'Date')::DATE AS received_date, --from_response_date,
	valute.key::TEXT AS char_code,
	(valute.value ->> 'Value')::NUMERIC AS value,
	(valute.value ->> 'Nominal')::INTEGER AS nominal,
	valute.value ->> 'Name' AS currency_name
  FROM source
  CROSS JOIN LATERAL  jsonb_each(source.raw_json -> 'Valute') AS valute
),
rate AS (
  SELECT
    requested_date,
	received_date,
	char_code,
	nominal,
	currency_name,
	value,
	ROUND((value / nominal)::NUMERIC, 4) AS rate
  FROM parse
)
SELECT * FROM rate
