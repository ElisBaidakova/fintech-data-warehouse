-- Проверка транзакций
SELECT transaction_dt::DATE AS dt, COUNT(*) AS cnt 
FROM VT260419031E56__STAGING.transactions
GROUP BY transaction_dt::DATE 
ORDER BY transaction_dt::DATE;


SELECT transaction_dt::DATE AS dt, COUNT(*) AS cnt 
FROM VT260419031E56__STAGING.transactions
WHERE status = 'done'
GROUP BY transaction_dt::DATE 
ORDER BY transaction_dt::DATE;


-- Проверка справочника валют
SELECT date_update, COUNT(*) AS cnt 
FROM VT260419031E56__STAGING.currencies 
GROUP BY date_update
ORDER BY date_update;

-- Проверка витрины
SELECT COUNT(*) FROM VT260419031E56__DWH.global_metrics;

SELECT * FROM VT260419031E56__DWH.global_metrics;