DROP TABLE IF EXISTS VT260419031E56__STAGING.transactions;
CREATE TABLE IF NOT EXISTS VT260419031E56__STAGING.transactions(
operation_id VARCHAR(50) NOT NULL,
account_number_from BIGINT NOT NULL,
account_number_to BIGINT NOT NULL,
currency_code BIGINT NOT NULL,
country VARCHAR(100) NOT NULL,
status VARCHAR(20) NOT NULL,
transaction_type VARCHAR(30) NOT NULL,
amount BIGINT NOT NULL,
transaction_dt TIMESTAMP(3) NOT NULL
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES
PARTITION BY (date_trunc('day', transaction_dt));


DROP TABLE IF EXISTS VT260419031E56__STAGING.currencies;
CREATE TABLE IF NOT EXISTS VT260419031E56__STAGING.currencies(
currency_code INT NOT NULL,
currency_code_with INT NOT NULL,
date_update DATE NOT NULL,
currency_with_div NUMERIC(15, 6) NOT NULL
)
ORDER BY date_update, currency_code
SEGMENTED BY HASH(date_update, currency_code) ALL NODES
PARTITION BY (date_update);


DROP TABLE IF EXISTS VT260419031E56__DWH.global_metrics CASCADE;
CREATE TABLE IF NOT EXISTS VT260419031E56__DWH.global_metrics(
date_update DATE NOT NULL,
currency_from INT NOT NULL,
amount_total NUMERIC(38, 6) NOT NULL,
cnt_transactions INT NOT NULL,
avg_transactions_per_account NUMERIC(10, 2) NOT NULL,
cnt_accounts_make_transactions INT NOT NULL,
CONSTRAINT uk_global_metrics_date_currency UNIQUE (date_update, currency_from)
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;
