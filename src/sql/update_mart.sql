MERGE INTO VT260419031E56__DWH.global_metrics AS target
USING (
    SELECT 
        t.transaction_dt::DATE AS date_update,
        t.currency_code AS currency_from,
        SUM(t.amount * c.currency_with_div) AS amount_total,
        COUNT(*) AS cnt_transactions,
        COUNT(*)::NUMERIC / NULLIF(COUNT(DISTINCT t.account_number_from), 0) AS avg_transactions_per_account,
        COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
    FROM VT260419031E56__STAGING.transactions t
    JOIN VT260419031E56__STAGING.currencies c 
        ON t.currency_code = c.currency_code
        AND c.currency_code_with = 420
        AND c.date_update = t.transaction_dt::DATE
    WHERE t.transaction_dt::DATE = %s
        AND t.status = 'done'
        AND t.account_number_from >= 0
    GROUP BY t.transaction_dt::DATE, t.currency_code
) AS source
ON target.date_update = source.date_update 
   AND target.currency_from = source.currency_from
WHEN MATCHED THEN 
    UPDATE SET 
        amount_total = source.amount_total,
        cnt_transactions = source.cnt_transactions,
        avg_transactions_per_account = source.avg_transactions_per_account,
        cnt_accounts_make_transactions = source.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN 
    INSERT (
        date_update,
        currency_from,
        amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions
    )
    VALUES (
        source.date_update,
        source.currency_from,
        source.amount_total,
        source.cnt_transactions,
        source.avg_transactions_per_account,
        source.cnt_accounts_make_transactions
    );