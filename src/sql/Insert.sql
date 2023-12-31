insert into FARRUHRUSYANDEXRU__DWH.global_metrics
with t0 as (
    select * from
        FARRUHRUSYANDEXRU__STAGING.currencies c
    where 1=1
        and currency_code_with = 420 -- USD
        and date_update = 'snap_date'::date-1
    ),
t1 as (
    select
        t0.date_update,
        t.currency_code as currency_from,
        t.account_number_from,
        (t.amount * t0.currency_with_div) as amount
    from FARRUHRUSYANDEXRU__STAGING.transactions t
    join t0 on t.transaction_dt::date = t0.date_update
        and t.currency_code = t0.currency_code
    where 1=1
        and t.status = 'done' and t.account_number_from>0
        and t.transaction_dt::date = 'snap_date'::date-1
    union all
    select
        transaction_dt::date as date_update,
        currency_code as currency_from,
        account_number_from,
        amount
    from FARRUHRUSYANDEXRU__STAGING.transactions
    where 1=1
        and currency_code = 420 -- USD
        and status = 'done'
        and account_number_from>0
        and transaction_dt::date = 'snap_date'::date-1
)
select
    date_update,
    currency_from,
    sum(amount) as amount_total,
    count(*) as cnt_transactions,
    round(sum(amount) / count(distinct account_number_from), 2) as avg_transactions_per_account,
    count(distinct account_number_from) as cnt_accounts_make_transactions
from t1
group by date_update,currency_from;