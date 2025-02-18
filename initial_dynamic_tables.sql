create database if not exists llm_schema_propagation;
create schema if not exists schema_evolution;
create warehouse if not exists setup_wh;

-- Medallion architecture: Bronze -> Silver -> Gold -> Gold2 -> Platinum
Here, 
-- Silver to Platinum tables are dynamic tables
-- Bronze: Upstream table
-- Rest: Downstream tables

-- Bronze table
create or replace table bronze_fraud_transactions(
    transaction_id VARCHAR,
    user_id VARCHAR,
    full_name VARCHAR,
    email VARCHAR,
    ip_address VARCHAR,
    transaction_amount NUMBER(10,2),
    transaction_date TIMESTAMP_NTZ,
    payment_method VARCHAR,
    transaction_source VARCHAR,
    merchant_id VARCHAR,
    merchant_category VARCHAR,
    city VARCHAR,
    country VARCHAR,
    card_type VARCHAR,
    status VARCHAR
)

-- Insert into bronze table
insert into bronze_fraud_transactions (transaction_id, user_id, full_name, email, ip_address, transaction_amount, transaction_date, payment_method, transaction_source, merchant_id, merchant_category, city, country, card_type, status)
values
    ('TXN101', 'user101', 'Alice Smith', 'alice@example.com', '192.168.1.10', 650.00, '2023-01-01T10:00:00', 'credit_card', 'web', 'M101', 'electronics', 'New York', 'USA', 'Visa', 'APPROVED'),
    ('TXN102', 'user102', 'Bob Johnson', 'bob@example.com', '192.168.1.20', 45.00, '2023-01-02T11:30:00', 'debit_card', 'mobile', 'M102', 'fashion', 'Los Angeles', 'USA', 'MasterCard', 'DECLINED'),
    ('TXN103', 'user103', 'Carol White', 'carol@example.com', '192.168.1.30', 250.00, '2023-01-03T09:15:00', 'online_transfer', 'ATM', 'M103', 'groceries', 'Chicago', 'USA', 'Amex', 'APPROVED');

-- Check inserted data
select * from bronze_fraud_transactions;

-- Silver table
-- This dynamic table cleans and augments the raw data. In addition to standardizing text fields, it derives the transaction hour, day, weekday label, and value category.
create or replace dynamic table silver_fraud_transactions
warehouse = setup_wh
lag = '1 day' -- refresh rate(once per day)
as 
with base as (
    select
        transaction_id,
        user_id,
        full_name,
        email,
        ip_address,
        transaction_amount,
        transaction_date,
        payment_method,
        transaction_source,
        merchant_id,
        merchant_category,
        city,
        country,
        card_type,
        status
    from bronze_fraud_transactions
)
select
    transaction_id,
    user_id,
    full_name,
    email,
    ip_address,
    transaction_amount,
    transaction_date,
    upper(payment_method) as payment_method,
    transaction_source,
    merchant_id,
    merchant_category,
    city,
    country,
    card_type,
    upper(status) as status,
    extract(hour from transaction_date) as transaction_hour,
    cast(transaction_date as date) as transaction_day,
    to_char(transaction_date, 'DY') as day_of_week,
    case 
        when transaction_amount > 500 then 'High Value'
        when transaction_amount between 100 and 500 then 'Medium Value'
        else 'Low Value'
    end as transaction_value_category,
    case 
        when to_char(transaction_date, 'DY') in ('sat','sun') then 'Weekend'
        else 'Weekday'
    end as day_type
from base;

-- Gold table
-- At this stage, we apply business logic to classify each transaction into a risk category. We also add a descriptive reason for the classification.
create or replace dynamic table gold_fraud_insights 
warehouse = setup_wh
lag = '1 day'
as 
with base as (
    SELECT
        transaction_id,
        user_id,
        full_name,
        email,
        ip_address,
        transaction_amount,
        transaction_date,
        payment_method,
        transaction_source,
        merchant_id,
        merchant_category,
        city,
        country,
        card_type,
        status,
        transaction_hour,
        transaction_value_category,
        day_type
    FROM silver_fraud_transactions
)
select 
    transaction_id,
    user_id,
    full_name,
    email,
    ip_address,
    transaction_amount,
    transaction_date,
    payment_method,
    transaction_source,
    merchant_id,
    merchant_category,
    city,
    country,
    card_type,
    status,
    transaction_hour,
    transaction_value_category,
    day_type,
    case 
        when status = 'DECLINED' then 'High Risk'
        when payment_method = 'CREDIT_CARD' and transaction_value_category = 'High Value' then 'Medium Risk'
        else 'Low Risk'
    end as fraud_risk_level,
    case
        when status = 'DECLINED' then 'Transaction declined by issuer'
        when payment_method = 'CREDIT_CARD' and transaction_value_category = 'High Value' then 'High amount on credit card'
        else 'Normal'
    end as risk_reason
from base;

-- Gold2 table
-- In this layer, we convert qualitative risk levels into numeric scores and add a weighted risk metric that also factors in the transaction amount.
create or replace dynamic table gold_fraud_insights_v2
warehouse = setup_wh
lag = '1 day'
as 
with base as (
    select 
        transaction_id,
        user_id,
        full_name,
        email,
        ip_address,
        transaction_amount,
        transaction_date,
        payment_method,
        transaction_source,
        merchant_id,
        merchant_category,
        city,
        country,
        card_type,
        status,
        transaction_hour,
        transaction_value_category,
        day_type,
        fraud_risk_level,
        risk_reason
    from gold_fraud_insights
)
select 
    transaction_id,
    user_id,
    full_name,
    email,
    ip_address,
    transaction_amount,
    transaction_date,
    payment_method,
    transaction_source,
    merchant_id,
    merchant_category,
    city,
    country,
    card_type,
    status,
    transaction_hour,
    transaction_value_category,
    day_type,
    fraud_risk_level,
    risk_reason,
    case 
        when fraud_risk_level = 'High Risk' then 0.9
        when fraud_risk_level = 'Medium Risk' then 0.6
        else 0.3
    end as fraud_risk_score,
    -- A weighted risk metric that factors in the transaction amount
    case 
        when fraud_risk_level = 'High Risk' then round(transaction_amount * 0.001 * 0.9, 2)
        when fraud_risk_level = 'Medium Risk' then round(transaction_amount * 0.001 * 0.6, 2)
        else round(transaction_amount * 0.001 * 0.3, 2)
    end as weighted_risk
from base;

-- Platinum table
-- This final downstream table aggregates data by day and merchant category. We compute totals, averages, and advanced metrics like standard deviation, percentage of high‐risk transactions, and a day-level risk ranking using window functions.
create or replace dynamic table platinum_fraud_insights
warehouse = setup_wh
lag = '1 day'
as 
with base as (
    select
        cast(transaction_date as date) as transaction_day,
        merchant_category,
        fraud_risk_score,
        weighted_risk,
        transaction_amount,
        fraud_risk_level
    from gold_fraud_insights_v2
),
aggregated as (
    select
        transaction_day,
        merchant_category,
        count(*) as total_transactions,
        avg(fraud_risk_score) as avg_fraud_risk_score,
        stddev(fraud_risk_score) as std_fraud_risk_score,
        avg(weighted_risk) as avg_weighted_risk,
        count(case when fraud_risk_level = 'High Risk' then 1 else 0 end) as count_high_risk
    from base
    group by transaction_day, merchant_category
),
advanced as (
    select
        transaction_day,
        merchant_category,
        total_transactions,
        avg_fraud_risk_score,
        avg_weighted_risk,
        count_high_risk,
        std_fraud_risk_score,
        round((count_high_risk / total_transactions)*100, 2) as percent_high_risk,
        -- Rank merchant categories by average risk score for each day
        rank() over(partition by transaction_day order by avg_fraud_risk_score) as risk_rank_by_day,
        -- moving average risk score over the past 3 days
        avg(avg_fraud_risk_score) over(
            partition by merchant_category 
            order by transaction_day 
            rows between 2 preceding and current row) 
        as moving_avg_risk_score
    from aggregated
)
select * from advanced;
