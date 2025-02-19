-- create a baseline table to store the current schema definition
create or replace table schema_baseline(
    table_name string,
    column_name string,
    data_type string
);

-- create a log tab;e to record any detected schema changes
create or replace table schema_change_log(
    change_detected_at timestamp,
    table_name string,
    column_name string,
    data_type string,
    change_type string
);

-- create the task that monitors schema changes
create or replace task schema_change_monitor
warehouse = setup_wh
schedule = 'using cron * * * * * UTC' -- Run every minute
as 
begin
    -- log new columns added to the key tables
    insert into schema_change_log (change_detected_at, table_name, column_name, data_type, change_type)
    select 
        current_timestamp as change_detected_at,
        table_name,
        column_name,
        data_type,
        'Added' as change_type
    from information_schema.columns
    where table_schema = 'SCHEMA_EVOLUTION'
        and table_catalog = 'CORTEX_LLM_ETL'
        and table_name in (
            'BRONZE_FRAUD_TRANSACTIONS',
            'SILVER_FRAUD_TRANSACTIONS',
            'GOLD_FRAUD_INSIGHTS',
            'GOLD_FRAUD_INSIGHTS_V2',
            'PLATINUM_FRAUD_INSIGHTS'
        )
        and (table_name, column_name) not in (select table_name, column_name from schema_baseline)
    order by table_name;

    -- log modifications when a column's data type changes
    insert into schema_change_log (change_detected_at, table_name, column_name, data_type, change_type)
    select
        current_timestamp,
        c.table_name,
        c.column_name,
        c.data_type,
        'Modified' as change_type
    from information_schema.columns c
    join schema_baseline b 
        on c.table_name = b.table_name and c.column_name = b.column_name
    where c.table_schema = 'SCHEMA_EVOLUTION'
        and c.table_catalog = 'CORTEX_LLM_ETL'
        and c.data_type <> b.data_type
        and c.table_name in (
            'BRONZE_FRAUD_TRANSACTIONS',
            'SILVER_FRAUD_TRANSACTIONS',
            'GOLD_FRAUD_INSIGHTS',
            'GOLD_FRAUD_INSIGHTS_V2',
            'PLATINUM_FRAUD_INSIGHTS'
        )
    order by c.table_name;

    -- Update the baseline table
        -- - if a column exists and its data type has changed, update it
        -- - if a column does not exist in the baseline, add it
    merge into schema_baseline as b        
    using (
        select table_name, column_name, data_type
        from information_schema.columns
        where table_schema = 'SCHEMA_EVOLUTION'
            and table_catalog = 'CORTEX_LLM_ETL'
            and table_name in (
                'BRONZE_FRAUD_TRANSACTIONS',
                'SILVER_FRAUD_TRANSACTIONS',
                'GOLD_FRAUD_INSIGHTS',
                'GOLD_FRAUD_INSIGHTS_V2',
                'PLATINUM_FRAUD_INSIGHTS'
            )
    ) as c
    on b.table_name = c.table_name and b.column_name = c.column_name
    when matched and b.data_type <> c.data_type then
        update set data_type = c.data_type
    when not matched then
        insert (table_name, column_name, data_type) values (c.table_name, c.column_name, c.data_type);
end;

-- resume the task to start monitoring 
alter task schema_change_monitor resume;
