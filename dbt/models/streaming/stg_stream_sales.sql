{{ config(materialized='view') }}

with parsed as (
    select
        PARSE_JSON(raw_payload) as payload,
        kafka_timestamp
    from {{ source('raw_supply_chain', 'raw_sales_events') }}
),
raw as (
    select
        payload,
        kafka_timestamp,
        row_number() over(
            partition by STRING(payload.event_id) 
            order by kafka_timestamp desc
        ) as rn
    from parsed
),
deduped as (
    select * from raw where rn = 1
)

select
    STRING(payload.event_id) as event_id,
    TIMESTAMP_SECONDS(CAST(STRING(payload.timestamp) as INT64)) as event_timestamp,
    STRING(payload.product_id) as product_id,
    STRING(payload.customer_id) as customer_id,
    CAST(STRING(payload.revenue) as FLOAT64) as revenue,
    CAST(STRING(payload.units_sold) as INT64) as units_sold,
    kafka_timestamp
from deduped
