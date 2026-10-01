{{ config(materialized='view') }}

with raw as (
    select
        raw_payload,
        kafka_timestamp
    from {{ source('raw_supply_chain', 'raw_sales_events') }}
)

select
    JSON_EXTRACT_SCALAR(raw_payload, '$.event_id') as event_id,
    CAST(JSON_EXTRACT_SCALAR(raw_payload, '$.timestamp') as FLOAT64) as event_timestamp,
    JSON_EXTRACT_SCALAR(raw_payload, '$.product_id') as product_id,
    JSON_EXTRACT_SCALAR(raw_payload, '$.customer_id') as customer_id,
    CAST(JSON_EXTRACT_SCALAR(raw_payload, '$.revenue') as FLOAT64) as revenue,
    CAST(JSON_EXTRACT_SCALAR(raw_payload, '$.units_sold') as INT64) as units_sold,
    kafka_timestamp
from raw
