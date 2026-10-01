{{ config(materialized='view') }}

with raw as (
    select
        raw_payload,
        kafka_timestamp
    from {{ source('raw_supply_chain', 'raw_inventory_events') }}
)

select
    JSON_EXTRACT_SCALAR(raw_payload, '$.event_id') as event_id,
    CAST(JSON_EXTRACT_SCALAR(raw_payload, '$.timestamp') as FLOAT64) as event_timestamp,
    JSON_EXTRACT_SCALAR(raw_payload, '$.product_id') as product_id,
    JSON_EXTRACT_SCALAR(raw_payload, '$.warehouse_id') as warehouse_id,
    CAST(JSON_EXTRACT_SCALAR(raw_payload, '$.quantity_change') as INT64) as quantity_change,
    JSON_EXTRACT_SCALAR(raw_payload, '$.event_type') as event_type,
    kafka_timestamp
from raw
