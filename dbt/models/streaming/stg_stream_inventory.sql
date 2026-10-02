{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_supply_chain', 'raw_inventory_events') }}
)

SELECT
    event_id,
    TIMESTAMP_MILLIS(CAST(timestamp * 1000 AS INT64)) as event_time,
    product_id,
    warehouse_id,
    quantity_change,
    event_type,
    kafka_timestamp
FROM source
