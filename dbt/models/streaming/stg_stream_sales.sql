{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_supply_chain', 'raw_sales_events') }}
)

SELECT
    event_id,
    TIMESTAMP_MILLIS(CAST(timestamp * 1000 AS INT64)) as event_time,
    product_id,
    customer_id,
    revenue,
    units_sold,
    kafka_timestamp
FROM source
