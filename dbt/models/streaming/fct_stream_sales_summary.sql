{{ config(
    materialized='table',
    partition_by={
      "field": "sale_date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

with sales as (
    select * from {{ ref('stg_stream_sales') }}
)

select
    product_id,
    customer_id,
    DATE(event_timestamp) as sale_date,
    sum(revenue) as total_revenue,
    sum(units_sold) as total_units_sold,
    count(event_id) as total_transactions,
    min(event_timestamp) as first_sale_timestamp,
    max(event_timestamp) as last_sale_timestamp
from sales
group by 1, 2, 3
