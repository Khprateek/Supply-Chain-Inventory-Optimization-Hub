-- ============================================================================
-- Enterprise Open Data Lakehouse — Trino Analytical & Metadata SQL Reference
-- Engine: Trino 440+ over Apache Iceberg v2 & Project Nessie
-- ============================================================================

-- 1. Verify Active Namespaces & Catalogs
SHOW SCHEMAS FROM iceberg;

-- 2. Inspect Iceberg Raw Streaming Ingestion Tables
SELECT 
    product_id,
    COUNT(*) as total_orders,
    SUM(units_sold) as total_units,
    ROUND(SUM(revenue), 2) as gross_revenue,
    MIN(from_unixtime(timestamp)) as earliest_order,
    MAX(from_unixtime(timestamp)) as latest_order
FROM iceberg.sales.raw_events
GROUP BY product_id
ORDER BY gross_revenue DESC
LIMIT 10;

-- 3. Query Inventory Real-Time Movement Telemetry
SELECT 
    warehouse_id,
    event_type,
    COUNT(*) as movement_events,
    SUM(quantity_change) as net_stock_delta
FROM iceberg.inventory.raw_events
GROUP BY warehouse_id, event_type
ORDER BY warehouse_id, event_type;

-- 4. Query Gold-Tier Analytical Aggregated Mart (Sales Summary)
SELECT 
    event_date,
    product_id,
    total_units_sold,
    total_revenue,
    transaction_count,
    ROUND(avg_order_value, 2) as avg_order_value
FROM iceberg.marts.fact_sales_summary
ORDER BY event_date DESC, total_revenue DESC
LIMIT 20;

-- 4b. Query Gold-Tier Supply Chain Optimization Mart (Inventory Health & Stockout Alerts)
SELECT 
    snapshot_date,
    warehouse_id,
    product_id,
    current_on_hand,
    total_receipts,
    total_picks,
    daily_turnover_rate,
    days_of_supply,
    stockout_risk_status
FROM iceberg.marts.fact_inventory_health
WHERE stockout_risk_status IN ('CRITICAL_STOCKOUT', 'LOW_STOCK_WARNING')
ORDER BY days_of_supply ASC
LIMIT 20;

-- 4c. Inspect Streaming Dead-Letter Queue (DLQ / Quarantine Audits)
SELECT 
    quarantined_at,
    topic,
    partition,
    offset,
    error_reason,
    raw_payload
FROM iceberg.sales.dlq_events
ORDER BY quarantined_at DESC
LIMIT 10;

-- ============================================================================
-- Advanced Lakehouse Operations: Metadata & Time-Travel
-- ============================================================================

-- 5. Inspect Iceberg Snapshot History (Git-like commits tracked by Nessie)
SELECT 
    committed_at,
    snapshot_id,
    parent_id,
    operation,
    summary['total-records'] as total_records,
    summary['added-data-files'] as added_files
FROM iceberg.sales."raw_events$snapshots"
ORDER BY committed_at DESC;

-- 6. Inspect Physical Parquet File Layout & Partition Pruning
SELECT 
    file_path,
    file_format,
    record_count,
    file_size_in_bytes,
    column_sizes
FROM iceberg.sales."raw_events$files"
LIMIT 10;

-- 7. Iceberg Time-Travel Query (Query state as of specific snapshot)
-- Replace <SNAPSHOT_ID> with a snapshot_id from step 5:
-- SELECT * FROM iceberg.sales.raw_events FOR VERSION AS OF <SNAPSHOT_ID> LIMIT 10;
