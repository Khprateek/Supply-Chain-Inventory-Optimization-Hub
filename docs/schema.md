# Backend Schema & Data Model
## Apache Iceberg: `nessie.inventory.streaming_events`

### Overview
This table stores the continuous stream of raw supply chain and inventory lifecycle events. It is designed as an append-only append log in Apache Iceberg format.

### Schema Definition
| Column Name       | Data Type   | Description                                                                 | Example                       |
|:------------------|:------------|:----------------------------------------------------------------------------|:------------------------------|
| `event_id`        | STRING      | Unique UUID for the specific event, used for deduplication.                 | `e28b8a05-1a2b-4c3d...`       |
| `timestamp`       | DOUBLE      | Epoch timestamp (in seconds) when the event occurred at the edge.           | `1695744231.452`              |
| `product_id`      | STRING      | The unique identifier for the SKU / Product.                                | `SKU-10294`                   |
| `warehouse_id`    | STRING      | The unique identifier for the Warehouse, Fulfillment Center, or Store.      | `WH-US-EAST-1`                |
| `quantity_change` | BIGINT      | The change in inventory count (positive for restock, negative for sales).   | `-5`                          |
| `event_type`      | STRING      | Categorical descriptor of the event (e.g., `SALE`, `RESTOCK`, `RETURN`).    | `SALE`                        |
| `ingested_at`     | TIMESTAMP   | Auto-generated timestamp when Spark processed the event.                    | `2026-09-26T14:31:05.000Z`    |

### Iceberg Partitioning Strategy
The Iceberg table is horizontally partitioned using a hidden partition transform on the `ingested_at` column:
* **Partition Scheme:** `days(ingested_at)`
* **Benefit:** Queries filtering by recent dates will automatically skip scanning older Parquet files (Partition Pruning), massive accelerating query performance without requiring users to manually specify physical partition columns.

### Downstream Views (Trino)
In the Trino serving layer, this raw table will be aggregated dynamically to produce real-time materialized-style views for end users:

**Current Inventory State (Logical View):**
```sql
SELECT 
    product_id, 
    warehouse_id, 
    SUM(quantity_change) AS current_stock_level
FROM nessie.inventory.streaming_events
GROUP BY product_id, warehouse_id;
```
