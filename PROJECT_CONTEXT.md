# ENTERPRISE SUPPLY CHAIN & INVENTORY OPTIMIZATION HUB

## 1. Project Objective

Build an industry-grade Enterprise Supply Chain & Inventory Optimization Hub for a fictional multinational retail/FMCG organization.

The system should help supply-chain planners and operations executives answer:

* What inventory do we currently have?
* Where is inventory overstocked?
* Where are stockouts occurring?
* Which SKUs are creating the most working-capital exposure?
* Which products have poor forecast accuracy?
* Which suppliers are causing lead-time problems?
* What should the recommended safety stock be?
* When should a replenishment order be triggered?
* Which SKUs require immediate planner attention?
* How does inventory performance vary by region, warehouse, category, supplier and time?
* What happens to inventory and working capital if demand, lead time or service-level assumptions change?

---

# 2. Business Domain

Use a realistic enterprise retail/FMCG supply-chain scenario.

The organization has:

* Multiple regions
* Multiple distribution centers / warehouses
* Multiple suppliers
* Thousands of SKUs
* Product categories and subcategories
* Customers / demand channels
* Purchase orders
* Purchase-order lines
* Sales orders
* Sales-order lines
* Inventory snapshots
* Inventory movements
* Forecasts
* Supplier lead times
* Supplier performance
* Stockout events
* Returns
* Calendar and fiscal periods

The dataset must be large enough to demonstrate enterprise analytics.

Target analytical volume:

* Minimum: 5 million fact rows
* Preferred: 10M+ rows across major fact tables
* Dimensions should remain realistically sized

Do NOT create meaningless random rows merely to reach 5M.

The data must have realistic relationships, distributions, seasonality, regional differences and operational patterns.

---

# 3. Core Technology Stack

Data Lakehouse & Streaming Architecture:

* Message Broker: Apache Kafka
* Distributed Processing: Apache Spark / PySpark Structured Streaming & Batch
* Lakehouse Table Format: Apache Iceberg v2 (ACID transactions, hidden partitioning)
* Catalog & Git-like Metadata: Project Nessie (transactional catalog, zero-copy branching)
* Cloud Object Storage: MinIO / S3 (Snappy/ZSTD compressed Parquet)
* Distributed SQL Query Engine: Trino (sub-second interactive OLAP)
* Operations & Control Tower: FastAPI, Uvicorn, Tailwind CSS

Primary BI:

* Microsoft Power BI Desktop
* Trino Direct / ODBC / REST connector
* DAX & Semantic Models

Version control:

* Git
* GitHub

Development environment:

* Windows
* VS Code / Antigravity
* Docker & Docker Compose

---

# 4. Required Architecture

Use a layered Lakehouse architecture:

STREAMING SOURCE (Python Event Engine)
↓
MESSAGE BROKER (Apache Kafka)
↓
DISTRIBUTED PROCESSING (PySpark Structured Streaming)
↓
LAKEHOUSE STORAGE & CATALOG (Apache Iceberg v2 + Project Nessie on S3)
↓
DIMENSIONAL TRANSFORMATION & MAINTENANCE (PySpark Batch MERGE INTO + Iceberg Compaction)
↓
DISTRIBUTED SERVING (Trino Distributed SQL Engine)
↓
CONTROL TOWER & ANALYTICS (FastAPI Dashboard / Power BI)

The analytical warehouse must follow a disciplined Kimball dimensional-modeling approach.

---

# 5. Required Dimensional Model

At minimum consider these dimensions:

* DimDate
* DimProduct
* DimSupplier
* DimWarehouse
* DimRegion
* DimCustomer / Channel
* DimEmployee / Planner
* DimScenario where appropriate

Fact tables should be designed around clear business processes.

Potential fact tables:

* FactSales
* FactInventorySnapshot
* FactPurchaseOrder
* FactForecast
* FactInventoryMovement
* FactStockout
* FactReturns

Do NOT automatically create every possible fact table.

For every fact table define:

* Business process
* Grain
* Primary/business key
* Foreign keys
* Measures
* Additive/semi-additive/non-additive behavior
* Expected row volume
* Refresh strategy

---

# 6. Advanced Dimensional Modeling Requirements

Where justified, implement:

### Role-playing Date Dimensions

Examples:

* Order Date
* Ship Date
* Delivery Date
* Expected Delivery Date
* Forecast Date

The implementation must avoid ambiguous relationships.

### Slowly Changing Dimensions

At least one dimension should demonstrate SCD Type 2.

Preferred candidate:

DimProduct or DimSupplier.

Document:

* Natural key
* Surrogate key
* Effective date
* Expiry date
* Current flag

### Bridging Tables

Use a bridge only where the business relationship genuinely requires many-to-many modeling.

Do NOT introduce bridge tables simply to make the project look advanced.

---

# 7. Supply Chain KPIs

The solution should eventually support:

## Inventory

* On-hand inventory
* Inventory value
* Average inventory
* Days of inventory
* Inventory turns
* Stockout rate
* Excess inventory
* Slow-moving inventory
* Dead stock

## Service

* Fill rate
* Service level
* OTIF
* Stockout frequency
* Backorder rate

## Forecast

* Forecast accuracy
* Forecast bias
* WAPE
* MAPE where statistically appropriate
* Forecast error
* SKU-level forecast variance

## Procurement

* Supplier lead time
* Lead-time variability
* Supplier OTIF
* Purchase-order cycle time
* Supplier performance

## Working Capital

* Inventory investment
* Excess working capital
* Inventory carrying cost
* Working-capital exposure

---

# 8. Advanced Analytics

Eventually implement:

### ABC Classification

Classify products based on contribution to annual consumption value/revenue.

### XYZ Classification

Classify products based on demand variability.

### ABC-XYZ Matrix

Examples:

* AX
* AY
* AZ
* BX
* BY
* BZ
* CX
* CY
* CZ

### Dynamic Safety Stock

Safety stock should be influenced by:

* Demand variability
* Lead-time variability
* Service level / Z-score
* Replenishment lead time
* Review period where appropriate

### Reorder Point

Consider:

* Expected demand during lead time
* Safety stock

### Inventory Turns

Calculate using appropriate average inventory methodology.

### What-If Analysis

Allow users to simulate:

* Service-level target
* Demand growth
* Lead-time change
* Safety-stock multiplier
* Carrying-cost assumption

The simulation must produce business-relevant outputs.

---

# 9. DAX Requirements

Use advanced DAX where it genuinely improves the model.

Potential techniques:

* CALCULATE
* FILTER
* REMOVEFILTERS
* ALLSELECTED
* DIVIDE
* SUMX
* AVERAGEX
* VALUES
* SELECTEDVALUE
* SWITCH
* VAR
* RANKX
* WINDOW
* INDEX
* OFFSET

Do NOT use advanced functions simply for complexity.

Every important measure must have:

1. Business definition
2. Mathematical logic
3. DAX
4. Explanation
5. Validation example

---

# 10. Power BI Requirements

The semantic model should demonstrate:

* Star schema
* Explicit measures
* Proper date table
* Correct filter direction
* Minimal bi-directional relationships
* Hidden technical columns
* Business-friendly naming
* Display folders
* Measure organization
* Calculation logic
* Performance-conscious modeling

Do NOT use a giant flat table as the final semantic model.

---

# 11. DirectQuery / Incremental Refresh

The project should demonstrate enterprise-scale Power BI concepts.

Target:

* 5M+ fact rows
* Large fact tables
* DirectQuery or composite-model architecture where appropriate
* Incremental refresh for large historical tables where supported
* Query folding
* Partition-aware filtering
* Aggregation strategy where useful

Do not claim a feature is implemented unless it has actually been configured and validated.

If a feature requires Power BI Service, premium capacity, Fabric, or another paid capability, explicitly identify the requirement.

---

# 12. Row-Level Security

Implement dynamic RLS.

Example hierarchy:

VP of Operations
→ sees all regions

Regional Supply Planner
→ sees assigned region(s)

Warehouse Planner
→ sees assigned warehouse(s)

Use a security mapping table such as:

SecurityUser

* UserEmail
* Role
* RegionKey
* WarehouseKey

The RLS design should be scalable rather than hardcoding individual users into DAX.

---

# 13. Data Quality

The system must include data-quality validation.

Examples:

* Referential integrity
* Duplicate business keys
* Missing dimension keys
* Invalid dates
* Negative inventory where invalid
* Invalid quantities
* Impossible lead times
* Missing supplier relationships
* Forecast anomalies
* Duplicate transactions

Use automated tests where possible.

---

# 14. Performance Engineering

The project must explicitly address:

* Fact-table size
* Cardinality
* Column selection
* Storage mode
* Query folding
* Aggregations
* DAX efficiency
* Relationship design
* Filter propagation
* Incremental refresh
* Visual count
* Report page performance

Do not optimize blindly.

Measure performance before and after optimization.

---

# 15. Dashboard Structure

The final Power BI report should have approximately:

### Page 1 — Executive Overview

* Inventory value
* Inventory turns
* Service level
* Stockout rate
* Forecast accuracy
* Working-capital exposure
* Critical alerts

### Page 2 — Inventory Health

* Inventory by region
* Inventory by warehouse
* Excess inventory
* Slow movers
* Days of inventory
* Aging

### Page 3 — SKU Optimization

* ABC/XYZ segmentation
* SKU ranking
* Demand variability
* Forecast error
* Safety stock
* Reorder point

### Page 4 — Supply & Procurement

* Supplier performance
* Lead time
* PO status
* OTIF
* Supplier risk

### Page 5 — Forecast Performance

* Forecast vs actual
* Forecast accuracy
* Bias
* Error trends
* Category/SKU analysis

### Page 6 — Working Capital

* Inventory investment
* Carrying cost
* Excess stock
* Potential working-capital release

### Page 7 — What-If Simulation

Parameters:

* Demand growth
* Lead-time change
* Service-level target
* Safety-stock multiplier
* Carrying-cost rate

### Page 8 — Planner Workbench

Action-oriented table showing:

* SKU
* Warehouse
* Current stock
* Demand
* Safety stock
* Reorder point
* Days of inventory
* Forecast error
* Supplier lead time
* Risk classification
* Recommended action

### Page 9 — Data / Pipeline Health

* Last refresh
* Row counts
* Data-quality checks
* Failed records
* Warehouse freshness
* Pipeline status

---

# 16. Industry-Grade Standards

Do not fabricate enterprise claims.

Clearly distinguish between:

* Implemented
* Simulated
* Demonstrated
* Conceptual

The final project must be technically defensible in an interview.

Every architectural decision must have a reason.

Every complex DAX measure must have a business explanation.

Every dataset assumption must be documented.

Every major component must be testable.

---

# 17. Development Rules

IMPORTANT:

Do NOT build the entire project in one response.

Work in controlled phases.

At the end of each phase:

1. Summarize what was completed.
2. List files created/modified.
3. Explain dependencies.
4. Identify unresolved decisions.
5. Provide validation checks.
6. STOP.

Do not automatically continue to the next phase.

Never modify unrelated files.

Never rewrite the entire project when only one component needs modification.

Prefer incremental changes.

Before creating code, inspect the existing project structure.

Before changing architecture, explain why.

Before implementing advanced functionality, establish the simpler working version first.

---

# 18. Quality Gate

A phase is complete only when:

* Its outputs exist
* The outputs are internally consistent
* Validation checks pass
* Dependencies are documented
* No major unresolved issue is hidden

If something cannot be implemented because of environment/licensing/tool limitations:

* Do not fake it.
* Explain the limitation.
* Provide the closest valid implementation.
* Clearly label what remains conceptual.

---

# 19. AI Working Style

Act as a senior:

* Data Architect
* Analytics Engineer
* Supply Chain Analytics Consultant
* Power BI Architect

But teach implementation at a practical level.

Do not assume the developer already knows every command.

When giving commands:

* State where to execute them.
* State expected output.
* Explain what the command does briefly.
* Do not combine 20 unrelated commands into one step.

When creating files:

* Give exact paths.
* Keep files modular.
* Avoid unnecessary complexity.

Never generate placeholder code that is presented as production-ready.

---

# 20. Primary Objective

The final result should look like a realistic enterprise supply-chain analytics platform that could be discussed in an interview with:

* Senior Data Analyst
* Analytics Engineer
* BI Developer
* Data Engineer
* Supply Chain Analytics Manager
* Power BI Developer

The project should demonstrate genuine understanding of:

Business → Data Model → Warehouse → Transformation → Semantic Model → DAX → Security → Performance → Decision Support.

Do not optimize for number of technologies.

Optimize for correctness, architecture, business value, maintainability and interview defensibility.
