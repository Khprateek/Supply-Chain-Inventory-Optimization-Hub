# 📚 Documentation & Technical Specifications

Welcome to the engineering documentation hub for the **Enterprise Open Data Lakehouse Platform**. This repository contains end-to-end specifications covering architecture, business logic, dimensional data models, streaming telemetry proofs, Power BI semantic models, and architectural decision records (ADRs).

---

## 🧭 Documentation Map

### 1. 🏗️ Architecture & System Design
* **[`ARCHITECTURE.md`](../ARCHITECTURE.md)**: Master technical specification of the Lakehouse platform covering Kafka partitioning, PySpark Structured Streaming, Iceberg ACID storage, Nessie catalog, and Trino query engine.
* **[`architecture/architecture_overview.md`](architecture/architecture_overview.md)**: High-level architectural topology and decoupling strategy.
* **[`architecture/data_flow.md`](architecture/data_flow.md)**: End-to-end streaming dataflow from event generator to Trino OLAP.
* **[`architecture/raw_layer_specification.md`](architecture/raw_layer_specification.md)**: Ingestion schema specifications and S3 partitioning scheme.
* **[`architecture/warehouse_performance_and_optimization.md`](architecture/warehouse_performance_and_optimization.md)**: Query acceleration, file compaction, and pruning strategies.

---

### 2. 🔬 Benchmarks & Verification Suite
* **[`BENCHMARK_AND_TESTING_SUITE.md`](BENCHMARK_AND_TESTING_SUITE.md)**: Empirical methodology, mathematical derivations, and execution plan for all 5 pipeline benchmarks (30K ev/s throughput, backpressure stress, storage compression, capacity scaling, and Trino latency).

---

### 3. 📋 Planning & Project Context
* **[`planning/PROJECT_PLAN.md`](planning/PROJECT_PLAN.md)**: Living master project plan, execution checklist, and feature backlog across development phases.
* **[`planning/PROJECT_CONTEXT.md`](planning/PROJECT_CONTEXT.md)**: Original enterprise business domain context, supply chain personas, target volume requirements, and foundational criteria.

---

### 4. 🗄️ Data Model & Schemas
* **[`data-model/dimensional_model_design.md`](data-model/dimensional_model_design.md)**: Kimball star schema design, fact tables grain, and dimension hierarchies.
* **[`data-model/schema_specification.md`](data-model/schema_specification.md)**: Precise table schemas, data types, constraints, and partition keys.
* **[`data-model/data_dictionary.md`](data-model/data_dictionary.md)**: Column-level definitions, business terms, and source-to-target mappings.
* **[`data-model/erd.md`](data-model/erd.md)**: Entity-Relationship diagram across operational and analytical entities.

---

### 5. 💼 Business Requirements & KPIs
* **[`business/project_charter.md`](business/project_charter.md)**: Project scope, business goals, and operational success metrics.
* **[`business/kpi_catalog.md`](business/kpi_catalog.md)**: Complete catalog of supply chain metrics (OTIF, Inventory Turn, GMROI, Stockout Rate).
* **[`business/business_personas.md`](business/business_personas.md)**: User profiles (VP Supply Chain, Inventory Planner, Procurement Manager).
* **[`business/acceptance_criteria.md`](business/acceptance_criteria.md)**: Verification criteria for production readiness.

---

### 6. 📊 Power BI & Semantic Model
* **[`powerbi/semantic_model.md`](powerbi/semantic_model.md)**: Power BI dataset design, table relationships, and DAX measure group structure.
* **[`powerbi/rls_design.md`](powerbi/rls_design.md)**: Row-Level Security roles for regional and warehouse-level access control.
* **[`powerbi/page1_executive_control_tower_spec.md`](powerbi/page1_executive_control_tower_spec.md)**: Control Tower visual specifications.
* **[`powerbi/page4_supplier_performance_spec.md`](powerbi/page4_supplier_performance_spec.md)**: Supplier scorecard visual specification.
* **[`powerbi/page7_what_if_simulation_spec.md`](powerbi/page7_what_if_simulation_spec.md)**: What-if inventory simulation engine.

---

### 7. ⚖️ Architecture Decision Records (ADRs)
* **[`decisions/ADR-001-technology-stack.md`](decisions/ADR-001-technology-stack.md)**: Core technology selection rationale.
* **[`decisions/ADR-002-data-volume-and-storage-strategy.md`](decisions/ADR-002-data-volume-and-storage-strategy.md)**: Object storage strategy and Parquet compression.
* **[`decisions/ADR-004-kimball-dimensional-modeling.md`](decisions/ADR-004-kimball-dimensional-modeling.md)**: Dimensional modeling principles for analytical serving.
* **[`decisions/ADR-007-incremental-processing-strategy.md`](decisions/ADR-007-incremental-processing-strategy.md)**: Micro-batch vs batch transformation strategy.
