# Master Project Plan

*This is a living document. As phases are completed, they will be checked off or archived. New ideas and requirements will be added to the Backlog.*

---

## ✅ Completed Phases
- [x] **Phase 0: Project Cleanup & Reset** - Removed old, broken batch/polars jobs and fixed Windows path issues for dbt.
- [x] **Phase 1: Kafka Event Generator** - Built a high-throughput Python script publishing continuous JSON events to `sales_events` and `inventory_events` topics.
- [x] **Phase 2: Architecture A (BigQuery & dbt)** - Implemented PySpark `foreachBatch` ingestion (bypassing JVM bugs) and dbt staging/mart transformations.
- [x] **Phase 3: Architecture B (Iceberg & Trino)** - Implemented PySpark streaming to Apache Iceberg via Project Nessie and MinIO S3 storage.
- [x] **Phase 4: Architecture Reconciliation** - Built `compare_architectures.py` utilizing Trino for dynamic Iceberg aggregations to safely bypass Java 11 HotSpot compiler crashes.
- [x] **Phase 5: Architecture Documentation** - Documented the final, stable dual-architecture flow in `ARCHITECTURE.md`.

---

## 🚧 Current Phase: Phase 6 - Interactive Web Dashboard
**Goal:** Build a unified, browser-based Control Center to visualize the data flow, track live ingestion speeds, and control the pipeline via buttons rather than terminal commands.

### Sub-tasks:
- [x] **6.1: Backend Process Manager** 
  - Create a FastAPI backend to programmatically start/stop the Kafka generator and PySpark streaming jobs via Python `subprocess`.
- [x] **6.2: Live Metrics API**
  - Create FastAPI endpoints that query BigQuery and Trino in real-time to track total row counts and calculate ingestion speed (events/sec).
- [x] **6.3: Frontend UI & Flow Diagram**
  - Build a dark-mode frontend (HTML/JS + Tailwind).
  - Embed a dynamic Mermaid.js architecture diagram.
  - Add control buttons (Start Generator, Start Streams, Run dbt).
- [x] **6.4: Real-Time Data Binding**
  - Use JavaScript `fetch()` polling to update the UI counters, speeds, and the Final Comparison Matrix dynamically without refreshing the page.

---

## 🔮 Future Ideas / Backlog
*(Ideas to explore after the dashboard is complete)*
- [ ] **Containerize the Generator**: Move the Python data generator into its own Docker container so the entire stack is 100% Dockerized.
- [ ] **BI Integration**: Deploy Apache Superset or Metabase in Docker and connect it to both BigQuery and Trino for visual charting.
- [ ] **Data Quality**: Add dbt tests for Architecture A, and implement Great Expectations for Architecture B.
- [ ] **Deployment**: Create cloud-ready deployment scripts (e.g., Terraform) to spin this up on AWS or GCP.
