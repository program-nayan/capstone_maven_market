# Maven Market Enterprise Lakehouse Blueprint

## 1. System Architecture Diagram

```mermaid
flowchart TD
    subgraph Data_Sources["Data Sources (Batch, Real-Time, NoSQL)"]
        S1["POS CSV Data\n(ADLS Gen2 Volume)"]
        S2["Confluent Kafka\n(orders-stream)"]
        S3["MongoDB Atlas\n(product_catalog)"]
    end

    subgraph Governance_Security["Unity Catalog & Security Governance (POC 3)"]
        UC["Unity Catalog Metastore\n(maven_market_uc)"]
        TAGS["Governed Tag Taxonomy\n(PII, Financial, EU/NA)"]
        RLS["Row-Level Security (RLS)\n(audit.user_region_mapping)"]
        CLS["Column-Level Masking (CLS)\n(Tag-Based Dynamic Masking)"]
        UC --> TAGS
        TAGS --> RLS
        TAGS --> CLS
    end

    subgraph Medallion_Pipeline["Delta Live Tables Medallion Architecture"]
        B["Bronze Layer\n(Auto Loader & Kafka Streams)"]
        S["Silver Layer\n(Quality Expectations & SCD Type 2)"]
        G["Gold Layer\n(Star Schema Dimensional Model)"]
        B -->|Cleanse & Enforce Schema| S
        S -->|Aggregate & Model Facts/Dims| G
    end

    subgraph Observability["Telemetry & Audit Framework"]
        CAPTURE["src/telemetry/run_audit.py\n(DLT event-log capture)"]
        LOG["src/telemetry/logger.py\n(Central structured logger)"]
        TBL["maven_market_uc.audit.audit_logs"]
        CAPTURE --> LOG
        LOG -->|Append run and event metadata| TBL
    end

    subgraph Consumption["Analytics & SQL Dashboards"]
        D1["1. Executive Overview"]
        D2["2. Real-Time Ops"]
        D3["3. Regional Sales (RLS/CLS Enforced)"]
        D4["4. Platform Health (Audit Telemetry)"]
        LF["Databricks Lakeflow Designer\n(Genie AI Data Prep)"]
    end

    Data_Sources --> Medallion_Pipeline
    Medallion_Pipeline <--> Governance_Security
    Medallion_Pipeline -.-> Observability
    Gold Layer --> Consumption
    Lakeflow Designer --> Gold Layer
```

The daily workflow runs `run_telemetry_audit` after the DLT task with `ALL_DONE`,
so DLT events are captured even when the pipeline task fails. The task receives
the deployed pipeline ID, catalog, and environment from the bundle. The logger
creates the audit schema and Delta table when needed; audit-write failures are
raised so the task cannot report successful persistence when it did not occur.
Captured DLT events are incrementally read and deduplicated before appending.
The audit views are refreshed by the same task.

The production workspace URL has an empty default intentionally; set
`BUNDLE_VAR_prod_workspace_host` before validating or deploying the `prod`
target. Configure it to the actual workspace URL in the deployment environment
rather than storing a workspace-specific endpoint in this file.

## 2. Telemetry Schema & Security Integration Matrix

| Data Layer | Target Table | Security Governance Applied | Operational Telemetry Tracked |
| :--- | :--- | :--- | :--- |
| **Audit** | `audit.audit_logs` | Append-Only, Admin Read Access | Pipeline execution status, execution duration, row mutations, exception traces |
| **Bronze** | `bronze.kafka_orders` | System Governed Storage | Ingestion throughput, corrupt record counts, raw Kafka offsets |
| **Silver** | `silver.dim_customers_scd` | CLS Masking (`email`, `phone`), Tag: `PII` | SCD2 validity timestamps, expectation pass/drop rates |
| **Gold** | `gold.fact_sales` | Dynamic RLS Row Filtering (`user_region_mapping`) | Query latency, aggregation record counts |