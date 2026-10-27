# 🛒 Capstone Project: Maven Market 
**Secure Retail Analytics Lakehouse (DevOps, CI/CD, & Enterprise-Grade Architecture)**

![Azure](https://img.shields.io/badge/Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)

---

## 📖 Overview

**Maven Market** is a multinational grocery and retail enterprise operating across North America, Europe, and Asia. This project modernizes their highly fragmented data landscape—spanning POS CSVs, MongoDB Master Data, and real-time Kafka event streams—into a unified **Data Intelligence Platform on Azure Databricks**.

This project implements modern software engineering practices: Git versioning, **Databricks Asset Bundles (DABs)**, CI/CD, 100% config-driven modularity, custom telemetry, and strict compute governance, fully aligning with the 7 Pillars of the Databricks Data + AI Well-Architected Framework.

---

## 🏗️ Architecture

### Medallion Data Pipeline
Data is processed using **Delta Live Tables (DLT)** through three discrete stages:

*   🥉 **Bronze (Raw Ingestion):** Append-only raw data from Auto Loader (CSVs), PyMongo (MongoDB Atlas), and PySpark Streaming (Confluent Kafka). Includes audit metadata (`_ingested_at`).
*   🥈 **Silver (Cleansing & History):** Enforces data quality via DLT Expectations, performs stream-static joins, and tracks dimensional updates using Slowly Changing Dimensions (SCD) Type 2.
*   🥇 **Gold (Star Schema):** Business-ready Fact and Dimension tables optimized for sub-second BI query performance.

```text
 Raw Sources (CSV, MongoDB, Kafka) 
                 │
                 ▼
 🥉 BRONZE LAYER (Raw Ingestion)         --> Ingests raw data as-is with metadata
                 │
                 ▼
 🥈 SILVER LAYER (Cleansing & History)   --> Cleans data, handles SCD2 history, joins streams
                 │
                 ▼
 🥇 GOLD LAYER (Star Schema Analytics)   --> Business-ready Fact & Dimension tables for BI
```

---

## 🔐 Enterprise Governance & Security (Unity Catalog)

Traditional Role-Based Access Control (RBAC) was replaced with **Attribute-Based Access Control (ABAC)** to solve policy bloat, utilizing dynamic masking and filtering based on user attributes.

### Governed Tag Taxonomy
| Tag Key | Scope | Allowed Values | Enforcement |
| :--- | :--- | :--- | :--- |
| `classification` | Column | `PII`, `Financial`, `Public` | **CLS:** Redacts strings dynamically (e.g., `***MASKED_PII***`). |
| `data_region` | Table/Column| `EU`, `NA`, `APAC` | **RLS:** Limits visibility based on user territory mapping. |
| `data_sensitivity` | Table | `PII`, `Financial`, `Internal`, `Public` | **Compliance:** Blocks table creation without assigned risk tier. |
| `owner_team` | Table | `data_engineering`, `revenue_analytics`, etc. | **Ownership:** Enforces operational accountability. |

### Access Control Matrix
| Identity / Role | `customer_email` (CLS) | Territory Data (RLS) | Mechanism |
| :--- | :--- | :--- | :--- |
| **Workspace Admin** | Unmasked | All Regions | `IS_ACCOUNT_GROUP_MEMBER` bypass |
| **Data Compliance** | Unmasked | All Regions | `IS_ACCOUNT_GROUP_MEMBER` bypass |
| **Regional Mgr (NA)** | Masked (`***MASKED_PII***`) | North America only | Dynamic `audit.user_region_mapping` |

---

## 📂 Repository Structure

```text
capstone_maven_market/
├── .github/workflows/
│   └── deploy.yml              # CI/CD Workflow automation script
├── config/
│   └── config.yml              # Config-driven pipeline parameters
├── src/
│   ├── dlt/
│   │   ├── 01_bronze.py        # Ingestion layer (Auto Loader, PyMongo, Kafka)
│   │   ├── 02_silver.py        # Cleansing, stream joins, SCD Type 2
│   │   └── 03_gold.py          # Star Schema (Fact & Dimension tables)
│   ├── security/
│   │   └── abac_governance.sql # Governed Tags, CLS Masking, RLS Row Filters
│   └── telemetry/
│       └── logger.py           # Custom structured logging and audit sync
├── tests/
│   └── test_config.py          # Pytest unit tests for configurations
├── databricks.yml              # Master DABs declarative IaC specification
└── requirements.txt            # Python dependencies
```

---

## 🚀 DevOps & CI/CD (Databricks Asset Bundles)

Infrastructure is managed purely as code using **Databricks Asset Bundles (DABs)**, fully integrated with **GitHub Actions**. Secrets are never hardcoded; they are retrieved dynamically via Azure Key Vault + Databricks Secret Scopes (`{{secrets/scope-maven-market/key-name}}`).

### 1. Local Setup & Validation
Ensure you have the Databricks CLI installed (v0.200.0+) and authenticate:
```bash
# Authenticate with Databricks Host
databricks auth login --host https://adb-<workspace-id>.azuredatabricks.net

# Validate Infrastructure-as-Code syntax
databricks bundle validate
```

### 2. Deploy to Development Environment
Deploy your DLT pipelines and Workflow jobs into an isolated developer workspace path without impacting production schemas:
```bash
databricks bundle deploy -t dev

# Trigger the end-to-end orchestration job
databricks bundle run lakehouse_daily_workflow -t dev
```

### 3. CI/CD Pipeline (GitHub Actions)
The automated deployment pipeline (`deploy.yml`) is triggered via Git events:
* **Pull Requests (main):** Triggers Python linting (`flake8`), unit tests (`pytest`), bundle validation, and deploys to the `dev` target.
* **Merge to Main:** Runs the complete QA suite and automatically executes `databricks bundle deploy -t prod`, promoting the Lakehouse to production.

---

## 📊 Analytics & Observability

The platform features comprehensive observability and self-service analytics via four primary **Databricks SQL Dashboards**:

1. 📈 **Executive Overview:** Total Revenue, Profit Margin, YoY Growth.
2. ⚡ **Real-Time Ops:** Kafka order streaming velocity, live low-inventory alerts.
3. 🌍 **Regional Sales:** ABAC-governed dashboard showing localized sales and leaderboard metrics strictly filtered to the user's territory.
4. 🩺 **Platform Health:** Centralized telemetry reading from `audit.audit_logs`, tracking pipeline runs, expectation drop rates, and record counts.

*(A Lakeflow Designer no-code visual preparation pipeline is also provisioned to empower business analysts with Genie AI-assisted calculated metrics).*