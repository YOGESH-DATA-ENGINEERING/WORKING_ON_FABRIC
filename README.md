# WORKING_ON_FABRIC

# Microsoft Fabric – Data Engineering Foundations (Week 1)

This repository captures **Week 1 (Days 1–7)** of a **30-day structured Microsoft Fabric learning & implementation plan**, focused on building **production-ready data engineering foundations**.

The goal is not just to learn tools, but to **design, build, and reason like a Lead Data Engineer**.

---

## 🎯 Week 1 Objective – Platform & Foundations

> *“Understand the system before touching production.”*

Focus areas:
- Fabric workspace & Git integration
- Lakehouse architecture
- Raw → Silver → Gold layered design
- Delta tables & data modeling
- Batch ingestion & transformations
- Data quality, monitoring & observability

---

## 🏗️ Architecture Implemented

<img width="679" height="355" alt="image" src="https://github.com/user-attachments/assets/baab0cf6-64df-40d2-93d3-27a3d8b2c9f0" />


---

## 📊 Data Layers Explained

### 🔹 Raw Layer
- Immutable source data
- Stored as Delta tables
- No transformations
- Acts as the source of truth

### 🔹 Silver Layer
- Data cleaning & standardization
- Type casting, validation, filtering
- No business logic or aggregations

### 🔹 Gold Core Layer
- Business-enriched datasets
- Derived columns (year, month, value buckets)
- Trusted source of business truth

### 🔹 Gold Serving Layer
- Denormalized, aggregated tables
- Optimized for analytics & BI
- City-wise and monthly aggregations

---

## 🧪 Pipelines & Transformations

### Implemented Transformations
- Raw → Silver cleansing
- Silver → Gold business logic
- Time-based and dimension-based aggregations

### Example Business Logic
- Order value buckets (LOW / MEDIUM / HIGH)
- Revenue calculations
- Time dimensions (year, month)

---

## 📈 Aggregated Tables (Gold Serving)

### `agg_city_sales`
- city
- order_year
- total_orders
- total_revenue

### `agg_monthly_sales`
- order_year
- order_month
- total_orders
- total_revenue

These tables are designed for **fast BI queries and dashboards**.

---

## 🛡️ Monitoring & Observability

A dedicated monitoring table was implemented:

### `pipeline_monitoring`
| Column | Description |
|-----|------------|
| pipeline_name | Pipeline identifier |
| layer | raw / silver / gold |
| record_count | Row count |
| load_date | Execution date |
| status | SUCCESS / FAILED |

### Capabilities
- Pipeline execution tracking
- Row count monitoring
- Failure simulation
- Operational visibility

---

## 🔄 Git Integration

- Fabric workspace connected to Git
- Notebooks, metadata & pipelines version-controlled
- Follows GitOps-style workflow
- Data is **not** stored in Git (only code & metadata)

---

## 🧠 Key Engineering Principles Practiced

- Raw data immutability
- Separation of concerns across layers
- Business logic isolation in Gold
- Denormalization for analytics
- Monitoring as a first-class citizen
- Cost & performance awareness

---

## 🚀 What’s Next (Week 2 Preview)

Upcoming focus areas:
- Streaming ingestion (Eventstream)
- Incremental loads
- CI/CD & multi-environment promotion
- Performance optimization
- Advanced monitoring & alerts

---

## 📌 About This Repository

This repo represents **real-world, production-style Microsoft Fabric work**, designed to demonstrate:
- Architecture thinking
- Engineering discipline
- End-to-end pipeline understanding
- Lead-level reasoning

If you’re hiring for **Data Engineer / Fabric Engineer roles**, feel free to explore the notebooks and structure.

---


