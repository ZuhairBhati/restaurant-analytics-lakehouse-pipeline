# 🍛 Spice Route — Restaurant Analytics Data Platform

![Databricks](https://img.shields.io/badge/Azure%20Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Azure](https://img.shields.io/badge/Microsoft%20Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/Azure%20SQL-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)
![Event Hub](https://img.shields.io/badge/Azure%20Event%20Hub-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)

An end-to-end data engineering platform for a fictional Indian restaurant chain — **Spice Route** — operating across the UAE. The platform ingests data from two sources (Azure SQL Database and Azure Event Hub), processes it through a **Bronze → Silver → Gold** Medallion Architecture on **Azure Databricks**, and delivers business-ready analytics including AI-enriched customer reviews, a Customer 360 view, and daily sales summaries.

---

## 🏛️ Architecture

```
┌───────────────────────┐          ┌───────────────────────┐
│    Azure SQL DB       │          │   Azure Event Hub     │
│                       │          │                       │
│  customers            │          │  live order events    │
│  restaurants          │          │  (Kafka protocol)     │
│  menu_items           │          └──────────┬────────────┘
│  historical_orders    │                     │
│  reviews              │                     │ Structured Streaming
└──────────┬────────────┘                     │
           │ JDBC (Databricks Notebook)       │
           ▼                                  ▼
┌─────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Delta)                 │
│                                                         │
│  customers │ restaurants │ menu_items │ reviews         │
│  historical_orders  │  orders  (stream + history)       │
└─────────────────────────┬───────────────────────────────┘
                          │
              CDC Merge (Dims)  +  DLT Streaming (Facts)
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Delta)                 │
│                                                         │
│  dim_customers │ dim_restaurants │ dim_menu_items       │
│  fact_orders   │ fact_order_items                       │
│  fact_reviews  ◄── AI Sentiment + Issue Classification  │
└─────────────────────────┬───────────────────────────────┘
                          │
              DLT Materialized Views
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Delta)                  │
│                                                         │
│  customer_360  │  daily_sales_summary                   │
│  daily_restaurant_reviews                               │
└─────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
.
├── 00_synthetic_data/
│   ├── 00_sql_db.py                    # Generates restaurants, menu_items, customers
│   ├── 01_historical_orders.py         # Generates 8,000 historical orders (6 months)
│   ├── 02_reviews.py                   # Generates customer reviews from orders
│   ├── 03_run.py                       # Master runner — executes all generators
│   ├── 04_eventhub_orders.py           # Streams synthetic live orders to Event Hub
│   ├── 05_csv_to_azuredb.py            # Uploads generated CSVs to Azure SQL DB
│   └── sql/
│       ├── azuresqldatabase_setup.sql  # Table DDL + CDC / Change Tracking setup
│       └── utility_script.sql          # Databricks Lakeflow utility stored procedures
│
└── 01_pipelines/
    ├── bronze/
    │   ├── sqldb_to_bronze.ipynb        # JDBC ingestion: SQL DB → Bronze Delta tables
    │   └── bronze_to_silver_cdc.ipynb   # Delta MERGE upsert: Bronze → Silver dims
    │
    ├── eventhub_ingestion/
    │   ├── explorations/
    │   │   └── eventhub_explore.py      # Interactive Event Hub exploration notebook
    │   └── transformations/
    │       └── eventhub.py              # DLT: streams Event Hub + unions historical orders
    │
    ├── silver/
    │   ├── explorations/
    │   │   ├── fact_orders_explore.py   # Ad-hoc exploration for orders
    │   │   └── order_items_explore.py   # Ad-hoc exploration for order line items
    │   └── transformations/
    │       ├── fact_orders.py           # DLT table: parsed & enriched orders fact
    │       ├── fact_order_items.py      # DLT table: exploded order line items
    │       └── fact_reviews.sql         # DLT streaming table: reviews + AI sentiment
    │
    └── gold/
        ├── explorations/
        │   ├── d_customer360.py          # Exploration for Customer 360
        │   ├── d_restaurant_reviews.py   # Exploration for restaurant review analytics
        │   └── d_sales_summary.py        # Exploration for daily sales summary
        └── transformations/
            ├── daliy_customer360.py           # DLT MV: Customer 360 with loyalty tiers
            ├── daily_sales_summary.py         # DLT MV: daily aggregated sales metrics
            └── daily_restaurant_reviews.py    # DLT MV: per-restaurant review analytics
```

---

## 🗂️ Data Model

A fictional Indian restaurant chain with **5 branches** across the UAE (Abu Dhabi · Dubai · Sharjah), **500 customers**, and **29 menu items** per restaurant.

### 🥉 Bronze Layer — Raw Ingested Data

| Table | Source | Rows | Description |
|---|---|---|---|
| `customers` | ![SQL](https://img.shields.io/badge/SQL%20DB-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white) | 500 | Customer profiles |
| `restaurants` | ![SQL](https://img.shields.io/badge/SQL%20DB-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white) | 5 | Branch details |
| `menu_items` | ![SQL](https://img.shields.io/badge/SQL%20DB-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white) | 145 | Menu per restaurant, price varies ±5% per branch |
| `historical_orders` | ![SQL](https://img.shields.io/badge/SQL%20DB-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white) | 8,000 | 6 months of completed orders |
| `reviews` | ![SQL](https://img.shields.io/badge/SQL%20DB-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white) | ~80 | Customer reviews (~1% of orders) |
| `orders` | ![EH](https://img.shields.io/badge/Event%20Hub-0078D4?style=flat-square&logo=microsoftazure&logoColor=white) + historical | Live | Unified real-time stream + historical batch |

### 🥈 Silver Layer — Cleaned & Modelled

| Table | Type | Key Transformations |
|---|---|---|
| `dim_customers` | Dimension | CDC upsert from Bronze |
| `dim_restaurants` | Dimension | CDC upsert from Bronze |
| `dim_menu_items` | Dimension | CDC upsert, composite PK (`restaurant_id` + `item_id`) |
| `fact_orders` | Fact | Timestamp parsing · `order_date` · `order_hour` · `day_of_week` · `is_weekend` · `item_count` |
| `fact_order_items` | Fact | JSON items array exploded to one row per line item |
| `fact_reviews` | Fact | ![AI](https://img.shields.io/badge/ai__query()-FF3621?style=flat-square&logo=databricks&logoColor=white) Sentiment + issue classification via `ai_query()` |

**`fact_reviews` enriched schema:**

```
sentiment                  -- positive / neutral / negative
issue_delivery             -- BOOLEAN + reason
issue_food_quality         -- BOOLEAN + reason
issue_pricing              -- BOOLEAN + reason
issue_portion_size         -- BOOLEAN + reason
```

### 🥇 Gold Layer — Business-Ready Aggregates

| Table | Description |
|---|---|
| `customer_360` | Loyalty tier (Bronze → Platinum), lifetime spend, avg order value, favourite restaurant & item, VIP flag (`spend ≥ AED 5,000`) |
| `daily_sales_summary` | Daily revenue, order count, avg order value, unique customers, breakdown by order type (dine-in / takeaway / delivery) |
| `daily_restaurant_reviews` | Avg rating, full 1–5 star distribution, positive / neutral / negative sentiment counts per restaurant |

---

## ⚙️ Design Decisions

### CDC Upsert for Dimension Tables
![Delta Lake](https://img.shields.io/badge/Delta%20MERGE-003366?style=flat-square&logo=apachespark&logoColor=white)

Dimension tables in Silver are maintained using Delta Lake `MERGE` — matching on primary keys and applying updates or inserts as they arrive. This avoids full table rewrites and keeps Silver always in sync with the source without duplication.

### Dual-Source Orders Stream
![Kafka](https://img.shields.io/badge/Event%20Hub%20%2F%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Delta](https://img.shields.io/badge/Delta%20Streaming-003366?style=flat-square&logo=apachespark&logoColor=white)

The Bronze `orders` table unions **real-time Event Hub events** (Kafka protocol) with the **historical orders batch** from Azure SQL DB. All downstream Silver and Gold tables consume a single, continuous stream covering every order — past and present.

```python
return df_parsed.unionByName(df_history, allowMissingColumns=True)
```

### AI-Powered Review Enrichment
![Databricks](https://img.shields.io/badge/ai__query()-FF3621?style=flat-square&logo=databricks&logoColor=white)

`fact_reviews` enriches every review inline using Databricks' `ai_query()` directly inside the DLT SQL pipeline. Each review is classified for sentiment and four issue categories — no separate ML job required.

```sql
ai_query(
  'databricks-gpt-oss-20b',
  CONCAT('Analyze the following review and return ONLY a valid JSON object...', review_text)
)
```

---

## 🛠️ Tech Stack

| Category | Tool |
|---|---|
| Cloud Platform | ![Azure](https://img.shields.io/badge/Microsoft%20Azure-0078D4?style=flat-square&logo=microsoftazure&logoColor=white) Microsoft Azure |
| Data Platform | ![Databricks](https://img.shields.io/badge/Azure%20Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white) Azure Databricks |
| Storage Format | ![Delta](https://img.shields.io/badge/Delta%20Lake-003366?style=flat-square&logo=apachespark&logoColor=white) Delta Lake |
| Orchestration | ![DLT](https://img.shields.io/badge/Delta%20Live%20Tables-FF3621?style=flat-square&logo=databricks&logoColor=white) Delta Live Tables |
| Streaming Source | ![EventHub](https://img.shields.io/badge/Azure%20Event%20Hub-0078D4?style=flat-square&logo=microsoftazure&logoColor=white) Azure Event Hub (Kafka protocol) |
| Batch Source | ![SQL](https://img.shields.io/badge/Azure%20SQL%20Database-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white) Azure SQL Database (JDBC) |
| Languages | ![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white) Python · ![SQL](https://img.shields.io/badge/SQL-003B57?style=flat-square&logoColor=white) SQL |
| AI / ML | ![Databricks](https://img.shields.io/badge/Foundation%20Models-FF3621?style=flat-square&logo=databricks&logoColor=white) Databricks `ai_query()` |
| Data Generation | ![Faker](https://img.shields.io/badge/Faker-3776AB?style=flat-square&logo=python&logoColor=white) Python Faker (`en_IN` locale) |
