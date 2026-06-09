# PC Market Intelligence

## Project Overview

This project help in getting various detailed insights from pc hardware data from reliable indian retailers like MD Computers a helpful tool for pc builders and enthusiasts

-----

## Architecture overivew

The project follows a structured data flow to transform raw web scraped date into actionable market insights with the implementation of **3 Medallion Architecture**. This project has web scrappers that get real world data from trusted indian retail sites after that the data gets cleaned from various scripts using polars and then it is staged and made model of by leveraging **dbt** then utilizing the model an easy to understand dashboard is made using **evidence.dev**.

-----

## How to use

> in terminal
```bash
cd
```
```bash
cd pc-market-intelligence
```
```bash
uv run python run_pipeline.py
```
> Now please wait accordingly
 
## 📂 Project Structure

```text
.
├── dbt/                         # SQL Transformation logic (Silver/Gold)
├── ingestion_script/            # Python scrapers & dlt pipelines
├── scripts/                     # Helper utilities for PDF generation
├── docker-compose.yml           # Full infrastructure definition
├── Dockerfile                   # Custom image for Python-based tasks
└── .env                         # Sensitive credentials (ignored by git)
```

-----

## 📈 Key Features

  * **Resilient Ingestion:** Uses `dlt` to handle schema evolution and state management.
  * **Containerized Execution:** Every task runs in an isolated Docker container managed by Kestra.
  * **Hybrid Cloud:** Combines local "heavy" storage (MinIO) with cloud-based analytics (MotherDuck).
  * **Automated Reporting:** Generates AI-driven PDF reports sent via WhatsApp/Email (developed during Hacked 4.0).

-----

## 👨‍💻 Author

**Kavyansh (krish007code)**
  * Focus: Data Engineering | DevSecOps | Linux Systems

