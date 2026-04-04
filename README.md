# 🚀 PC Market Intelligence Pipeline

### *Automated Data Engineering Platform for Real-time Hardware Analytics*

## 📌 Project Overview

This platform automates the collection, transformation, and analysis of PC component pricing from major Indian retailers (MD Computers, Vedant Computers, PrimeABGB, and TheITDepot).
-----

## 🏗️ Architecture

The pipeline follows a structured data flow to transform raw web scrapes into actionable market insights:

1.  **Bronze Layer (Raw):** Multi-source scraping using `dlt` and `BeautifulSoup4/Playwright`. Raw Parquet files are stored in a local **MinIO** S3 bucket.
2.  **Silver Layer (Cleaned):** Data is ingested into **MotherDuck** (DuckDB in the cloud), where schemas are enforced, and prices are normalized into numeric formats.
3.  **Gold Layer (Analytics):** Business logic ranks laptops and components based on price-to-performance metrics, generating an automated **AI Stock Report**.

-----

## 🛠️ Tech Stack

  * **Orchestration:** [Kestra](https://kestra.io/) (Docker-based workflow automation)
  * **Ingestion:** [dlt (Data Load Tool)](https://dlthub.com/)
  * **Storage (Object):** [MinIO](https://min.io/) (S3-compatible local storage)
  * **Warehouse:** [MotherDuck](https://motherduck.com/) (Serverless DuckDB)
  * **Environment:** Docker Compose, uv, Python 3.13
  * **OS:** Fedora Linux (Optimized for DevSecOps workflows)

-----

## 🚀 Getting Started

### 1\. Prerequisites

  * Docker & Docker Compose
  * A MotherDuck account and API Token
  * Fedora Linux (recommended) or any stable Linux distro

### 2\. Environment Setup

Create a `.env` file in the root directory:

```bash
POSTGRES_USER=kestra
POSTGRES_PASSWORD=your_password
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password123
MOTHERDUCK_TOKEN=your_md_token
```

### 3\. Deployment

Spin up the entire stack (Database, MinIO, and Kestra) with a single command:

```bash
docker compose up --build -d
```

Access the Kestra UI at `http://localhost:8080`.

-----

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

-----

### 💡 Pro-Tip for your Portfolio

When you push this to GitHub, make sure you don't commit your `.venv` or your `.env` file. This README tells a story of **Architecture** and **Systems Design**, which is exactly what top-tier recruiters look for in a 9.37 SGPA student\!
