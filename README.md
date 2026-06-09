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
>> if u don't want to invest too much time and need specifically current data previous data is also there as parquet u can just move them in root and skip scrapper part

```bash
uv run python load_bronze.py
```
> move to dashboard
```bash
cd dashboard
```
> move dahsboard
```bash
cp ~/pc-market-intelligence/pc_parts.db ~/pc-market-intelligence/dashboard/pc_parts.db
```
> finally run
```bash
npm run sources && npm run dev
```
> to explore dbt and my schema
```bash
cd ..
```
```bash
cd PC_MARKET_INTELLIGENCE/
```
```bash
dbt run && dbt test
```
```bash
dbt docs generate && dbt docs serve
```

## 📂 Project Structure

```text
.
├── PC-MARKET-INTELLIGENCE       # All dbt models are here
├── ingestion_script/            # web scrapers using beautiful soup with lxml parser 
├── sync_script/                 # All polars based cleaners that do basic cleaning and casting [bronze layer]
├── dashboard                    # All file related to evidence.dev dashboard
├── prev_data                    # All parquet file during time of committing for time saving 
├── load_bronze.py               # Connecting file for duckDB
└── run_pipeline.py              # a main runner just run this to run all scripts
```

-----


## 👨‍💻 Author

**Kavyansh (krish007code)**
  * Focus: Analytical Engineering | Data Architectures | Linux Systems

I tried my best to make this if someone found anything that can be imporvised i am happy to hear on **kavyanshkumarbaghel@gmail.com**
