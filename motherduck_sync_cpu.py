import duckdb
import os

token = os.environ["MOTHERDUCK_TOKEN"]
minio_user = os.environ["MINIO_USER"]
minio_pass = os.environ["MINIO_PASSWORD"]

# Step 1: read from MinIO locally
local = duckdb.connect()
local.execute("INSTALL httpfs; LOAD httpfs;")
local.execute("SET s3_region='us-east-1';")
local.execute("SET s3_endpoint='minio:9000';")
local.execute("SET s3_use_ssl=false;")
local.execute("SET s3_url_style='path';")
local.execute(f"SET s3_access_key_id='{minio_user}';")
local.execute(f"SET s3_secret_access_key='{minio_pass}';")

df = local.execute("""
    SELECT * FROM read_parquet(
        's3://pc-parts-bronze/cpus/**/*.parquet',
        union_by_name = true
    )
""").df()

local.close()
print(f"📦 Fetched {len(df)} rows from MinIO")

# Step 2: push to MotherDuck
md = duckdb.connect(f"md:pc_market_intelligence?motherduck_token={token}")
md.execute("CREATE OR REPLACE TABLE cpus AS SELECT * FROM df")
print(f"✅ Synced {len(df)} rows to MotherDuck")
md.close()
