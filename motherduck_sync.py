import duckdb
import os

token = os.environ["MOTHERDUCK_TOKEN"]
minio_user = os.environ["MINIO_USER"]
minio_pass = os.environ["MINIO_PASSWORD"]

con = duckdb.connect(f"md:pc_market_intelligence?motherduck_token={token}")

con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_region='us-east-1';")
con.execute("SET s3_endpoint='minio:9000';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")
con.execute(f"SET s3_access_key_id='{minio_user}';")
con.execute(f"SET s3_secret_access_key='{minio_pass}';")

con.execute("""
    CREATE OR REPLACE TABLE laptops AS
    SELECT * FROM read_parquet(
        's3://pc-parts-bronze/laptops/**/*.parquet',
        union_by_name = true
    )
""")
print("Sync complete:", con.execute("SELECT COUNT(*) FROM laptops").fetchone())
con.close()
