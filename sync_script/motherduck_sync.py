import os
import sys
import duckdb

# ──────────────────────────────────────────────────────────────────────────────
# CREDENTIALS
# ──────────────────────────────────────────────────────────────────────────────

MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
# Supporting both Kestra environment names and local dlt names
MINIO_USER       = os.environ.get("MINIO_USER",    os.environ.get("DESTINATION__S3__ACCESS_KEY_ID"))
MINIO_PASS       = os.environ.get("MINIO_PASSWORD", os.environ.get("DESTINATION__S3__SECRET_ACCESS_KEY"))

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = "minio:9000"          # Docker service name
# Pattern matches dlt's layout: s3://<bucket>/<dataset_name>/<resource_name>/...
PARQUET_GLOB   = "s3://pc-parts-bronze/laptops/bronze_layer/**/*.parquet"
MOTHERDUCK_DB  = "pc_market_intelligence"
TARGET_TABLE   = "laptops"

# ──────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ──────────────────────────────────────────────────────────────────────────────

def _validate_env() -> None:
    """Ensure all required secrets are present before starting."""
    missing = [name for name, val in [
        ("MOTHERDUCK_TOKEN", MOTHERDUCK_TOKEN),
        ("MINIO_USER",       MINIO_USER),
        ("MINIO_PASSWORD",   MINIO_PASS),
    ] if not val]

    if missing:
        print(f"❌ Missing required environment variables: {', '.join(missing)}")
        print("   Check your Kestra KV store or Docker .env file.")
        sys.exit(1)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN SYNC LOGIC
# ──────────────────────────────────────────────────────────────────────────────

def sync() -> None:
    _validate_env()

    local = None
    md    = None

    try:
        print("=" * 60)
        print("🚀 MotherDuck Sync – Laptop Pipeline")
        print("=" * 60)
        
        # ── Step 1: Read from MinIO ──────────────────────────────────────────
        print(f"📡 Connecting to MinIO at {MINIO_ENDPOINT} …")
        local = duckdb.connect()
        local.execute("INSTALL httpfs; LOAD httpfs;")
        local.execute("SET s3_region='us-east-1';")
        local.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
        local.execute("SET s3_use_ssl=false;")
        local.execute("SET s3_url_style='path';")
        local.execute(f"SET s3_access_key_id='{MINIO_USER}';")
        local.execute(f"SET s3_secret_access_key='{MINIO_PASS}';")

        print(f"📂 Reading Parquet files from: {PARQUET_GLOB}")

        df = local.execute(f"""
            SELECT * FROM read_parquet(
                '{PARQUET_GLOB}',
                union_by_name = true
            )
        """).df()

        rows_read = len(df)
        print(f"📦 Rows fetched from MinIO : {rows_read:,}")

        if rows_read == 0:
            print("⚠️  No rows found – ensure the ingestion step ran successfully.")
            return

        # ── Step 1.5: Basic cleaning (Silver Layer logic) ────────────────────
        # Remove empty names and nonsensical prices
        before = len(df)
        df = df.dropna(subset=["name"])
        df = df[df["price_inr"] > 0]
        rows_clean = len(df)
        
        dropped = before - rows_clean
        if dropped > 0:
            print(f"🧹 Cleaned {dropped:,} invalid/empty rows.")
        
        print(f"✅ Rows ready for cloud    : {rows_clean:,}")

        # ── Step 2: Push to MotherDuck ───────────────────────────────────────
        print(f"☁️  Connecting to MotherDuck (db: {MOTHERDUCK_DB}) …")
        md = duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}")

        print(f"📤 Writing to table '{TARGET_TABLE}' (CREATE OR REPLACE) …")
        md.execute(f"CREATE OR REPLACE TABLE {TARGET_TABLE} AS SELECT * FROM df")

        # ── Step 3: Validate Row Count ───────────────────────────────────────
        result = md.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()
        rows_in_md = result[0] if result else 0

        if rows_in_md == rows_clean:
            print(f"✅ Synced {rows_in_md:,} rows → MotherDuck '{TARGET_TABLE}'")
        else:
            print(f"⚠️  Mismatch: Sent {rows_clean:,}, but MD shows {rows_in_md:,}")

        print("🏁 Sync complete.")

    except duckdb.Error as exc:
        print(f"❌ DuckDB error: {exc}")
        raise
    except Exception as exc:
        print(f"❌ Unexpected error: {exc}")
        raise

    finally:
        # ── Step 4: Always close to save RAM ────────────────────────────────
        if local:
            local.close()
            print("🔒 Local DuckDB connection closed.")
        if md:
            md.close()
            print("🔒 MotherDuck connection closed.")

if __name__ == "__main__":
    sync()