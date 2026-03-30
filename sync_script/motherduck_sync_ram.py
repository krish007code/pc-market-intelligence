import os
import sys
import duckdb

# ──────────────────────────────────────────────────────────────────────────────
# CREDENTIALS
# ──────────────────────────────────────────────────────────────────────────────

MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
# Mapping both Kestra (MINIO_USER) and dlt (DESTINATION__S3...) env patterns
MINIO_USER       = os.environ.get("MINIO_USER",    os.environ.get("DESTINATION__S3__ACCESS_KEY_ID"))
MINIO_PASS       = os.environ.get("MINIO_PASSWORD", os.environ.get("DESTINATION__S3__SECRET_ACCESS_KEY"))

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = "minio:9000"          # Docker service name
# Pattern targets dlt's layout: s3://<bucket>/<dataset_name>/bronze_layer/...
PARQUET_GLOB   = "s3://pc-parts-bronze/bronze_layer/**/*.parquet"
MOTHERDUCK_DB  = "pc_market_intelligence"
TARGET_TABLE   = "ram"

# ──────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ──────────────────────────────────────────────────────────────────────────────

def _validate_env() -> None:
    """Ensure all required secrets are present to avoid cryptic DuckDB crashes."""
    missing = [name for name, val in [
        ("MOTHERDUCK_TOKEN", MOTHERDUCK_TOKEN),
        ("MINIO_USER",       MINIO_USER),
        ("MINIO_PASSWORD",   MINIO_PASS),
    ] if not val]

    if missing:
        print(f"❌ Missing required environment variables: {', '.join(missing)}")
        print("   Verify your Kestra KV store or Docker .env file.")
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
        print("🚀 MotherDuck Sync – RAM Pipeline")
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
            print("⚠️  No rows found – ensure the ram_ingest step ran first.")
            return

        # ── Step 1.5: Silver Layer Cleaning ──────────────────────────────────
        # RAM kits often have duplicate listings or '0' prices if out of stock
        before = len(df)
        df = df.dropna(subset=["name"])
        df = df[df["price_inr"] > 0]
        rows_clean = len(df)
        
        dropped = before - rows_clean
        if dropped > 0:
            print(f"🧹 Filtered {dropped:,} invalid/OOS RAM records.")
        
        print(f"✅ Cleaned Rows for Cloud   : {rows_clean:,}")

        # ── Step 2: Push to MotherDuck ───────────────────────────────────────
        print(f"☁️  Connecting to MotherDuck (db: {MOTHERDUCK_DB}) …")
        md = duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}")

        print(f"📤 Writing to table '{TARGET_TABLE}' (CREATE OR REPLACE) …")
        md.execute(f"CREATE OR REPLACE TABLE {TARGET_TABLE} AS SELECT * FROM df")

        # ── Step 3: Row Count Audit ──────────────────────────────────────────
        result = md.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()
        rows_in_md = result[0] if result else 0

        if rows_in_md == rows_clean:
            print(f"✅ Synced {rows_in_md:,} rows → MotherDuck '{TARGET_TABLE}'")
        else:
            print(f"⚠️  Mismatch detected: Sent {rows_clean:,}, MD has {rows_in_md:,}")

        print("🏁 Sync complete.")

    except duckdb.Error as exc:
        print(f"❌ DuckDB error: {exc}")
        raise
    except Exception as exc:
        print(f"❌ Unexpected error: {exc}")
        raise

    finally:
        # ── Step 4: Explicit Close (Crucial for HP laptop RAM management) ───
        if local:
            local.close()
            print("🔒 Local DuckDB connection closed.")
        if md:
            md.close()
            print("🔒 MotherDuck connection closed.")

if __name__ == "__main__":
    sync()