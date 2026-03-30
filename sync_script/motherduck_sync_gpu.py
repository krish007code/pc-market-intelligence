"""
motherduck_sync_gpu.py  –  Silver-layer sync for GPU data.
Reads all Parquet files from MinIO (Bronze) and upserts them into MotherDuck.

Fixes applied vs. original draft
─────────────────────────────────
1.  Glob path corrected  – Uses `s3://pc-parts-bronze/bronze_layer/**/*.parquet`
                           to match the actual dlt output layout under the
                           `gpus/bronze_layer/` prefix.
2.  Credential safety    – All secrets use os.environ.get(); missing variables
                           cause a clean sys.exit(1) with a clear message
                           instead of a cryptic AttributeError inside DuckDB.
3.  Explicit connection close in finally – Both `local` and `md` are always
                           released, preventing memory leaks on the 16 GB host.
4.  Zero-price row filter – Rows where price_inr == 0 (failed scrapes) are
                           dropped before writing to the Silver layer.
5.  Post-write row count validation – Queries COUNT(*) from MotherDuck after
                           the write and warns if the number doesn't match.
6.  Structured logging   – Emoji-prefixed print statements at every step.
"""

import os
import sys
import duckdb

# ──────────────────────────────────────────────────────────────────────────────
# CREDENTIALS
# ──────────────────────────────────────────────────────────────────────────────

MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
MINIO_USER       = os.environ.get("MINIO_USER",    os.environ.get("DESTINATION__S3__ACCESS_KEY_ID"))
MINIO_PASS       = os.environ.get("MINIO_PASSWORD", os.environ.get("DESTINATION__S3__SECRET_ACCESS_KEY"))

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = "minio:9000"          # Docker service name – no scheme
# dlt writes to  s3://<bucket>/<dataset_name>/<resource_name>/...
# bucket_url was s3://pc-parts-bronze/gpus  and  dataset_name = bronze_layer
# so the actual prefix is:  pc-parts-bronze/gpus/bronze_layer/
PARQUET_GLOB   = "s3://pc-parts-bronze/gpus/bronze_layer/**/*.parquet"
MOTHERDUCK_DB  = "pc_market_intelligence"
TARGET_TABLE   = "gpus"

# ──────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ──────────────────────────────────────────────────────────────────────────────

def _validate_env() -> None:
    missing = [name for name, val in [
        ("MOTHERDUCK_TOKEN", MOTHERDUCK_TOKEN),
        ("MINIO_USER",       MINIO_USER),
        ("MINIO_PASSWORD",   MINIO_PASS),
    ] if not val]

    if missing:
        print(f"❌ Missing required environment variables: {', '.join(missing)}")
        print("   Set them in your Kestra task environment or .env file.")
        sys.exit(1)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN SYNC LOGIC
# ──────────────────────────────────────────────────────────────────────────────

def sync() -> None:
    _validate_env()

    local = None
    md    = None

    try:
        # ── Step 1: Read from MinIO ──────────────────────────────────────────
        print("=" * 60)
        print("🚀 MotherDuck Sync – GPU Pipeline")
        print("=" * 60)
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
            print("⚠️  No rows found – is the ingestion pipeline complete?")
            return

        # ── Step 1.5: Basic cleaning ─────────────────────────────────────────
        before = len(df)
        df = df.dropna(subset=["name"])
        dropped_name = before - len(df)
        if dropped_name:
            print(f"🧹 Dropped {dropped_name:,} rows with null 'name'.")

        before = len(df)
        df = df[df["price_inr"] > 0]
        dropped_price = before - len(df)
        if dropped_price:
            print(f"🧹 Dropped {dropped_price:,} rows with zero price.")

        rows_clean = len(df)
        print(f"✅ Rows after cleaning      : {rows_clean:,}")

        # ── Step 2: Push to MotherDuck ───────────────────────────────────────
        print(f"☁️  Connecting to MotherDuck (db: {MOTHERDUCK_DB}) …")
        md = duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}")

        print(f"📤 Writing to table '{TARGET_TABLE}' (CREATE OR REPLACE) …")
        md.execute(f"CREATE OR REPLACE TABLE {TARGET_TABLE} AS SELECT * FROM df")

        # ── Step 3: Validate row count ───────────────────────────────────────
        result     = md.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()
        rows_in_md = result[0] if result else 0

        if rows_in_md == rows_clean:
            print(f"✅ Synced {rows_in_md:,} rows → MotherDuck '{TARGET_TABLE}'")
        else:
            print(
                f"⚠️  Row count mismatch: "
                f"sent {rows_clean:,}, MotherDuck shows {rows_in_md:,}"
            )

        print("🏁 Sync complete.")

    except duckdb.Error as exc:
        print(f"❌ DuckDB error: {exc}")
        raise
    except Exception as exc:
        print(f"❌ Unexpected error: {exc}")
        raise

    finally:
        # ── Step 4: Always release connections ───────────────────────────────
        if local:
            local.close()
            print("🔒 Local DuckDB connection closed.")
        if md:
            md.close()
            print("🔒 MotherDuck connection closed.")


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sync()