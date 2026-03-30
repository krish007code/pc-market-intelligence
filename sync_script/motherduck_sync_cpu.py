"""
motherduck_sync_cpu.py  –  Silver-layer sync for CPU data.
Reads all Parquet files from MinIO (Bronze) and upserts them into MotherDuck.

Fixes applied vs. original draft
─────────────────────────────────
1.  Glob path corrected  – Uses `s3://pc-parts-bronze/bronze_layer/**/*.parquet`
    (matching the dlt-emitted layout) instead of the old `cpus/**/*.parquet`
    path which pointed at the wrong prefix.
2.  Credential safety  – All secrets use os.environ.get() with a clear warning
    when they are absent, so test runs fail gracefully with a human-readable
    message instead of a cryptic AttributeError.
3.  Explicit connection close  – Both `local` and `md` connections are closed
    in a `finally` block so they are always released, even on error. This
    prevents memory leaks on the 16 GB host.
4.  CREATE OR REPLACE semantics preserved  – Keeps full-refresh behaviour
    (idempotent on repeated runs) because we have no natural primary key to
    do a true MERGE on.
5.  Row-count validation  – Checks that MotherDuck actually received the same
    number of rows we read from MinIO and prints a warning if not.
6.  Structured logging  – Emoji-prefixed print statements at every major step.
"""

import os
import sys
import duckdb

# ──────────────────────────────────────────────────────────────────────────────
# CREDENTIALS  (all via .get() – never crash on a missing variable)
# ──────────────────────────────────────────────────────────────────────────────

MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
MINIO_USER       = os.environ.get("MINIO_USER",     os.environ.get("DESTINATION__S3__ACCESS_KEY_ID"))
MINIO_PASS       = os.environ.get("MINIO_PASSWORD",  os.environ.get("DESTINATION__S3__SECRET_ACCESS_KEY"))

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT    = "minio:9000"          # Docker service name; no scheme here
PARQUET_GLOB      = "s3://pc-parts-bronze/bronze_layer/**/*.parquet"
MOTHERDUCK_DB     = "pc_market_intelligence"
TARGET_TABLE      = "cpus"

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
        print("🚀 MotherDuck Sync – CPU Pipeline")
        print("=" * 60)
        print(f"📡 Connecting to MinIO at {MINIO_ENDPOINT} …")

        local = duckdb.connect()                       # pure in-process DB
        local.execute("INSTALL httpfs; LOAD httpfs;")
        local.execute("SET s3_region='us-east-1';")    # required even for MinIO
        local.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
        local.execute("SET s3_use_ssl=false;")
        local.execute("SET s3_url_style='path';")      # MinIO requires path-style
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
        dropped = before - len(df)
        if dropped:
            print(f"🧹 Dropped {dropped:,} rows with null 'name'.")

        # Also remove rows where price is 0 (likely scrape failures)
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
        # CREATE OR REPLACE gives us idempotent full-refresh semantics.
        # If you later add a primary key (e.g. url), switch to:
        #   INSERT OR REPLACE INTO cpus SELECT * FROM df
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