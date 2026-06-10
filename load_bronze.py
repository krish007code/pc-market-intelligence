import duckdb

conn = duckdb.connect("pc_parts.db")

conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

conn.execute("CREATE OR REPLACE TABLE bronze.cpu_cooler_cleaned as SELECT * FROM read_parquet('cpu_cooler_cleaned.parquet')")
print(conn.execute("SELECT COUNT(*) FROM bronze.cpu_cooler_cleaned").fetchone())

conn.execute("CREATE OR REPLACE TABLE bronze.gpu_sync_cleaned as SELECT * FROM read_parquet('gpu_sync_cleaned.parquet')")
print(conn.execute("SELECT COUNT(*) FROM bronze.gpu_sync_cleaned").fetchone())

conn.execute("CREATE OR REPLACE TABLE bronze.monitor_cleaned as SELECT * FROM read_parquet('monitor_cleaned.parquet')")
print(conn.execute("SELECT COUNT(*) FROM bronze.monitor_cleaned").fetchone())

conn.execute("CREATE OR REPLACE TABLE bronze.motherboard_cleaned as SELECT * FROM read_parquet('motherboard_cleaned.parquet')")
print(conn.execute("SELECT COUNT(*) FROM bronze.motherboard_cleaned").fetchone())

conn.execute("CREATE OR REPLACE TABLE bronze.processor_cleaned as SELECT * FROM read_parquet('processor_cleaned.parquet')")
print(conn.execute("SELECT COUNT(*) FROM bronze.processor_cleaned").fetchone())

conn.execute("CREATE OR REPLACE TABLE bronze.ram_cleaned as SELECT * FROM read_parquet('ram_cleaned.parquet')")
print(conn.execute("SELECT COUNT(*) FROM bronze.ram_cleaned").fetchone())