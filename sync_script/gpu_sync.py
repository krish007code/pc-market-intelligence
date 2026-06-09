from ingestion_script.gpu_ingest import scrape_all
import polars as pl

def run():
    data = scrape_all()
    df = pl.DataFrame(data)
    print(df.columns)
    df = df.unique()
    df = df.drop_nulls(subset=["url", "price", "name"])
    df = df.drop('note**')

    df = df.with_columns(
        pl.col("price").str.replace_all(r"[₹,]", "").cast(pl.Float64).alias('price_inr'),
        pl.col("recommended_psu").str.extract(r"(\d+)").cast(pl.Int64),
        pl.col("gpu_core_(cuda_core)").cast(pl.Int64).alias('gpu_core'),
        pl.col("pci_express").cast(pl.Float64),
        pl.col("memory_clock").str.extract(r"(\d+)").cast(pl.Int64),
        pl.col("memory_size").str.extract(r"(\d+)").cast(pl.Int64),
        pl.col("memory_interface").str.extract(r"(\d+)").cast(pl.Int64),
        pl.col("opengl").cast(pl.Float64),
        pl.col("max_display_support").cast(pl.Float64),
        # ports to be extended by pl.extend
        pl.col('warranty').str.extract(r'(\d+)').cast(pl.Int64).alias('warranty_in_year')
    )
    df = df.drop('price')
    df = df.drop('gpu_core_(cuda_core)')
    df =df.drop('warranty')
    print(df.shape)
    print(df.schema)
    df.write_parquet('gpu_sync_cleaned.parquet')

if __name__ == "__main__":
    run()