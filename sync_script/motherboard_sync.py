from ingestion_script.motherboard_ingest import scrape_all
import polars as pl

def run():
    data = scrape_all()
    df = pl.DataFrame(data)
    df = df.unique()
    df = df.drop_nulls(subset=["name","url","price"])

    df = df.with_columns(
        pl.col('price').str.replace_all(r"[₹,]", "").cast(pl.Float64).alias('price_inr'),
        pl.col('memory_speed').str.extract(r"(/d+)").cast(pl.Int32).alias('memory_speed_upto'),
        pl.col('max_memory_support').str.extract(r"(/d+)").cast(pl.Int32).alias('max_memory_support_in_GB'),
        pl.col('warranty').str.extract(r'(\d+)').cast(pl.Int32).alias('warranty_in_years')
    )
    for _ in df.columns:
        if df[_].null_count() > df.height * 0.8:
            df = df.drop(_)
            
    df = df.drop('price')
    df = df.drop('warranty')
    df = df.drop('memory_speed')
    df = df.drop('max_memory_support')
    df = df.drop('note')
    df = df.drop('operating_system')


    df.write_parquet('motherboard_cleaned.parquet')
    print(df.shape)
    print(df.schema)
if __name__ == "__main__":
    run()