from ingestion_script.cpu_coolers_ingest import scrape_all
import polars as pl

def run():
    data = scrape_all()
    df = pl.DataFrame(data)
    df = df.unique()
    df = df.drop('note***')
    df = df.drop('note')
    df = df.drop_nulls(subset=["name","url","price"])
    for _ in df.columns:
        if df[_].null_count() > df.height * 0.8:
            df = df.drop(_)
    df = df.with_columns(
        pl.col('price').str.replace_all(r"[₹,]", "").cast(pl.Float64).alias('price_inr'),
        pl.col('warranty').str.extract(r'(\d+)').cast(pl.Int32).alias('warranty_in_years'),
        pl.col('radiator_size').str.extract(r'(\d+)').cast(pl.Int32).alias('radiator_size_in_mm'),
        pl.col('fan_size').str.extract(r'(\d+)').cast(pl.Int32).alias('fan_size_in_mm')
    )
    df = df.drop('price')
    df = df.drop('warranty')
    df = df.drop('radiator_size')
    df = df.drop('fan_size')

    df.write_parquet('cpu_cooler_cleaned.parquet')
    print(df.shape)
    print(df.schema)
if __name__ == "__main__":
    run()