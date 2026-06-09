from ingestion_script.processor_ingest import scrape_all
import polars as pl

def run():
    data = scrape_all()
    df = pl.DataFrame(data)
    df = df.unique()
    df = df.drop_nulls(subset=["name","url","price"])

    for _ in df.columns:
        if df[_].null_count() > df.height * 0.4:
            df = df.drop(_)
    
    df = df.with_columns(
        pl.col('price').str.replace_all(r"[₹,]", "").cast(pl.Float64).alias('price_inr'),
        pl.col('warranty*').str.extract(r'(\d+)').cast(pl.Int32).alias('warranty_in_years')
    )

    df = df.drop('price')
    df = df.drop('note')
    df = df.drop('warranty')
    df = df.drop('warranty*')

    df.write_parquet('processor_cleaned.parquet')
    print(df.shape)
    print(df.schema)
if __name__ == "__main__":
    run()