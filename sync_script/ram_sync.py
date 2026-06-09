from ingestion_script.ram_ingest import scrape_all
import polars as pl
 

def run():
    data = scrape_all()
    df = pl.DataFrame(data)
    df = df.unique()
    df = df.drop_nulls(subset=["name","url","price"])

    for _ in df.columns:
        if df[_].null_count() > df.height * 0.8:
            df = df.drop(_)
        
    df = df.with_columns(
        pl.col('price').str.replace_all(r"[₹,]", "").cast(pl.Float64).alias('price_inr'),
        pl.col('warranty').str.replace('Limited Lifetime', '99').str.extract(r'\d+').cast(pl.Int32).alias('warranty_in_years'),
        pl.col('capacity').str.extract(r'(\d+)').cast(pl.Int32).alias('capacity_in_GB'),
        pl.col('speed').str.extract(r'(\d+)').cast(pl.Int32).alias('speed_in_MHz'),
        pl.col('tested_voltage').str.extract(r'(\d+\.?\d*)').cast(pl.Float64).alias('tested_voltage_in_V')
    )
    df = df.drop('price')
    df = df.drop('note')
    df = df.drop('warranty')
    df = df.drop('capacity')
    df = df.drop('speed')
    df = df.drop('tested_voltage')
    df.write_parquet('ram_cleaned.parquet')
    print(df.shape)
    print(df.schema)

if __name__ == "__main__":
    run()