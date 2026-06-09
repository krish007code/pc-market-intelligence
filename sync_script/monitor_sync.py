from ingestion_script.monitor_ingest import scrape_all
import polars as pl


def run():
    data = scrape_all()
    df = pl.DataFrame(data)

    total_r = df.height
    ans = []

    for _ in df.columns:
        if df[_].count() < total_r * 0.8:
            ans.append(_)
    
    df = df.drop(ans)
    df = df.unique()
    df = df.with_columns(
        pl.col("price").str.replace_all(r"[₹,]", "").cast(pl.Float64).alias("price_inr"),
        pl.col("viewing_angle").str.replace_all(r"/.*|°.*", "").str.strip_chars().cast(pl.Int32, strict=False)
    )
    df = df.drop('price')
    df = df.rename({"built_-_in_speaker": "built_in_speaker"})
    df.write_parquet('monitor_cleaned.parquet')
    print(df.shape)
    print(df.schema)

if __name__ == "__main__":
    run()