import argparse

from pyspark.sql import functions as F

from lakehouse import bronze, gold, silver
from lakehouse.download import fetch
from lakehouse.paths import BRONZE, GOLD_DIR, QUARANTINE, SILVER
from lakehouse.spark import get_spark


def main():
    ap = argparse.ArgumentParser(description="Bronze, silver and gold build of the CFPB complaint database.")
    ap.add_argument("--csv", help="Use this CSV instead of downloading the CFPB file")
    ap.add_argument("--batch-id", help="Required with --csv")
    a = ap.parse_args()
    if a.csv:
        if not a.batch_id:
            ap.error("--csv needs --batch-id")
        csv_path, batch_id = a.csv, a.batch_id
    else:
        csv_path, batch_id = fetch()

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"bronze: {bronze.load(spark, str(csv_path), str(BRONZE), batch_id)} rows loaded for batch {batch_id}")

    valid, rejected = silver.clean(bronze.read_batch(spark, str(BRONZE), batch_id))
    silver.quarantine(spark, str(QUARANTINE), rejected, batch_id)
    counts = silver.upsert(spark, str(SILVER), valid)
    print(f"silver: {counts['inserted']} inserted, {counts['updated']} updated")
    q = spark.read.format("delta").load(str(QUARANTINE)).where(F.col("_batch_id") == batch_id)
    print("quarantine:", {r.reject_reason: r["count"] for r in q.groupBy("reject_reason").count().collect()} or "none")

    gold.build(spark, str(SILVER), str(GOLD_DIR))
    print("gold: rebuilt")


if __name__ == "__main__":
    main()
