from pyspark.sql import functions as F

RELIEF = ["Closed with monetary relief", "Closed with non-monetary relief"]


def build(spark, silver_path, gold_dir):
    s = spark.read.format("delta").load(silver_path)
    out = {
        "complaints_by_month": s.groupBy(F.trunc("date_received", "month").alias("month"), "product").agg(F.count("*").alias("complaints")),
        "company_scorecard": s.groupBy("company").agg(
            F.count("*").alias("complaints"),
            F.avg(F.col("timely_response").cast("int")).alias("timely_rate"),
            F.avg(F.col("company_response").isin(RELIEF).cast("int")).alias("relief_rate"),
        ),
        "state_year": s.where("state is not null").groupBy("state", F.year("date_received").alias("year")).agg(F.count("*").alias("complaints")),
    }
    for name, df in out.items():
        df.write.format("delta").mode("overwrite").save(f"{gold_dir}/{name}")
