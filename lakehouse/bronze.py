from delta.tables import DeltaTable
from pyspark.sql import functions as F

RENAME = {
    "Date received": "date_received", "Product": "product", "Sub-product": "sub_product",
    "Issue": "issue", "Sub-issue": "sub_issue", "Company public response": "company_public_response",
    "Company": "company", "State": "state", "ZIP code": "zip_code", "Tags": "tags",
    "Submitted via": "submitted_via", "Date sent to company": "date_sent_to_company",
    "Company response to consumer": "company_response", "Timely response?": "timely_response",
    "Complaint ID": "complaint_id",
}


def load(spark, csv_path, bronze_path, batch_id):
    if DeltaTable.isDeltaTable(spark, bronze_path):
        if spark.read.format("delta").load(bronze_path).where(F.col("_batch_id") == batch_id).limit(1).count():
            return 0
    df = (
        spark.read.option("header", True).option("escape", '"').csv(csv_path)
        .select(*[F.col(f"`{o}`").alias(n) for o, n in RENAME.items()])
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(csv_path))
    )
    df.write.format("delta").mode("append").partitionBy("_batch_id").save(bronze_path)
    return spark.read.format("delta").load(bronze_path).where(F.col("_batch_id") == batch_id).count()


def read_batch(spark, bronze_path, batch_id=None):
    b = spark.read.format("delta").load(bronze_path)
    if batch_id is None:
        batch_id = b.groupBy("_batch_id").agg(F.max("_ingested_at").alias("t")).orderBy(F.desc("t")).first()["_batch_id"]
    return b.where(F.col("_batch_id") == batch_id)
