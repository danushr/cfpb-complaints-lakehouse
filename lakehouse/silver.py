from delta.tables import DeltaTable
from pyspark.sql import functions as F

from lakehouse.bronze import RENAME

TEXT = ["product", "sub_product", "issue", "sub_issue", "company_public_response", "company", "state",
        "zip_code", "tags", "submitted_via", "company_response"]
HASHED = TEXT + ["date_received", "date_sent_to_company", "timely_response"]


def _blank_to_null(c):
    return F.nullif(F.trim(F.col(c)), F.lit(""))


def clean(bronze):
    df = bronze.select(*[_blank_to_null(n).alias(n) for n in RENAME.values()])
    df = (
        df.withColumn("complaint_id_raw", F.col("complaint_id"))
        .withColumn("complaint_id", F.col("complaint_id_raw").try_cast("long"))
        .withColumn("date_received_raw", F.col("date_received"))
        .withColumn("date_received", F.try_to_timestamp(F.col("date_received"), F.lit("yyyy-MM-dd")).cast("date"))
        .withColumn("date_sent_to_company", F.try_to_timestamp(F.col("date_sent_to_company"), F.lit("yyyy-MM-dd")).cast("date"))
        .withColumn("timely_raw", F.col("timely_response"))
        .withColumn("timely_response", F.when(F.col("timely_raw") == "Yes", True).when(F.col("timely_raw") == "No", False))
    )
    reason = (
        F.when(F.col("complaint_id").isNull(), "complaint_id_not_numeric")
        .when(F.col("date_received").isNull(), "date_received_invalid")
        .when(F.col("date_received") > F.current_date(), "date_received_in_future")
        .when(F.col("date_sent_to_company") < F.col("date_received"), "sent_before_received")
        .when(F.col("timely_raw").isNotNull() & F.col("timely_response").isNull(), "timely_response_unknown")
    )
    df = df.withColumn("reject_reason", reason)
    rejected = df.where("reject_reason is not null").drop("timely_raw", "date_received_raw")
    valid = (
        df.where("reject_reason is null")
        .drop("complaint_id_raw", "date_received_raw", "timely_raw", "reject_reason")
        .dropDuplicates(["complaint_id"])
        .withColumn("row_hash", F.sha2(F.concat_ws("\x01", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in HASHED]), 256))
    )
    return valid, rejected


def upsert(spark, path, df):
    if not DeltaTable.isDeltaTable(spark, path):
        n = df.count()
        df.write.format("delta").save(path)
        return {"inserted": n, "updated": 0}
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), "t.complaint_id = s.complaint_id")
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )
    m = DeltaTable.forPath(spark, path).history(1).collect()[0].operationMetrics
    return {"inserted": int(m["numTargetRowsInserted"]), "updated": int(m["numTargetRowsUpdated"])}


def quarantine(spark, path, rejected, batch_id):
    (
        rejected.withColumn("_batch_id", F.lit(batch_id))
        .write.format("delta").mode("overwrite").partitionBy("_batch_id")
        .option("replaceWhere", f"_batch_id = '{batch_id}'").save(path)
    )
