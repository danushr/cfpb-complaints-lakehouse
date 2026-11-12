import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app="cfpb-lakehouse"):
    b = (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(b).getOrCreate()
