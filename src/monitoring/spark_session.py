import os
from pyspark.sql import SparkSession

_spark = None

def get_shared_spark():
    global _spark
    if _spark is not None:
        return _spark

    _spark = SparkSession.builder \
        .appName("Iceberg-Shared-Session") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", f"{os.getcwd()}/warehouse") \
        .config("spark.sql.catalog.local.cache-enabled", "false") \
        .getOrCreate()

    _spark.sparkContext.setLogLevel("ERROR")
    return _spark