import os
from pyspark.sql import SparkSession

# Connect to the exact same local warehouse
warehouse_path = os.path.join(os.getcwd(), "warehouse")
spark = SparkSession.builder \
    .appName("VerifyData") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", warehouse_path) \
    .config("spark.sql.defaultCatalog", "local") \
    .getOrCreate()

# Suppress messy logs
spark.sparkContext.setLogLevel("ERROR")

print("\n=== CURRENT DATA IN db.occ_test ===")
# Query the table to see the final result
spark.sql("SELECT * FROM db.occ_test").show()
print("=====================================\n")

spark.stop()