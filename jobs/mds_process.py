import sys
from pyspark.sql import SparkSession
import pandas as pd
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
        .appName("excelToSpark")
        .master("local")
        .getOrCreate()
)

filePath = "/opt/bitnami/spark/data/mds_security_logs_202403_01.csv"
# filePath = "/opt/bitnami/spark/data/epp_security_logs_202401.csv"


df = spark.read \
  .format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load(filePath)


df_with_detection_ymd = df.withColumn("detection_ymd", F.to_date(F.col("detection_timestamp")));

df_with_detection_ymd.printSchema();
result_df = df_with_detection_ymd.groupBy("detection_ymd").count()


result_df.show()


spark.stop()
