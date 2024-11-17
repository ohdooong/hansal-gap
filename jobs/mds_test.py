import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
        .appName("saveToPostgre")
        .master("local")
        .config("spark.jars","/opt/bitnami/spark/jars/postgresql-42.6.0.jar")
        .getOrCreate()
)

# jdbc_url = "jdbc:postgresql://192.168.245.143:5432/hsgdb"
# connection_properties = {
#     "user": "hsgdb",
#     "password": "hsgdb00!",
#     "driver": "org.postgresql.Driver"
# }
# table_name = "mds_security_logs_202403"

agg_count = 0;
for x in ['1','2','3','4','5','6'] : 
    filePath = f"/opt/bitnami/spark/data/mds_security_logs_202403_0{x}.csv"
    # filePath = "/opt/bitnami/spark/data/epp_security_logs_202401.csv"


    df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(filePath)

    agg_count += df.count()









spark.stop()
