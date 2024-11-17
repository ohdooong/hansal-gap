import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from concurrent.futures import ThreadPoolExecutor


def read_table(table_name) :
    return spark.read.jdbc(
    url=jdbc_url,
    table=table_name,
    properties=connection_properties,
    column="id",  # 파티션 기준 열
    lowerBound=1,  # 최소값
    upperBound=1000000,  # 최대값
    numPartitions=10  # 파티션 수
)

def agg_count(df, cond, col_name) :
    return df.withColumn(col_name,F.when(cond, 1).otherwise(0)).agg(F.sum(col_name.alias(col_name)))



spark = (
    SparkSession.builder
        .appName("readFromPostgreSql")
        .master("local")
        .config("spark.jars","/opt/bitnami/spark/jars/postgresql-42.6.0.jar")
        .getOrCreate()
)

jdbc_url = "jdbc:postgresql://192.168.245.143:5432/hsgdb"
connection_properties = {
    "user": "hsgdb",
    "password": "hsgdb00!",
    "driver": "org.postgresql.Driver"
}

table_name = ['mds_security_logs_202403','mds_security_logs_202404','mds_security_logs_202405','mds_security_logs_202406','mds_security_logs_202407','mds_security_logs_202408']
insert_table_name = "mds_timeseries"

# df.show()
# with ThreadPoolExecutor() as excutor :
#     dataframes = list(excutor.map(read_table, table_name))


# for df in dataframes:
#     df.show()

check_severity = ['High[U]', 'High[K]', 'Low[U]','Low[K]','Medium[U]','Medium[K]']

for table in table_name :
    df = read_table(table)
    df = df.withColumn("filtered_date",F.date_format(F.col("log_received_timestamp"), "yyyy-MM-dd"))
    df = df.withColumn("dummy_count",F.lit(1));
    df = df.withColumn("danger_severity_count",F.when(F.col("severity").isin(check_severity), 1).otherwise(0))
    new_df = df.groupby("filtered_date") \
                        .agg(
                            F.sum("dummy_count").alias("total_severity_sum"),
                            F.sum("danger_severity_count").alias("danger_severity_sum")
                        ) \
                        .withColumnRenamed("filtered_date", "log_received_date")  # 그룹화 키 컬럼명 변경  

    new_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", insert_table_name) \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .mode("append") \
        .save()

spark.stop();