import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = (
    SparkSession.builder
        .appName("saveToPostgre")
        .master("local")
        .config("spark.jars","/opt/bitnami/spark/jars/postgresql-42.6.0.jar")
        .getOrCreate()
)


table_name = "mds_security_logs_202408"

# for i in range(1,8) :
  # filePath = f"/opt/bitnami/spark/data/mds_security_logs_202404_{i}.csv"
  # filePath = f"/opt/bitnami/spark/data/test.csv"
  # filePath = "/opt/bitnami/spark/data/epp_security_logs_202401.csv"
filePath = "/opt/bitnami/spark/data/*.csv"

# schema = StructType([
#     StructField("event_id", StringType(), True),
#     StructField("verification_status", StringType(), True),
#     StructField("severity", StringType(), True),
#     StructField("detection_target", StringType(), True),
#     StructField("md5", StringType(), True),
#     StructField("file_type", StringType(), True),
#     StructField("file_size", StringType(), True),
#     StructField("attack_phase", StringType(), True),
#     StructField("diagnosis_name", StringType(), True),
#     StructField("custom_rule_diagnosis", StringType(), True),
#     StructField("analysis_info_source", StringType(), True),
#     StructField("antivirus_diagnosis_details", StringType(), True),
#     StructField("attack_target", StringType(), True),
#     StructField("collection_path", StringType(), True),
#     StructField("detailed_path", StringType(), True),
#     StructField("session_id", StringType(), True),
#     StructField("source_port", StringType(), True),
#     StructField("destination_ip", StringType(), True),
#     StructField("destination_port", StringType(), True),
#     StructField("client_ip", StringType(), True),
#     StructField("client_port", StringType(), True),
#     StructField("server_ip", StringType(), True),
#     StructField("server_port", StringType(), True),
#     StructField("attacker_location", StringType(), True),
#     StructField("mail_server_ip", StringType(), True),
#     StructField("direction", StringType(), True),
#     StructField("status", StringType(), True),
#     StructField("message_id", StringType(), True),
#     StructField("response_status", StringType(), True),
#     StructField("analysis_duration_seconds", StringType(), True),
#     StructField("is_vm_used", StringType(), True),
#     StructField("detection_timestamp", TimestampType(), True),
#     StructField("log_received_timestamp", TimestampType(), True),
#     StructField("inflow_timestamp", TimestampType(), True),
#     StructField("latest_response_time", TimestampType(), True)
# ])

df = spark.read \
  .format("csv") \
  .option("header", "true") \
  .option("inferSchema","true") \
  .option("nullValue","") \
  .option("mode", "DROPMALFORMED") \
  .load(filePath)

df.printSchema();
df.show();

# df = spark.read.csv("file:///D:/DF/data-engineering/data/*.csv", header=True, inferSchema=True)



# ==========================================================DB 삽입
# df = df.drop("delete_column1","delete_column2")
# 배치 사이즈와 파티션 수 설정
batch_size = 10000        # 한 배치당 레코드 수
num_partitions = 10      # 동시에 처리할 파티션 수

# 데이터프레임의 파티션 수 조정 (병렬 처리 최적화)
df = df.repartition(num_partitions)
# PostgreSQL에 DataFrame 쓰기
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", connection_properties["driver"]) \
    .option("batchsize", batch_size) \
    .mode("append") \
    .save()




# df.write \
#     .jdbc(url=jdbc_url, table=table_name, mode='append', properties=connection_properties)


spark.stop()
