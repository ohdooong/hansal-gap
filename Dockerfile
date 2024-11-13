FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update
RUN apt-get install -y gcc python3-dev openjdk-11-jdk wget
RUN apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow
# Define Airflow constraints URL
ENV AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"

# Install Python packages using constraints to ensure compatibility
RUN pip install --no-cache-dir \
    apache-airflow \
    apache-airflow-providers-apache-spark \
    pyspark \
    elasticsearch \
    --constraint "${AIRFLOW_CONSTRAINTS_URL}"


    # 초기화 스크립트 복사
# COPY init.sh /init.sh

# ENTRYPOINT ["/init.sh"]