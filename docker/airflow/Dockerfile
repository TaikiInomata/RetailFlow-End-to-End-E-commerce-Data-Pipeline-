FROM apache/airflow:2.9.2

# Cài đặt Java (JRE) để chạy PySpark
USER root
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Chuyển về user airflow và cài thư viện Python
USER airflow
RUN pip install --no-cache-dir \
    requests \
    boto3 \
    pyspark==3.5.1 \
    delta-spark==3.1.0
