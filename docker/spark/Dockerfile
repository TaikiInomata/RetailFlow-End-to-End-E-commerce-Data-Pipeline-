# ============================================================
# Spark Streaming Image — retailflow-spark
# Base: apache/spark:3.5.4 (official Apache image, Java + Hadoop trên Linux)
# Role: Chạy PySpark job clickstream_streaming.py liên tục
# ============================================================
FROM apache/spark:3.5.4

USER root
WORKDIR /app

# Cài Python dependencies cho PySpark job
# Lưu ý: apache/spark image đã có pyspark — chỉ cài thêm boto3 và python-dotenv
COPY requirements-spark.txt .
RUN pip install --no-cache-dir -r requirements-spark.txt

# Copy spark-jobs và scripts/utils (MinioClientFactory)
COPY spark-jobs/ ./spark-jobs/
COPY scripts/utils/ ./scripts/utils/

# Entrypoint: chạy Spark Streaming job bằng python3 (apache/spark image dùng python3)
CMD ["python3", "spark-jobs/bronze/clickstream_streaming.py"]
