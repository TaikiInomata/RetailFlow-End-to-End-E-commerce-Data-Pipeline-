import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType
)
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, from_json, to_timestamp, date_format

# --- 0. LOAD MÔI TRƯỜNG ---
load_dotenv(Path(__file__).parent.parent.parent / '.env')

# Fix lỗi Spark trên Windows: HADOOP_HOME and hadoop.home.dir are unset
# Bỏ qua khi chạy trong Docker (Linux — winutils.exe không cần thiết)
if not os.getenv("RUNNING_IN_DOCKER"):
    os.environ["HADOOP_HOME"] = str(Path(__file__).parent.parent.parent / "hadoop")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

# --- 1. CẤU HÌNH ---
# Đọc MINIO_ENDPOINT từ .env — quan trọng khi chuyển sang Docker (minio:9000 thay vì localhost)
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
KAFKA_BROKER     = os.getenv("KAFKA_BROKER")
TOPIC_NAME       = os.getenv("CLICKSTREAM_TOPIC")
S3_BRONZE_PATH   = "s3a://bronze-zone/clickstream"
CHECKPOINT_PATH  = "s3a://bronze-zone/checkpoints/clickstream"

def create_spark_session():
    """Khởi tạo và cấu hình SparkSession với đầy đủ các cấu hình S3A cho MinIO."""
    logger.info("Khởi tạo SparkSession và nạp JAR packages (Kafka, Hadoop-AWS)...")
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
        "org.apache.hadoop:hadoop-aws:3.4.2"
    ]
    spark = SparkSession.builder \
        .appName("BronzeClickstreamStreaming") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .config("spark.hadoop.fs.s3a.buffer.dir", str(Path(__file__).parent.parent.parent / "hadoop" / "tmp" / "s3a").replace("\\", "/")) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ SparkSession khởi tạo thành công!")
    return spark

def get_clickstream_schema():
    """
    Định nghĩa Schema tương ứng với payload của clickstream_bot.py.

    - event_timestamp được parse từ chuỗi ISO 8601 (StringType) thay vì TimestampType
      để tránh lỗi null khi Spark gặp timezone offset dạng +00:00 thay vì Z.
      Việc chuyển đổi sang Timestamp sẽ được thực hiện bằng to_timestamp() sau khi parse JSON.
    - Các trường linh hoạt (item_id, duration_ms, ...) được khai báo nullable=True
      vì chúng chỉ xuất hiện trong một số loại event nhất định.
    """
    return StructType([
        StructField("event_id",          StringType(),  True),
        StructField("user_id",           IntegerType(), True),
        StructField("session_id",        StringType(),  True),
        StructField("event_type",        StringType(),  True),
        StructField("device",            StringType(),  True),
        StructField("referrer",          StringType(),  True),
        StructField("ip_address",        StringType(),  True),
        StructField("user_agent",        StringType(),  True),
        StructField("event_timestamp",   StringType(),  True),
        # Các trường linh hoạt tùy loại event
        StructField("item_id",           StringType(),  True),
        StructField("page_url",          StringType(),  True),
        StructField("duration_ms",       IntegerType(), True),
        StructField("scroll_depth_pct",  IntegerType(), True),
        StructField("search_query",      StringType(),  True),
        StructField("result_count",      IntegerType(), True),
        StructField("click_target",      StringType(),  True),
        StructField("quantity",          IntegerType(), True),
        StructField("cart_total",        FloatType(),   True),
        StructField("step",              StringType(),  True),
        StructField("pseudo_order_id",   StringType(),  True),
        StructField("purchase_amount",   FloatType(),   True),
    ])

if __name__ == "__main__":
    spark = create_spark_session()

    # --- KHỐI 2: Đọc dữ liệu Streaming từ Kafka ---
    logger.info(f"Bắt đầu đọc stream từ Kafka: {KAFKA_BROKER}, topic: {TOPIC_NAME}")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Data từ Kafka nằm ở cột "value" định dạng binary -> cast sang chuỗi JSON
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string")

    # Parse JSON bằng Schema
    schema = get_clickstream_schema()
    parsed_df = json_df.select(from_json(col("json_string"), schema).alias("data")).select("data.*")

    # Chuẩn hóa event_timestamp: ISO 8601 string (có thể có +00:00 hoặc Z) → TimestampType
    # Thêm cột event_date (dạng "yyyy-MM-dd") để dùng làm partition key
    transformed_df = parsed_df \
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
        .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))

    # --- KHỐI 3: Ghi dữ liệu xuống MinIO (S3) theo định dạng Parquet ---
    # Phân vùng theo event_date và event_type để tối ưu hiệu năng khi đọc downstream.
    # Cấu trúc thư mục: bronze-zone/clickstream/event_date=.../event_type=.../
    logger.info(f"Bắt đầu ghi stream xuống MinIO: {S3_BRONZE_PATH}")
    query = transformed_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", S3_BRONZE_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("event_date", "event_type") \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("Streaming job đang chạy... (Nhấn Ctrl+C để dừng)")
    query.awaitTermination()