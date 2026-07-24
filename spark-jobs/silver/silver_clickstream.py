# stdlib
import sys
import logging
from pathlib import Path

# Gắn đường dẫn TRƯỚC tất cả local import
sys.path.insert(0, str(Path(__file__).parent.parent / 'utils'))  # Cho spark_builder
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common'))  # Cho minio_client

# third-party
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, when, lit
# pyrefly: ignore [missing-import]
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, FloatType
)
# pyrefly: ignore [missing-import]
from pyspark.sql.utils import AnalysisException

# local
# pyrefly: ignore [missing-import]
from spark_builder import get_spark_session
# pyrefly: ignore [missing-import]
from minio_client import MinioClientFactory

# Cấu hình Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s")
logger = logging.getLogger("Silver_Clickstream")

def process_silver_clickstream():
    spark = get_spark_session("Silver_Clickstream")
    
    # 1. Khai báo đường dẫn
    # Tầng Bronze của Clickstream là các file Parquet đã được transform nhẹ (chuyển JSON -> Parquet, to_timestamp)
    bronze_path = "s3a://bronze-zone/clickstream"
    silver_path = "s3a://silver-zone/clickstream"
    checkpoint_path = "s3a://silver-zone/_checkpoints/clickstream"
    
    # 0. ĐẢM BẢO BUCKET ĐÍCH TỒN TẠI
    MinioClientFactory().ensure_bucket_exists("silver-zone")
    
    # Mặc dù Parquet có Schema Inference, Streaming vẫn khuyến khích ép kiểu tĩnh
    # Lưu ý: Lấy y hệt cấu trúc schema được ghi bởi script bronze/clickstream_streaming.py
    clickstream_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", IntegerType(), True),  # Bronze ghi INT32 (IntegerType), không phải bigint
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("device", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
        
        # Bronze script đã ép kiểu timestamp và thêm event_date
        StructField("event_timestamp", TimestampType(), True),
        StructField("event_date", StringType(), True),
        
        # Các cột tùy chọn
        StructField("item_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("scroll_depth_pct", IntegerType(), True),
        StructField("search_query", StringType(), True),
        StructField("result_count", IntegerType(), True),
        StructField("click_target", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("cart_total", FloatType(), True),
        StructField("step", StringType(), True),
        StructField("pseudo_order_id", StringType(), True),
        StructField("purchase_amount", FloatType(), True)
    ])
    
    logger.info(f"🌊 Đang khởi động luồng Spark Streaming đọc dữ liệu Parquet từ {bronze_path}...")
    
    # Fix: Thêm try/except AnalysisException để xử lý trường hợp bucket Bronze chưa có dữ liệu
    try:
        # Đọc luồng dữ liệu bằng .parquet thay vì .json
        # BẮT BUỘC: Thêm option("basePath") để Spark Streaming hiểu được cấu trúc thư mục Partition (event_date=.../event_type=...)
        df_stream = spark.readStream \
            .schema(clickstream_schema) \
            .option("maxFilesPerTrigger", 100) \
            .option("basePath", bronze_path) \
            .parquet(bronze_path)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            logger.warning("⚠️ Chưa có dữ liệu Clickstream ở Bronze Zone. Bỏ qua chạy Job.")
            return
        raise e
        
    # 2. BIẾN ĐỔI (TRANSFORM)
    # Fix: Watermark PHẢI được đặt NGAY SAU readStream, TRƯỚC mọi transform
    # Vì Watermark phải được Spark biết trước khi lên kế hoạch xử lý luồng
    df_with_watermark = df_stream.withWatermark("event_timestamp", "10 minutes")
    
    # Lưu ý: Bronze script đã xử lý to_timestamp nên không cần to_timestamp ở đây nữa
    df_silver = df_with_watermark.withColumn(
        # Điền khuyết: Nếu không phải hành động thanh toán (checkout), số tiền BẮT BUỘC bằng Null
        "purchase_amount", 
        when(col("event_type") == "checkout", col("purchase_amount")).otherwise(lit(None))
    )
    
    # 3. TẢI DỮ LIỆU (LOAD): Ghi liên tục xuống Delta Table
    logger.info(f"💾 Đang đẩy luồng dữ liệu xuống Delta Table tại: {silver_path}")
    
    query = df_silver.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(availableNow=True) \
        .start(silver_path)
        
    query.awaitTermination()
    
    logger.info("✅ Đã xử lý xong batch Clickstream hiện tại và lưu vào Silver Zone.")

if __name__ == "__main__":
    process_silver_clickstream()