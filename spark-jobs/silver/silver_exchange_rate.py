# stdlib
import os
import sys
import logging
from pathlib import Path

# Gắn đường dẫn TRƯỚC tất cả local import — loại bỏ try/except ImportError anti-pattern
sys.path.insert(0, str(Path(__file__).parent.parent / 'utils'))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common'))

# third-party
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, explode, to_date, from_json, to_json
# pyrefly: ignore [missing-import]
from pyspark.sql.types import MapType, StringType, DoubleType
# pyrefly: ignore [missing-import]
from pyspark.sql.utils import AnalysisException

# local
# pyrefly: ignore [missing-import]
from spark_builder import get_spark_session
# pyrefly: ignore [missing-import]
from minio_client import MinioClientFactory

# Cấu hình Logging chuẩn Production
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Silver_ExchangeRate")

def process_silver_exchange_rates():
    # 1. Khởi tạo Spark Session
    logger.info("Đang khởi tạo Spark Session...")
    spark = get_spark_session(app_name="Silver_Exchange_Rate_Batch")
    
    # Kích hoạt Dynamic Partition Overwrite cho các thao tác Write (Hỗ trợ Parquet & Delta >= 2.0)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # 2. Định nghĩa đường dẫn
    bronze_path = "s3a://bronze-zone/api_exchange_rates/*/*/*/*.json"
    
    # Ở Delta Lake, silver_path chỉ cần trỏ tới thư mục gốc,
    # Spark sẽ tự động chia các folder con dạng exchange_date=YYYY-MM-DD ở bên trong nhờ lệnh partitionBy()
    silver_bucket = "silver-zone"
    silver_path = f"s3a://{silver_bucket}/exchange_rates"
    
    # 0. ĐẢM BẢO BUCKET ĐÍCH TỒN TẠI (Sử dụng Utils dùng chung)
    MinioClientFactory().ensure_bucket_exists("silver-zone")
            
    logger.info(f"Đang đọc dữ liệu thô từ: {bronze_path}")
    
    try:
        try:
            # Dữ liệu từ API được lưu với indent=2 (nhiều dòng), bắt buộc phải có multiLine=True
            df_raw = spark.read \
                .option("multiLine", "true") \
                .option("mode", "DROPMALFORMED") \
                .json(bronze_path)
        except AnalysisException as e:
            if "Path does not exist" in str(e):
                logger.warning("Thư mục chưa tồn tại ở Bronze Zone. Bỏ qua chạy Job.")
                sys.exit(0)
            raise e
            
        # Fix: isEmpty() hiệu quả hơn limit(1).count() — không trigger full shuffle
        if df_raw.isEmpty():
            logger.warning("Không có dữ liệu mới ở Bronze Zone. Bỏ qua chạy Job.")
            sys.exit(0)
            
        # Fix: Dùng str(schema) thay vì _jdf (Private JVM API)
        logger.info(f"Schema dữ liệu gốc (Bronze):\n{str(df_raw.schema)}")

        # 3. BIẾN ĐỔI DỮ LIỆU (TRANSFORMATION)
        # Ép kiểu cấu trúc lồng nhau (Struct) thành Map(Key-Value) để kháng lỗi khi API đổi Schema
        map_type = MapType(StringType(), DoubleType())
        
        df_mapped = df_raw.withColumn(
            "rates_map", 
            from_json(to_json(col("rates")), map_type)
        )
        
        # Flatten dữ liệu, loại bỏ ép kiểu thừa vì rates_map đã là DoubleType
        df_silver = df_mapped.select(
            to_date(col("_ingested_at")).alias("exchange_date"), # Dùng _ingested_at (chính xác với thực tế)
            col("base_code").alias("base_currency"),             # Dùng base_code (chính xác với thực tế)
            explode(col("rates_map")).alias("target_currency", "exchange_rate")
        )
        
        logger.info("Biến đổi dữ liệu thành công. Chuẩn bị ghi xuống Delta Table...")
        
        # 4. GHI DỮ LIỆU XUỐNG MINIO (LOAD)
        # Lũy đẳng: Bật mode "overwrite" kết hợp partitionOverwriteMode=dynamic đã set ở trên.
        # Delta Lake sẽ tự động đối chiếu các partition có trong df_silver và chỉ ghi đè những partition đó.
        logger.info(f"Đang ghi dữ liệu (Dynamic Overwrite) xuống: {silver_path}")
        
        # Fix: cache() để count() và write dùng chung plan, tránh Spark tính toán 2 lần
        df_silver.cache()
        record_count = df_silver.count()
        
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("exchange_date") \
            .save(silver_path)
        
        df_silver.unpersist()
            
        logger.info(f"✅ Đã hoàn tất xử lý Tầng Silver cho Exchange Rates! Tổng số bản ghi ghi được: {record_count}")
        
    except Exception as e:
        logger.error(f"❌ Có lỗi xảy ra trong quá trình xử lý Spark: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    process_silver_exchange_rates()