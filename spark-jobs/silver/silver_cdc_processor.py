# stdlib
import argparse
import sys
from pathlib import Path

# Gắn đường dẫn TRƯỚC tất cả local import
sys.path.insert(0, str(Path(__file__).parent.parent / 'utils'))  # Cho spark_builder
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common'))  # Cho minio_client

# third-party
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, row_number, expr
# pyrefly: ignore [missing-import]
from pyspark.sql.window import Window
# pyrefly: ignore [missing-import]
from pyspark.sql.types import StructType
# pyrefly: ignore [missing-import]
from pyspark.sql.utils import AnalysisException
# pyrefly: ignore [missing-import]
from delta.tables import DeltaTable

# local
# pyrefly: ignore [missing-import]
from spark_builder import get_spark_session
# pyrefly: ignore [missing-import]
from minio_client import MinioClientFactory
# pyrefly: ignore [missing-import]
from logger_utils import get_logger, timeit

@timeit(logger_name="Silver_CDC")
def process_silver_cdc(table_name, pk_col="id"):
    # Khởi tạo Logger và Spark động theo tên bảng
    job_name = f"Silver_CDC_{table_name.capitalize()}"
    logger = get_logger(job_name)
    spark = get_spark_session(job_name)
    
    # Đường dẫn tự động nội suy theo tên bảng
    bronze_path = f"s3a://bronze-zone/cdc_postgres/retailflow_server_v2.public.{table_name}/*/*/*/*.json"
    silver_path = f"s3a://silver-zone/cdc_data/{table_name}"
    
    # 0. ĐẢM BẢO BUCKET ĐÍCH TỒN TẠI (Sử dụng Utils dùng chung)
    MinioClientFactory().ensure_bucket_exists("silver-zone")
            
    logger.info(f"📥 Đang đọc dữ liệu CDC thô cho bảng '{table_name}' từ: {bronze_path}")
    
    # 1. ĐỌC DỮ LIỆU & BẮT LỖI AN TOÀN
    try:
        df_raw = spark.read.option("mode", "DROPMALFORMED").json(bronze_path)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            logger.warning("Thư mục chưa tồn tại ở Bronze Zone. Bỏ qua chạy Job.")
            sys.exit(0)
        raise e
        
    # Fix: isEmpty() (Spark 3.3+) hiệu quả hơn limit(1).count() — không trigger full shuffle
    if df_raw.isEmpty():
        logger.warning(f"⚠️ Không có dữ liệu CDC {table_name} mới. Kết thúc.")
        sys.exit(0)

    logger.info("Bắt đầu xử lý giải nén (Flatten) và Khử trùng lặp (Deduplication)...")
    
    # 2. GIẢI NÉN (FLATTEN) SCHEMA DEBEZIUM
    # Lấy danh sách các cột nằm trong payload 'after', ngoại trừ khóa chính (pk_col) để tự tạo tay
    after_cols = [col(f"after.{c}") for c in df_raw.select("after.*").columns if c != pk_col]
    
    # Khắc phục Spark Schema Inference: Kiểm tra xem 'before' có phải là StructType không.
    # Nếu không (do 100% bản ghi là Insert -> 'before' toàn null -> Spark gán StringType), ta chỉ lấy 'after'.
    is_before_struct = "before" in df_raw.columns and isinstance(df_raw.schema["before"].dataType, StructType)
    
    if is_before_struct:
        pk_expr = expr(f"COALESCE(after.{pk_col}, before.{pk_col})")
    else:
        pk_expr = expr(f"after.{pk_col}")
    
    df_flattened = df_raw.select(
        col("op"),
        col("ts_ms"),
        pk_expr.alias(pk_col),
        *after_cols,
        expr("op = 'd'").alias("is_deleted")
    )

    # 3. KHỬ TRÙNG LẶP (DEDUPLICATION)
    # Sử dụng ts_ms (thời gian ghi log của Debezium) thay vì updated_at để chính xác tuyệt đối
    window_spec = Window.partitionBy(pk_col).orderBy(col("ts_ms").desc())
    
    df_latest = df_flattened.withColumn("rn", row_number().over(window_spec)) \
                            .filter(col("rn") == 1) \
                            .drop("rn")
    
    # Fix: Dùng str(schema) thay vì _jdf (Private JVM API — dễ bị xóa khi nâng cấp Spark)
    logger.info(f"✨ Schema sau khi làm sạch:\n{str(df_latest.schema)}")

    # 4. LOGIC UPSERT VỚI DELTA LAKE (MERGE INTO)
    logger.info(f"💾 Đang đồng bộ trạng thái xuống: {silver_path}")
    
    # Chuẩn Senior: Cache df_latest vì nó được dùng cho cả việc Write (Merge/Save) và Count
    df_latest.cache()
    
    if DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("🔄 Bảng Silver đã tồn tại -> Thực hiện MERGE INTO (Upsert)...")
        delta_table = DeltaTable.forPath(spark, silver_path)
        
        # Merge logic bao gồm Xử lý Delete (Soft Delete)
        delta_table.alias("target") \
            .merge(
                df_latest.alias("source"),
                f"target.{pk_col} = source.{pk_col}"
            ) \
            .whenMatchedUpdate( # SOFT DELETE: Chỉ cập nhật cờ xóa và thời gian xóa, giữ nguyên dữ liệu lịch sử
                condition="source.op = 'd'",
                set={
                    "is_deleted": "true",
                    "ts_ms": "source.ts_ms"
                }
            ) \
            .whenMatchedUpdateAll(condition="source.op != 'd'") \
            .whenNotMatchedInsertAll(condition="source.op != 'd'") \
            .execute()
    else:
        logger.info(f"🚀 Bảng Silver chưa tồn tại -> Khởi tạo lần đầu (Initial Load) cho {table_name}...")
        # Lọc bỏ các bản ghi Delete trong lần chạy đầu (vì bảng đích chưa có gì để xóa)
        df_initial = df_latest.filter(col("op") != 'd')
        df_initial.write.format("delta").mode("overwrite").save(silver_path)
        
    record_count = df_latest.count()
    df_latest.unpersist()
    
    logger.info(f"✅ Đã hoàn tất luồng CDC cho {table_name}! Xử lý thành công {record_count} bản ghi cập nhật.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Xử lý luồng CDC từ Bronze lên Silver")
    parser.add_argument("--table_name", required=True, help="Tên bảng CDC cần xử lý (VD: orders, products, users)")
    parser.add_argument("--primary_key", default="id", help="Khóa chính của bảng (Mặc định: id)")
    args = parser.parse_args()
    
    process_silver_cdc(args.table_name, args.primary_key)