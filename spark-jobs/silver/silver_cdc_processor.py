import os
import sys
import logging
from pathlib import Path
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, row_number, expr
# pyrefly: ignore [missing-import]
from pyspark.sql.window import Window
# pyrefly: ignore [missing-import]
from pyspark.sql.utils import AnalysisException
# pyrefly: ignore [missing-import]
from delta.tables import DeltaTable

# Gắn đường dẫn để import các thư mục utils
sys.path.insert(0, str(Path(__file__).parent.parent / 'utils'))  # Cho spark_builder
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common'))  # Cho minio_client

# pyrefly: ignore [missing-import]
from spark_builder import get_spark_session
# pyrefly: ignore [missing-import]
from minio_client import MinioClientFactory

import argparse
# pyrefly: ignore [missing-import]
from pyspark.sql.types import StructType

# Cấu hình Logger mặc định (Sẽ cập nhật tên logger động bên trong hàm)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s")

def process_silver_cdc(table_name, pk_col="id"):
    # Khởi tạo Logger và Spark động theo tên bảng
    job_name = f"Silver_CDC_{table_name.capitalize()}"
    logger = logging.getLogger(job_name)
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
        
    if df_raw.limit(1).count() == 0:
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
        *after_cols
    )

    # 3. KHỬ TRÙNG LẶP (DEDUPLICATION)
    # Sử dụng ts_ms (thời gian ghi log của Debezium) thay vì updated_at để chính xác tuyệt đối
    window_spec = Window.partitionBy(pk_col).orderBy(col("ts_ms").desc())
    
    df_latest = df_flattened.withColumn("rn", row_number().over(window_spec)) \
                            .filter(col("rn") == 1) \
                            .drop("rn")
    
    logger.info(f"✨ Schema sau khi làm sạch:\n{df_latest._jdf.schema().treeString()}")

    # 4. LOGIC UPSERT VỚI DELTA LAKE (MERGE INTO)
    logger.info(f"💾 Đang đồng bộ trạng thái xuống: {silver_path}")
    
    if DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("🔄 Bảng Silver đã tồn tại -> Thực hiện MERGE INTO (Upsert)...")
        delta_table = DeltaTable.forPath(spark, silver_path)
        
        # Merge logic bao gồm Xử lý Delete (Hard Delete)
        delta_table.alias("target") \
            .merge(
                df_latest.alias("source"),
                f"target.{pk_col} = source.{pk_col}"
            ) \
            .whenMatchedDelete(condition="source.op = 'd'") \
            .whenMatchedUpdateAll(condition="source.op != 'd'") \
            .whenNotMatchedInsertAll(condition="source.op != 'd'") \
            .execute()
    else:
        logger.info(f"🚀 Bảng Silver chưa tồn tại -> Khởi tạo lần đầu (Initial Load) cho {table_name}...")
        # Lọc bỏ các bản ghi Delete trong lần chạy đầu (vì bảng đích chưa có gì để xóa)
        df_initial = df_latest.filter(col("op") != 'd')
        df_initial.write.format("delta").mode("overwrite").save(silver_path)
        
    logger.info(f"✅ Đã hoàn tất luồng CDC cho {table_name}! Xử lý thành công {df_latest.count()} bản ghi cập nhật.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Xử lý luồng CDC từ Bronze lên Silver")
    parser.add_argument("--table_name", required=True, help="Tên bảng CDC cần xử lý (VD: orders, products, users)")
    parser.add_argument("--primary_key", default="id", help="Khóa chính của bảng (Mặc định: id)")
    args = parser.parse_args()
    
    process_silver_cdc(args.table_name, args.primary_key)