import os
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

# Load biến môi trường (hỗ trợ cho lúc test dưới máy Local)
load_dotenv()

def get_spark_session(app_name="CartStream_Silver_Layer"):
    """
    Khởi tạo SparkSession dùng chung được trang bị tận răng:
    - Delta Lake Core (để đọc/ghi định dạng Delta)
    - AWS SDK & Hadoop S3A (để giao tiếp với MinIO)
    """
    
    # Lấy thông tin từ biến môi trường — Fail-Fast nếu thiếu biến quan trọng
    _required_env_vars = {
        "MINIO_ENDPOINT":      os.getenv("MINIO_ENDPOINT"),
        "MINIO_ROOT_USER":     os.getenv("MINIO_ROOT_USER"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
    }
    _missing = [k for k, v in _required_env_vars.items() if not v]
    if _missing:
        raise EnvironmentError(
            f"❌ Thiếu các biến môi trường bắt buộc: {', '.join(_missing)}\n"
            f"   Hãy kiểm tra file .env hoặc cấu hình Docker Compose của bạn."
        )

    minio_endpoint = _required_env_vars["MINIO_ENDPOINT"]
    minio_access_key = _required_env_vars["MINIO_ROOT_USER"]
    minio_secret_key = _required_env_vars["MINIO_ROOT_PASSWORD"]
    
    # 1. Khởi tạo Builder với Delta, Timezone và gói Hadoop-AWS
    builder = SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    
    # 2. Bơm thư viện Delta Lake vào Spark Builder
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # 3. Ép kiểu S3A Configurations siêu bền bỉ (Resilient S3A configs)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", minio_endpoint)
    hadoop_conf.set("fs.s3a.access.key", minio_access_key)
    hadoop_conf.set("fs.s3a.secret.key", minio_secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Tối ưu hóa Network/Pool cho S3
    hadoop_conf.set("fs.s3a.connection.maximum", "100")
    hadoop_conf.set("fs.s3a.threads.max", "100")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    # Giảm bớt log rác của Spark trên console
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

# Kịch bản Test nhanh
if __name__ == "__main__":
    print("🚀 Đang khởi động Spark Engine (Delta Lake + S3A Optimized)...")
    spark = get_spark_session("Test_Environment")
    print(f"✅ Khởi tạo thành công! Phiên bản Spark: {spark.version}")