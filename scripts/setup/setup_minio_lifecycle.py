"""
setup_minio_lifecycle.py
========================
Thiết lập MinIO Lifecycle Policy để tự động xóa dữ liệu cũ trong Bronze Zone.

Chiến lược:
  - batch_ecommerce/   → KHÔNG áp dụng lifecycle (dữ liệu lịch sử Kaggle, xóa là mất)
  - clickstream/       → Xóa sau 7 ngày (Spark Streaming — sinh lại được)
  - checkpoints/       → Xóa sau 7 ngày (Spark checkpoint — tự tạo lại)
  - cdc_postgres/      → Xóa sau 7 ngày (Debezium CDC — simulation sinh lại được)
  - api_exchange_rates/ → Xóa sau 30 ngày (fetch lại được, nhưng nên giữ lâu hơn)

Chạy một lần sau khi khởi tạo dự án (đã được tích hợp vào setup.ps1 và Makefile).
"""
import os
import sys
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / '.env')

sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common'))
# pyrefly: ignore [missing-import]
from minio_client import MinioClientFactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

BUCKET_NAME = "bronze-zone"

# Định nghĩa lifecycle rules
# Key: prefix trong bucket, Value: số ngày trước khi tự xóa
LIFECYCLE_RULES = [
    {
        "prefix": "clickstream/",
        "days":   7,
        "reason": "Spark Streaming Parquet — simulation sinh lại được",
    },
    {
        "prefix": "checkpoints/",
        "days":   7,
        "reason": "Spark Checkpoint — tự tạo lại khi streaming khởi động",
    },
    {
        "prefix": "cdc_postgres/",
        "days":   7,
        "reason": "Debezium CDC JSON — simulation sinh lại được",
    },
    {
        "prefix": "api_exchange_rates/",
        "days":   30,
        "reason": "Exchange Rates JSON — giữ 30 ngày để phân tích xu hướng",
    },
    # batch_ecommerce/ KHÔNG có trong danh sách → KHÔNG bị xóa tự động
]


def build_lifecycle_config(rules: list[dict]) -> dict:
    """Tạo cấu hình S3 Lifecycle XML theo chuẩn AWS/MinIO."""
    lifecycle_rules = []
    for i, rule in enumerate(rules):
        lifecycle_rules.append({
            "ID": f"expire-{rule['prefix'].rstrip('/').replace('/', '-')}-{rule['days']}d",
            "Status": "Enabled",
            "Filter": {"Prefix": rule["prefix"]},
            "Expiration": {"Days": rule["days"]},
        })
    return {"Rules": lifecycle_rules}


def apply_lifecycle(s3_client, bucket: str, config: dict) -> None:
    """Áp dụng lifecycle configuration lên bucket."""
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration=config,
    )


def main() -> None:
    logger.info("=== THIẾT LẬP MINIO LIFECYCLE POLICY ===")
    logger.info("Bucket: %s", BUCKET_NAME)

    factory   = MinioClientFactory()
    if not factory.verify_connection():
        logger.critical("[BatchJob] Dừng Job: Không thể kết nối tới MinIO Data Lake.")
        sys.exit(1)
    
    s3_client = factory.get_client()
    if not factory.ensure_bucket_exists(BUCKET_NAME):
        logger.critical("[BatchJob] Dừng Job: Không thể tạo hoặc truy cập bucket '%s'.", BUCKET_NAME)
        sys.exit(1)

    # Build config
    config = build_lifecycle_config(LIFECYCLE_RULES)

    logger.info("Sẽ áp dụng %d lifecycle rules:", len(LIFECYCLE_RULES))
    for rule in LIFECYCLE_RULES:
        logger.info("  %-25s → Xóa sau %d ngày  (%s)", rule["prefix"], rule["days"], rule["reason"])
    logger.info("  %-25s → KHÔNG áp dụng  (dữ liệu lịch sử Kaggle, KHÔNG xóa)", "batch_ecommerce/")

    try:
        apply_lifecycle(s3_client, BUCKET_NAME, config)
        logger.info("✅ Lifecycle policy đã được áp dụng thành công.")
    except Exception as e:
        logger.error("❌ Lỗi khi áp dụng lifecycle: %s", e)
        sys.exit(1)

    # Verify
    try:
        result = s3_client.get_bucket_lifecycle_configuration(Bucket=BUCKET_NAME)
        logger.info("Xác nhận: %d rules đang hoạt động.", len(result.get("Rules", [])))
    except Exception as e:
        logger.warning("Không thể xác nhận lifecycle policy: %s", e)

    logger.info("=== HOÀN TẤT ===")


if __name__ == "__main__":
    main()
