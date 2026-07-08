import os
import sys
import json
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

# Mẹo: Giúp Python nhận diện được thư mục scripts/utils khi chạy từ bất kỳ đâu
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'utils'))
# pyrefly: ignore [missing-import]
from minio_client import MinioClientFactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

# Cấu hình API và S3
API_URL = "https://open.er-api.com/v6/latest/USD"
BUCKET_NAME = "bronze-zone"
PREFIX_PATH = "api_exchange_rates"

def fetch_exchange_rates():
    """Gọi API để lấy dữ liệu tỷ giá hối đoái mới nhất"""
    logger.info(f"Đang gọi API lấy tỷ giá từ: {API_URL}")
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status() # Bắn lỗi nếu HTTP status != 200
        
        data = response.json()
        logger.info(f"✅ Đã lấy thành công tỷ giá gốc {data.get('base_code')} cho {len(data.get('rates', []))} loại tiền tệ.")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Lỗi khi gọi API: {e}")
        return None

def upload_to_minio(data):
    """Lưu dữ liệu JSON xuống MinIO theo định dạng Partition by Date"""
    if not data:
        logger.warning("Không có dữ liệu để upload.")
        return

    # Lấy Client kết nối MinIO từ Factory (Tái sử dụng code cũ của bạn)
    factory = MinioClientFactory()
    s3_client = factory.get_client()

    # Bổ sung metadata về thời gian kéo dữ liệu (Cực kỳ quan trọng trong Data Lake)
    now_utc = datetime.now(timezone.utc)
    data["_ingested_at"] = now_utc.isoformat()

    # Thiết kế đường dẫn Hive-Partitioning: year=.../month=.../day=...
    year = now_utc.strftime("%Y")
    month = now_utc.strftime("%m")
    day = now_utc.strftime("%d")
    file_name = f"rates_{now_utc.strftime('%H%M%S')}.json"
    
    # Path hoàn chỉnh: api_exchange_rates/year=2026/month=07/day=05/rates_...json
    s3_path = f"{PREFIX_PATH}/year={year}/month={month}/day={day}/{file_name}"

    try:
        # Biến Dictionary thành chuỗi JSON và upload trực tiếp từ RAM (không cần lưu file tạm)
        json_string = json.dumps(data, indent=2)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_path,
            Body=json_string.encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"💾 Đã lưu thành công dữ liệu xuống Data Lake: s3a://{BUCKET_NAME}/{s3_path}")
    except Exception as e:
        logger.error(f"❌ Lỗi khi upload lên MinIO: {e}")

if __name__ == "__main__":
    logger.info("=== BẮT ĐẦU JOB FETCH EXCHANGE RATES ===")
    
    # Bước 1: Kéo dữ liệu
    rates_data = fetch_exchange_rates()
    
    # Bước 2: Load thẳng xuống Bronze Zone
    upload_to_minio(rates_data)
    
    logger.info("=== KẾT THÚC JOB ===")