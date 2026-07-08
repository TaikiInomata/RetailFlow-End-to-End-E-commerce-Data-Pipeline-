import os
import sys
import json
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
from requests.adapters import HTTPAdapter, Retry

# Mẹo: Giúp Python nhận diện được thư mục scripts/utils khi chạy từ bất kỳ đâu
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'utils'))
# pyrefly: ignore [missing-import]
from minio_client import MinioClientFactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

# --- CẤU HÌNH ---
API_URL     = "https://open.er-api.com/v6/latest/USD"
BUCKET_NAME = "bronze-zone"
PREFIX_PATH = "api_exchange_rates"

# Chỉ lưu các currency liên quan đến dataset (ecommerce behavior - chủ yếu RUB)
# và các đồng tiền phổ biến để hỗ trợ phân tích đa tiền tệ ở Silver layer.
# Giảm từ ~162 currencies → 10 currencies — tránh noise và tiết kiệm storage.
CURRENCIES_OF_INTEREST = {"USD", "VND", "RUB", "EUR", "GBP", "JPY", "CNY", "KRW", "THB", "SGD"}


def build_http_session() -> requests.Session:
    """
    Tạo HTTP Session với cơ chế retry tự động (3 lần, tăng dần delay).
    Giúp chống mất dữ liệu khi API tạm thời không phản hồi.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=3,            # Tổng số lần retry
        backoff_factor=2,   # Delay: 2s → 4s → 8s
        status_forcelist=[429, 500, 502, 503, 504],  # Retry khi gặp các HTTP error code này
    )
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session


def fetch_exchange_rates() -> dict | None:
    """
    Gọi API để lấy tỷ giá USD mới nhất.
    Chỉ giữ lại các currencies trong CURRENCIES_OF_INTEREST để tránh noise.
    Trả về None nếu thất bại sau tất cả các lần retry.
    """
    logger.info("Đang gọi API lấy tỷ giá từ: %s", API_URL)
    try:
        session  = build_http_session()
        response = session.get(API_URL, timeout=10)
        response.raise_for_status()  # Bắn lỗi nếu HTTP status != 2xx

        data = response.json()
        # Filter: chỉ giữ lại currencies cần thiết
        all_rates = data.get("rates", {})
        data["rates"] = {k: v for k, v in all_rates.items() if k in CURRENCIES_OF_INTEREST}

        logger.info(
            "✅ Đã lấy thành công tỷ giá gốc %s cho %d currencies (đã lọc từ %d).",
            data.get("base_code"),
            len(data["rates"]),
            len(all_rates),
        )
        return data
    except requests.exceptions.RequestException as e:
        logger.error("❌ Lỗi khi gọi API sau tất cả các lần retry: %s", e)
        return None


def upload_to_minio(data: dict) -> None:
    """
    Lưu dữ liệu JSON xuống MinIO theo định dạng Hive Partitioning (year=/month=/day=/).
    Sử dụng microsecond trong tên file để đảm bảo idempotency (tránh ghi đè
    khi job chạy nhiều lần trong cùng 1 giây).
    """
    factory  = MinioClientFactory()

    # Kiểm tra và tạo bucket nếu chưa tồn tại (tái sử dụng helper của dự án)
    if not factory.ensure_bucket_exists(BUCKET_NAME):
        logger.error("❌ Không thể tạo/truy cập bucket '%s'. Dừng job.", BUCKET_NAME)
        sys.exit(1)

    s3_client = factory.get_client()

    # Bổ sung metadata về thời gian kéo dữ liệu (quan trọng trong Data Lake để audit)
    now_utc       = datetime.now(timezone.utc)
    data["_ingested_at"] = now_utc.isoformat()

    # Hive Partitioning: year=.../month=.../day=...
    year  = now_utc.strftime("%Y")
    month = now_utc.strftime("%m")
    day   = now_utc.strftime("%d")
    # Dùng microsecond (%f) để đảm bảo tên file unique khi chạy nhiều lần trong cùng 1 giây
    file_name = f"rates_{now_utc.strftime('%H%M%S%f')}.json"

    # Path hoàn chỉnh: api_exchange_rates/year=2026/month=07/day=08/rates_...json
    s3_path = f"{PREFIX_PATH}/year={year}/month={month}/day={day}/{file_name}"

    try:
        # Upload trực tiếp từ RAM — không cần lưu file tạm trên ổ cứng
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_path,
            Body=json_bytes,
            ContentLength=len(json_bytes),
            ContentType="application/json",
        )
        logger.info("💾 Đã lưu thành công: s3a://%s/%s", BUCKET_NAME, s3_path)
    except Exception as e:
        logger.error("❌ Lỗi khi upload lên MinIO: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    logger.info("=== BẮT ĐẦU JOB FETCH EXCHANGE RATES ===")

    # Bước 1: Kéo dữ liệu từ API (có retry tự động)
    rates_data = fetch_exchange_rates()

    # Bước 2: Kiểm tra kết quả — thoát với exit code 1 nếu thất bại
    # (exit code 1 giúp scheduler/Docker biết job lỗi để cảnh báo hoặc retry)
    if not rates_data:
        logger.error("Job thất bại — không lấy được dữ liệu từ API. Dừng lại.")
        sys.exit(1)

    # Bước 3: Load thẳng xuống Bronze Zone
    upload_to_minio(rates_data)

    logger.info("=== KẾT THÚC JOB THÀNH CÔNG ===")