"""
batch_ingestion_job.py
======================
Upload dữ liệu CSV lịch sử từ Kaggle lên MinIO Bronze Zone.

Luồng thực thi:
  1. download_dataset_if_needed() — Tải CSV từ Kaggle nếu chưa có
       a. Files đã tồn tại trong datasets/    → bỏ qua, tiết kiệm thời gian
       b. ZIP cục bộ tìm thấy ở project root  → giải nén ngay, không cần internet
       c. Cả hai đều không có                 → tải trực tiếp từ Kaggle API
  2. Upload từng CSV lên MinIO với multipart transfer + progress bar
  3. In báo cáo tổng kết (Audit Report)

Dataset Kaggle:
  mkechinov/ecommerce-behavior-data-from-multi-category-store
  https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

Cấu hình trong .env:
  KAGGLE_USERNAME  — Kaggle username (tạo tại https://www.kaggle.com/settings/account)
  KAGGLE_KEY       — Kaggle API key  (tạo tại https://www.kaggle.com/settings/account)
"""
import os
import sys
import shutil
import zipfile
import logging
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

# --- SETUP PATH ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "common"))
load_dotenv(PROJECT_ROOT / '.env')

# pyrefly: ignore [missing-import]
from minio_client import MinioClientFactory  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# --- CẤU HÌNH ---
MB            = 1024 ** 2
BUCKET_NAME   = 'bronze-zone'
DATASET_DIR   = PROJECT_ROOT / 'scripts' / 'ingestion' / 'batch' / 'datasets'
KAGGLE_DATASET = "mkechinov/ecommerce-behavior-data-from-multi-category-store"

# Ánh xạ tên tháng tiếng Anh sang số để tạo Hive partition chuẩn
MONTH_NAME_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

transfer_config = TransferConfig(
    multipart_threshold=100 * MB,
    multipart_chunksize=50  * MB,
    max_concurrency=10,
    use_threads=True
)


# ── Progress Bar ─────────────────────────────────────────────────────────────
class ProgressPercentage:
    def __init__(self, filename):
        self._filename     = os.path.basename(filename)
        self._size         = float(os.path.getsize(filename))
        self._seen_so_far  = 0
        self._lock         = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                f"\r[>>] {self._filename}: "
                f"{self._seen_so_far / MB:.2f} MB / {self._size / MB:.2f} MB "
                f"({percentage:.2f}%)"
            )
            sys.stdout.flush()


# ── Download from Kaggle ──────────────────────────────────────────────────────
def _extract_zip(zip_path: Path, target_dir: Path) -> list[str]:
    """
    Giải nén TẤT CẢ file CSV từ ZIP vào target_dir.
    Trả về danh sách tên file đã giải nén.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    logger.info("[BatchJob] Giải nén: %s → %s", zip_path.name, target_dir)
    extracted = []
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Lấy tất cả file .csv trong ZIP (bất kể tên gì)
        csv_members = [m for m in zf.namelist() if m.lower().endswith('.csv')]
        if not csv_members:
            logger.warning("[BatchJob] Không tìm thấy file .csv nào trong ZIP.")
            return extracted
        logger.info("[BatchJob] Tìm thấy %d file CSV trong ZIP.", len(csv_members))
        for member in csv_members:
            # Flatten: bỏ đường dẫn thư mục trong ZIP, chỉ lấy tên file
            out_path = target_dir / Path(member).name
            if out_path.exists():
                logger.info("[BatchJob]   Đã tồn tại, bỏ qua: %s", out_path.name)
                extracted.append(out_path.name)
                continue
            with zf.open(member) as src, open(out_path, 'wb') as dst:
                # Tối ưu RAM & Tốc độ: Đọc/ghi theo chunk 16MB thay vì 64KB mặc định để tránh nghẽn I/O
                shutil.copyfileobj(src, dst, length= 32 * MB)
            logger.info("[BatchJob]   Giải nén thành công: %s (%.1f MB)",
                        out_path.name, out_path.stat().st_size / MB)
            extracted.append(out_path.name)
    return extracted


def _download_via_kaggle_api(out_zip_path: Path) -> bool:
    """
    Lấy link tải trực tiếp từ Kaggle API và dùng Multi-Threading để tăng tốc độ tải file ZIP.
    Trả về True nếu tải xong thành công.
    """
    kaggle_user = os.getenv("KAGGLE_USERNAME")
    kaggle_key  = os.getenv("KAGGLE_KEY")

    if not kaggle_user or not kaggle_key:
        logger.error("[BatchJob] Thiếu biến môi trường KAGGLE_USERNAME hoặc KAGGLE_KEY.")
        logger.error("[BatchJob] Tạo API key tại: https://www.kaggle.com/settings/account")
        logger.error("[BatchJob] Sau đó thêm vào file .env:")
        logger.error("[BatchJob]   KAGGLE_USERNAME=your_username")
        logger.error("[BatchJob]   KAGGLE_KEY=your_api_key")
        return False

    url = f"https://www.kaggle.com/api/v1/datasets/download/{KAGGLE_DATASET}"
    logger.info("[BatchJob] Lấy URL tải trực tiếp từ Kaggle API...")
    
    try:
        r_head = requests.get(url, auth=(kaggle_user, kaggle_key), stream=True, timeout=15)
        r_head.raise_for_status()
        direct_url = r_head.url
        file_size = int(r_head.headers.get("Content-Length", 0))
        r_head.close()
    except Exception as e:
        logger.error("[BatchJob] Lỗi kết nối Kaggle API: %s", e)
        return False

    if file_size == 0:
        logger.error("[BatchJob] Không xác định được kích thước file từ Kaggle.")
        return False

    logger.info("[BatchJob] Kích thước dataset: %.2f GB. Bắt đầu tải đa luồng...", file_size / (1024**3))

    # Pre-allocate file
    with open(out_zip_path, "wb") as f:
        f.truncate(file_size)

    chunk_size = 50 * 1024 * 1024  # 50MB per chunk
    chunks = []
    for start in range(0, file_size, chunk_size):
        end = min(start + chunk_size - 1, file_size - 1)
        chunks.append((start, end))

    downloaded_bytes = 0
    lock = threading.Lock()
    shutdown_event = threading.Event()
    f_out = open(out_zip_path, "r+b")  # Mở file một lần dùng chung cho các luồng

    def download_chunk(start, end):
        nonlocal downloaded_bytes
        current_pos = start
        retries = 5
        while current_pos <= end and retries > 0:
            if shutdown_event.is_set():
                break
            headers = {"Range": f"bytes={current_pos}-{end}"}
            try:
                with requests.get(direct_url, headers=headers, timeout=60, stream=True) as resp:
                    resp.raise_for_status()
                    # Tải streaming từng MB để cập nhật progress bar mượt mà
                    for chunk_data in resp.iter_content(chunk_size=1024 * 1024):
                        if shutdown_event.is_set():
                            break
                        if chunk_data:
                            with lock:
                                f_out.seek(current_pos)
                                f_out.write(chunk_data)
                                current_pos += len(chunk_data)
                                
                                downloaded_bytes += len(chunk_data)
                                pct = (downloaded_bytes / file_size) * 100
                                sys.stdout.write(f"\r[>>] Đang tải (Multi-Thread): {downloaded_bytes / MB:.1f} MB / {file_size / MB:.1f} MB ({pct:.1f}%)")
                                sys.stdout.flush()
                break # Tải xong chunk này thành công thì thoát vòng lặp retry
            except Exception as e:
                retries -= 1
                if retries == 0:
                    raise e
                import time
                time.sleep(2)

    success = True
    try:
        from concurrent.futures import wait
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(download_chunk, c[0], c[1]) for c in chunks]
            # Vòng lặp chờ với timeout 1 giây để luồng chính (main thread) kịp bắt phím Ctrl+C
            while futures:
                done, not_done = wait(futures, timeout=1.0)
                for future in done:
                    future.result()  # Báo lỗi nếu 1 luồng bị crash
                    futures.remove(future)
    except KeyboardInterrupt:
        logger.error("\n[BatchJob] Đã nhận lệnh ngắt (Ctrl+C). Đang dọn dẹp và đóng các luồng tải...")
        shutdown_event.set()
        success = False
    except Exception as e:
        logger.error("\n[BatchJob] Lỗi khi tải đa luồng: %s", e)
        shutdown_event.set()
        success = False
    finally:
        f_out.close()

    sys.stdout.write("\n")
    if not success and out_zip_path.exists():
        out_zip_path.unlink()  # Cleanup file tải dở
    elif success:
        logger.info("[BatchJob] Tải dataset hoàn tất.")
        
    return success


def discover_csv_files() -> list[Path]:
    """Quét DATASET_DIR và trả về danh sách tất cả file .csv tìm được."""
    return sorted(DATASET_DIR.glob('*.csv'))


def download_dataset_if_needed() -> bool:
    """
    Đảm bảo có ít nhất 1 CSV file trong DATASET_DIR trước khi upload.

    Thứ tự ưu tiên:
      1. Đã có CSV trong datasets/     → skip (idempotent, nhanh nhất)
      2. ZIP cục bộ ở project root     → extract tất cả CSV (không cần internet)
      3. Tải trực tiếp từ Kaggle API  → download + extract (cần KAGGLE_USERNAME + KAGGLE_KEY)
    """
    # Case 1: Đã có ít nhất 1 CSV file
    existing_csvs = discover_csv_files()
    if existing_csvs:
        logger.info("[BatchJob] ✅ Tìm thấy %d CSV file trong %s — bỏ qua download.",
                    len(existing_csvs), DATASET_DIR)
        for f in existing_csvs:
            logger.info("[BatchJob]   • %s (%.1f GB)", f.name, f.stat().st_size / (1024 ** 3))
        return True

    # Case 2: ZIP cục bộ tồn tại ở project root
    local_zip = PROJECT_ROOT / "ecommerce-behavior-data-from-multi-category-store.zip"
    if local_zip.exists():
        logger.info("[BatchJob] Tìm thấy ZIP cục bộ: %s (%.1f GB) — đang giải nén TẤT CẢ CSV...",
                    local_zip.name, local_zip.stat().st_size / (1024 ** 3))
        extracted = _extract_zip(local_zip, DATASET_DIR)
        if extracted:
            logger.info("[BatchJob] Giải nén thành công %d file CSV.", len(extracted))
            logger.info("[BatchJob] Đang xóa file ZIP gốc để giải phóng ổ cứng: %s", local_zip.name)
            try:
                local_zip.unlink()
            except Exception as e:
                logger.warning("[BatchJob] Không thể xóa file ZIP gốc: %s", e)
            return True
        logger.warning("[BatchJob] ZIP không chứa CSV nào. Thử tải từ Kaggle...")

    # Case 3: Tải từ Kaggle API bằng cơ chế tải đa luồng
    logger.info("[BatchJob] Bắt đầu tải dataset từ Kaggle: %s", KAGGLE_DATASET)
    local_zip = PROJECT_ROOT / "ecommerce-behavior-data-from-multi-category-store.zip"
    if _download_via_kaggle_api(local_zip):
        # Tải xong file ZIP, đệ quy gọi lại hàm này để nó rơi vào Case 2 (giải nén)
        return download_dataset_if_needed()
    return False


# ── Upload to MinIO ───────────────────────────────────────────────────────────
def _parse_partition(file_name: str) -> tuple[str, str] | None:
    """
    Trích xuất (year, month_number) từ tên file.
    Hỗ trợ cả 2 định dạng:
      '2019-Oct.csv' → ('2019', '10')
      '2019-09.csv'  → ('2019', '09')
    Trả về None nếu không parse được.
    """
    stem  = Path(file_name).stem  # '2019-Oct' hoặc '2019-09'
    parts = stem.split("-")
    if len(parts) != 2:
        return None

    year, month_raw = parts
    # Tháng tên tiếng Anh (Oct, Nov, ...) → số
    month_num = MONTH_NAME_MAP.get(month_raw, None)
    # Tháng dạng số (09, 10, ...) → giữ nguyên
    if month_num is None and month_raw.isdigit():
        month_num = month_raw.zfill(2)

    return (year, month_num) if month_num else None


def main():
    logger.info("[BatchJob] === Khởi động Batch Ingestion Job ===")

    # 1. Đảm bảo CSV files có sẵn (download nếu cần)
    if not download_dataset_if_needed():
        logger.critical("[BatchJob] Dừng Job: Không thể tải dataset. Xem hướng dẫn ở trên.")
        sys.exit(1)

    # 2. Khám phá tất cả CSV files hiện có trong DATASET_DIR
    csv_files = discover_csv_files()
    if not csv_files:
        logger.critical("[BatchJob] Không tìm thấy file CSV nào trong %s sau bước download.", DATASET_DIR)
        sys.exit(1)

    logger.info("[BatchJob] Sẽ sử dụng %d CSV file(s) để seed cho PostgreSQL:", len(csv_files))
    for f in csv_files:
        logger.info("[BatchJob]   • %s (%.2f GB)", f.name, f.stat().st_size / (1024 ** 3))

    logger.info("[BatchJob] Đã tắt tính năng Upload lên MinIO theo yêu cầu. Dữ liệu sẽ chỉ được dùng để seed Postgres.")
    sys.exit(0)


if __name__ == "__main__":
    main()