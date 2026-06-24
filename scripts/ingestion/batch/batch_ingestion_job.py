import os
import sys
import logging
import threading
from boto3.s3.transfer import TransferConfig

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
sys.path.append(PROJECT_ROOT)

from scripts.utils.minio_client import MinioClientFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

MB = 1024 ** 2

BUCKET_NAME = 'bronze-zone'
DATASET_DIR = os.path.join(PROJECT_ROOT, 'scripts/ingestion/batch/datasets')
FILES_TO_UPLOAD = ['2019-09.csv', '2019-10.csv']

transfer_config = TransferConfig(
    multipart_threshold=100 * MB,
    multipart_chunksize=50 * MB,
    max_concurrency=10,
    use_threads=True
)

class ProgressPercentage:
    def __init__(self, filename):
        self._filename = os.path.basename(filename)
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # Bắt buộc xếp hàng để cập nhật tiến trình (Thread-safe)
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            # \r giúp xóa dòng cũ, in dòng mới đè lên để tạo thanh tiến trình mượt mà
            sys.stdout.write(
                f"\r⏳ [Ingesting] {self._filename}: {self._seen_so_far / MB:.2f} MB / {self._size / MB:.2f} MB ({percentage:.2f}%)"
            )
            sys.stdout.flush()

def main():
    logger.info("[BatchJob] === Khởi động Batch Ingestion Job ===")

    # 1. Khởi tạo kết nối qua Factory
    factory = MinioClientFactory()
    if not factory.verify_connection():
        logger.critical("[BatchJob] Dừng Job: Không thể kết nối tới MinIO Data Lake.")
        sys.exit(1)
        
    minio_client = factory.get_client()

    if not factory.ensure_bucket_exists(BUCKET_NAME):
        logger.critical("[BatchJob] Dừng Job: Không thể tạo hoặc truy cập bucket '%s'.", BUCKET_NAME)
        sys.exit(1)

    # Sử dụng mảng để theo dõi vết (Audit Trail)
    success_files = []
    failed_files = []
    unpartitioned_files = []

    # 2. Xử lý từng file trong danh sách
    for file_name in FILES_TO_UPLOAD:
        local_file_path = os.path.join(DATASET_DIR, file_name)
        if not os.path.exists(local_file_path):
            logger.warning("[BatchJob] Bỏ qua — file không tồn tại: %s", local_file_path)
            failed_files.append({"file": file_name, "error": "File không tồn tại trên ổ cứng"})
            continue
        
        name_without_ext = os.path.splitext(file_name)[0]
        parts = name_without_ext.split("-")
        if len(parts) == 2:
            year = parts[0]   
            month = parts[1]  
            minio_object_path = f"batch_ecommerce/year={year}/month={month}/{file_name}"
        else:
            minio_object_path = f"batch_ecommerce/unpartitioned/{file_name}"
            unpartitioned_files.append(file_name)
            logger.warning("[BatchJob] Không xác định được partition từ tên file '%s' — chuyển vào unpartitioned/.", file_name)
        
        logger.info("[BatchJob] Bắt đầu xử lý: %s → s3://%s/%s", file_name, BUCKET_NAME, minio_object_path)
        
        try:
            minio_client.upload_file(
                Filename=local_file_path, 
                Bucket=BUCKET_NAME, 
                Key=minio_object_path,
                Config=transfer_config,
                Callback=ProgressPercentage(local_file_path)
            )
            success_files.append(file_name)
            logger.info("[BatchJob] Upload thành công: s3://%s/%s", BUCKET_NAME, minio_object_path)
        except Exception as e:
            logger.error("[BatchJob] Upload thất bại cho file '%s': %s", file_name, e)
            failed_files.append({"file": file_name, "error": str(e)})
    
    # --- PHẦN TỔNG KẾT BÁO CÁO (AUDIT REPORT) ---
    logger.info("[BatchJob] === BÁO CÁO TỔNG KẾT INGESTION ===")

    if unpartitioned_files:
        logger.warning("[BatchJob] %d/%d file không xác định được partition, đã chuyển vào unpartitioned/:",
                       len(unpartitioned_files), len(FILES_TO_UPLOAD))
        for file in unpartitioned_files:
            logger.warning("[BatchJob]   - %s", file)

    logger.info("[BatchJob] Upload thành công : %d/%d file.", len(success_files), len(FILES_TO_UPLOAD))
    for f in success_files:
        logger.info("[BatchJob]   + %s", f)

    if failed_files:
        logger.error("[BatchJob] Upload thất bại   : %d file.", len(failed_files))
        for item in failed_files:
            logger.error("[BatchJob]   - %s | Lý do: %s", item['file'], item['error'])
        logger.critical("[BatchJob] KẾt luận: JOB THẤT BẠI MỘT PHẦN. Kiểm tra lại các file lỗi bên trên.")
        sys.exit(1)
    else:
        logger.info("[BatchJob] KẾt luận: JOB HOÀN THÀNH — tất cả %d file đã được nạp thành công.", len(success_files))
        sys.exit(0)


if __name__ == "__main__":
    main()