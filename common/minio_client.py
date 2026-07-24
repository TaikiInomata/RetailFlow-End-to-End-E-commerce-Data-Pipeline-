import boto3
import logging
from botocore.client import Config
from botocore.exceptions import EndpointConnectionError, ClientError
from pathlib import Path
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)

load_dotenv(Path(__file__).parent.parent.parent / '.env')

class MinioClientFactory:
    def __init__(self, endpoint=None, access_key=None, secret_key=None):
        # Tự động lấy cấu hình mặc định nếu người dùng không truyền tham số vào
        self.endpoint_url = endpoint or os.getenv("MINIO_ENDPOINT") or f"http://localhost:{os.getenv('MINIO_API_PORT', 9000)}"
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD")
        
        # Biến nội bộ dùng để lưu trữ kết nối sau khi khởi tạo (Tránh khởi tạo lại nhiều lần)
        self._client = None

    def get_client(self):
        """Khởi tạo và trả về boto3 s3 client duy nhất (Singleton Pattern)"""
        if self._client is None:
            try:
                self._client = boto3.client(
                    's3',
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    config=Config(signature_version='s3v4')
                )
            except Exception as e:
                logger.error("[MinIO] Khởi tạo Boto3 Client thất bại: %s", e)
                raise
        return self._client

    def verify_connection(self) -> bool:
        """Kiểm tra xem hệ thống MinIO có phản hồi hay không (Health Check)"""
        try:
            client = self.get_client()
            client.list_buckets()
            logger.info("[MinIO] Health Check: Kết nối tới endpoint %s thành công.", self.endpoint_url)
            return True
        except EndpointConnectionError:
            logger.error("[MinIO] Health Check: Không thể kết nối tới endpoint %s.", self.endpoint_url)
            return False
        except Exception as e:
            logger.error("[MinIO] Health Check: Lỗi không xác định — %s", e)
            return False

    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """Kiểm tra và tạo bucket nếu chưa tồn tại."""
        try:
            client = self.get_client()
            client.head_bucket(Bucket=bucket_name)
            logger.info("[MinIO] Bucket '%s' đã tồn tại — bỏ qua bước tạo.", bucket_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                client.create_bucket(Bucket=bucket_name)
                logger.info("[MinIO] Bucket '%s' chưa tồn tại — đã tạo thành công.", bucket_name)
                return True
            logger.error("[MinIO] Lỗi khi kiểm tra bucket '%s': %s", bucket_name, e)
            return False
        except Exception as e:
            logger.error("[MinIO] Lỗi không xác định khi kiểm tra bucket '%s': %s", bucket_name, e)
            return False