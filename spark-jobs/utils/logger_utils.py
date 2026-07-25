import logging
import time
from functools import wraps

def get_logger(name="SparkJob"):
    """
    Tạo và cấu hình logger chuẩn Production, đảm bảo tính nhất quán cho toàn bộ dự án.
    """
    logger = logging.getLogger(name)
    # Tránh add handler nhiều lần nếu hàm được gọi nhiều nơi
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        # Không truyền log lên root logger để tránh in trùng lặp
        logger.propagate = False
    return logger

def timeit(logger_name="SparkJob"):
    """
    Decorator đo lường thời gian thực thi (Execution Metrics) và bọc try-except tự động.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(logger_name)
            start_time = time.time()
            logger.info(f"⏳ Bắt đầu thực thi: {func.__name__}")
            try:
                result = func(*args, **kwargs)
                elapsed = time.time() - start_time
                logger.info(f"⏱️ Hoàn tất: '{func.__name__}' trong {elapsed:.2f} giây.")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"❌ Lỗi nghiêm trọng tại '{func.__name__}' sau {elapsed:.2f} giây: {str(e)}", exc_info=True)
                raise e
        return wrapper
    return decorator
