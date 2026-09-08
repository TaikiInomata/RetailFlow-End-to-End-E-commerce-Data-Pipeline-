"""
superset_config.py
──────────────────
Cấu hình cho Apache Superset trong môi trường RetailFlow Docker.

Chạy trong container với biến môi trường SUPERSET_SECRET_KEY được inject
từ docker-compose.yml.
"""
import os

# ── Bảo mật ──────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "retailflow_superset_secret_key_change_in_prod")

# ── Database (SQLite nội bộ để đơn giản — đủ cho portfolio demo) ─────────────
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# ── Feature Flags ─────────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,     # Cho phép Jinja trong SQL queries
    "DASHBOARD_NATIVE_FILTERS": True,       # Bộ lọc Dashboard tích hợp
    "DASHBOARD_CROSS_FILTERS": True,        # Lọc chéo giữa các chart
    "ALERT_REPORTS": False,                 # Tắt Alert/Report (cần Redis)
    "EMBEDDABLE_CHARTS": True,              # Cho phép embed chart ra ngoài
}

# ── Cache (dùng SimpleCache cho dev/demo, Redis cho production) ───────────────
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,           # 5 phút
}

# ── Upload & Security ─────────────────────────────────────────────────────────
UPLOAD_FOLDER = "/app/superset_home/uploads/"
IMG_UPLOAD_FOLDER = "/app/superset_home/images/"

# Đổi tên session cookie → tránh đụng với Airflow (cùng dùng tên "session" mặc định trên localhost)
SESSION_COOKIE_NAME = "superset_session"

# Cho phép kết nối đến Trino trong Docker network (không yêu cầu SSL)
PREVENT_UNSAFE_DB_CONNECTIONS = False

# ── Timeout ───────────────────────────────────────────────────────────────────
# Trino query timeout (Delta Lake scan có thể mất vài giây)
SUPERSET_WEBSERVER_TIMEOUT = 120
