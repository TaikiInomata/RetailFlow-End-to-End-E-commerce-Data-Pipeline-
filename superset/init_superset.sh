#!/bin/bash
# init_superset.sh
# ─────────────────
# Script khởi tạo Superset lần đầu tiên:
#   1. Migrate database schema
#   2. Tạo admin user
#   3. Cài thêm driver: trino
#   4. Khởi động Superset web server
#
# Chạy tự động khi container start lần đầu.
# Các lần sau (db đã có) sẽ bỏ qua bước migrate+create user và chạy thẳng server.

set -e

echo "=========================================="
echo "  RetailFlow — Superset Initialization"
echo "=========================================="

# ── Cài driver kết nối Trino (nếu chưa có) ────────────────────────────────
echo "▶ Cài đặt sqlalchemy-trino driver..."
pip install --quiet sqlalchemy-trino trino

# ── Migrate database ───────────────────────────────────────────────────────
echo "▶ Migrate Superset database..."
superset db upgrade

# ── Tạo admin user (idempotent — bỏ qua nếu đã tồn tại) ──────────────────
echo "▶ Tạo admin user..."
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER:-admin}" \
    --firstname "RetailFlow" \
    --lastname "Admin" \
    --email "admin@retailflow.local" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" 2>/dev/null || echo "  (User đã tồn tại — bỏ qua)"

# ── Init default roles & permissions ──────────────────────────────────────
echo "▶ Khởi tạo roles và permissions..."
superset init

echo "=========================================="
echo "  ✅ Superset đã sẵn sàng!"
echo "  🌐 Mở trình duyệt: http://localhost:8088"
echo "  👤 Login: admin / admin"
echo "=========================================="

# ── Khởi động web server ───────────────────────────────────────────────────
echo "▶ Khởi động Superset web server..."
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
