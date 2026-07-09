# ============================================================
# Makefile — RetailFlow One-Time Setup (Linux / Mac)
# ============================================================
# Chạy một lần sau khi pull project về để khởi tạo dữ liệu.
# Yêu cầu: Docker đang chạy (docker compose up -d)
#
# Lệnh:
#   make setup        — Chạy toàn bộ 4 bước setup
#   make seed         — Chỉ seed product catalog
#   make batch        — Chỉ chạy batch ingestion
#   make fetch-rates  — Chỉ fetch tỷ giá
#   make register-cdc — Chỉ đăng ký Debezium connector
# ============================================================

PYTHON := $(shell if [ -f ./venv/bin/python ]; then echo ./venv/bin/python; elif [ -f ./venv/Scripts/python.exe ]; then echo ./venv/Scripts/python.exe; else echo python; fi)

.PHONY: setup seed batch fetch-rates register-cdc setup-lifecycle help
# Mặc định: hiện danh sách lệnh
help:
	@echo ""
	@echo "RetailFlow — Available Commands:"
	@echo "  make setup        Run all 4 one-time setup steps"
	@echo "  make seed         Seed product catalog into PostgreSQL"
	@echo "  make batch        Upload CSV files to MinIO (Bronze)"
	@echo "  make fetch-rates  Fetch initial exchange rates"
	@echo "  make register-cdc Register Debezium CDC connector"
	@echo ""

setup: batch seed fetch-rates register-cdc setup-lifecycle
	@echo ""
	@echo "✅ RetailFlow setup hoàn tất!"
	@echo "   MinIO UI   : http://localhost:9001"
	@echo "   Kafka UI   : http://localhost:8080"
	@echo "   Debezium   : http://localhost:8083"
	@echo ""

seed:
	@echo "=== [2/4] Seeding Product Catalog vào PostgreSQL ==="
	$(PYTHON) scripts/seeds/seed_product_catalog.py

batch:
	@echo "=== [1/4] Batch Ingestion CSV → MinIO ==="
	$(PYTHON) scripts/ingestion/batch/batch_ingestion_job.py

fetch-rates:
	@echo "=== [3/4] Fetching Exchange Rates ==="
	$(PYTHON) scripts/ingestion/fetch/fetch_exchange_rates.py

register-cdc:
	@echo "=== [4/5] Đăng ký Debezium CDC Connector ==="
	curl -sf -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @config/debezium/ecommerce-postgres-connector.json \
		| python -m json.tool
	@echo "[4/5] Debezium Connector đã đăng ký thành công."

setup-lifecycle:
	@echo "=== [5/5] Thiết lập MinIO Lifecycle Policy (bảo vệ ổ đĩa) ==="
	$(PYTHON) scripts/utils/setup_minio_lifecycle.py
