#!/bin/bash
set -e

echo "============================================"
echo "   RetailFlow - One-Time Project Setup      "
echo "============================================"

echo "=== [1/4] Batch Ingestion CSV -> MinIO ==="
python scripts/ingestion/batch/batch_ingestion_job.py

echo "=== [2/4] Seeding Product Catalog into PostgreSQL ==="
python scripts/seeds/seed_product_catalog.py

echo "=== [3/4] Fetching Exchange Rates ==="
python scripts/ingestion/fetch/fetch_exchange_rates.py

echo "=== [3.5/5] Init Database Schemas (users, orders) ==="
python -c "from scripts.simulation.mock_backend import TransactionSimulator; s = TransactionSimulator(); s.setup_tables(); s.conn.commit(); s.cursor.close(); s.conn.close(); print('Schemas created successfully.')"

echo "=== [4/5] Registering Debezium CDC Connector ==="
curl -sf -X POST http://debezium:8083/connectors \
    -H "Content-Type: application/json" \
    -d @config/debezium/ecommerce-postgres-connector.json || echo "Connector might already exist or Debezium is not ready."

echo "=== [5/5] Setup MinIO Lifecycle Policy ==="
python scripts/utils/setup_minio_lifecycle.py

echo "============================================"
echo "   ✅ Setup Completed!                        "
echo "============================================"

