#!/bin/bash
set -e

# Thay thế các biến môi trường vào hive-site.xml (Vì Hive không đọc env trực tiếp trong xml)
sed -i "s|\${DB_USER}|${DB_USER}|g" /opt/hive/conf/hive-site.xml
sed -i "s|\${DB_PASSWORD}|${DB_PASSWORD}|g" /opt/hive/conf/hive-site.xml
sed -i "s|\${MINIO_ENDPOINT}|${MINIO_ENDPOINT}|g" /opt/hive/conf/hive-site.xml
sed -i "s|\${MINIO_ROOT_USER}|${MINIO_ROOT_USER}|g" /opt/hive/conf/hive-site.xml
sed -i "s|\${MINIO_ROOT_PASSWORD}|${MINIO_ROOT_PASSWORD}|g" /opt/hive/conf/hive-site.xml

echo "Waiting for postgres-airflow to be ready on port 5432..."
while ! nc -z postgres-airflow 5432; do
  sleep 1
done
echo "Postgres is ready."

echo "Checking schema status in Postgres (database: metastore)..."
# schematool is provided by Hive
/opt/hive/bin/schematool -dbType postgres -info > /tmp/schema_info.txt 2>&1 || true

if grep -q "Metastore schema version is not compatible" /tmp/schema_info.txt || grep -q "Schema initialization FAILED" /tmp/schema_info.txt || grep -q "Error: relation" /tmp/schema_info.txt; then
  echo "Initializing Hive Metastore Schema..."
  /opt/hive/bin/schematool -dbType postgres -initSchema
else
  echo "Schema already initialized or ready."
fi

echo "Starting Hive Metastore Service..."
exec /opt/hive/bin/hive --service metastore
