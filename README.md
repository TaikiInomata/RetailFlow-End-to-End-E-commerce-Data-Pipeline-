# RetailFlow 🛍️ — End-to-End E-commerce Data Lakehouse

<div align="center">

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00ADD8?logo=databricks&logoColor=white)](https://delta.io/)
[![dbt](https://img.shields.io/badge/dbt-1.12-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Trino](https://img.shields.io/badge/Trino-435-DD00A1?logo=trino&logoColor=white)](https://trino.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3--compatible-C72E49?logo=minio&logoColor=white)](https://min.io/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Debezium](https://img.shields.io/badge/Debezium-2.5-FF2D20?logoColor=white)](https://debezium.io/)
[![Superset](https://img.shields.io/badge/Apache%20Superset-4.1-20A6C9?logo=apachesuperset&logoColor=white)](https://superset.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)

**Pipeline dữ liệu E-commerce end-to-end: từ nguồn (PostgreSQL, Kafka) → Bronze → Silver → Gold → Dashboard**

</div>

---

## 💡 Dự án giải quyết bài toán gì cho doanh nghiệp?

Dự án **RetailFlow** được thiết kế như một giải pháp nền tảng Dữ liệu (Data Platform) hoàn chỉnh để giải quyết các bài toán lõi trong doanh nghiệp Thương mại điện tử:
1. **Phá vỡ rào cản dữ liệu (Data Silos):** Thu thập và tập trung hóa dữ liệu từ nhiều nguồn khác nhau (Database giao dịch OLTP, Hệ thống Tracking hành vi Web/App, API bên thứ ba) về một nguồn sự thật duy nhất (Single Source of Truth).
2. **Bảo vệ hệ thống bán hàng (Offload OLTP):** Sử dụng kiến trúc **CDC (Change Data Capture)** qua Debezium và Kafka để bắt các thay đổi dữ liệu theo thời gian thực mà không cần chạy các câu lệnh truy vấn nặng (quét bảng) trên database bán hàng, giúp hệ thống luôn mượt mà.
3. **Thấu hiểu khách hàng (Customer 360 & Funnel):** Kết hợp dữ liệu đơn hàng (Giao dịch) và dữ liệu Clickstream (Hành vi) để xây dựng Phễu chuyển đổi (Conversion Funnel) và Phân khúc khách hàng (RFM Segments). Điều này giúp đội ngũ Marketing và Sales ra quyết định chính xác hơn.
4. **Tối ưu chi phí & Đảm bảo toàn vẹn dữ liệu:** Áp dụng kiến trúc **Data Lakehouse** hiện đại (lưu trữ giá rẻ trên MinIO kết hợp định dạng Delta Lake hỗ trợ ACID), thay thế cho các giải pháp Data Warehouse đắt đỏ truyền thống.

---

## 🏗️ Architecture Overview

![Project Architecture](images/architechture.png)

---

## ⚡ Tech Stack

| Công nghệ | Version | Vai trò | Lý do chọn |
|:---|:---|:---|:---|
| **Apache Airflow** | 2.x | Orchestration | Event-driven scheduling qua Datasets, thay thế cron cứng nhắc |
| **Apache Kafka** | 7.4.4 (Confluent) | Message Broker | Buffer bất đồng bộ giữa CDC và Spark, replay được |
| **Debezium** | 2.5 | CDC Engine | Capture thay đổi PostgreSQL WAL log theo thời gian thực |
| **Apache Spark** | 3.x | Transformation | Structured Streaming + `foreachBatch` + Delta MERGE INTO |
| **Delta Lake** | 3.x | Storage Format | ACID transactions, Time Travel, OPTIMIZE tích hợp sẵn |
| **MinIO** | Latest | Object Storage | S3-compatible, self-hosted, phù hợp cho on-premise/dev |
| **dbt-trino** | 1.12 | Data Transformation | SQL-first, incremental models, data tests tích hợp |
| **Trino** | 435 | Query Engine | MPP in-memory, query trực tiếp Delta Lake, nhanh hơn Hive |
| **Apache Superset** | 4.1 | BI / Dashboard | Kết nối Trino qua SQLAlchemy, no-code chart builder |

---

## 🚀 Quick Start

**Yêu cầu:** Docker Desktop ≥ 4.20, Git, 16GB RAM khuyến nghị

```bash
# 1. Clone repo
git clone https://github.com/TaikiInomata/RetailFlow-End-to-End-E-commerce-Data-Pipeline-.git
cd RetailFlow-End-to-End-E-commerce-Data-Pipeline-

# 2. Tạo file cấu hình (copy từ template)
cp .env.docker.example .env.docker
# Chỉnh sửa .env nếu cần thay đổi mật khẩu (mặc định đã dùng được)

# 3. Khởi động toàn bộ hệ thống
docker compose -f docker/docker-compose.yml --profile airflow up -d

# 4. Đợi khoảng 2-3 phút để tất cả services healthy, sau đó kiểm tra:
docker ps | grep retailflow

# 5. Đăng ký Debezium CDC connector
python scripts/setup/register_connector.py

# 6. Khởi tạo bảng Trino
python scripts/setup/init_trino_schemas.py

# 7. Kiểm tra kết quả
```

**Các giao diện web sau khi khởi động:**

| Service | URL | Tài khoản |
|:---|:---|:---|
| Airflow UI | http://localhost:8083 | admin / admin |
| MinIO Console | http://localhost:9001 | (xem .env) |
| Kafka UI | http://localhost:8080 | - |
| Trino UI | http://localhost:8082 | - |
| Superset Dashboard | http://localhost:8088 | admin / admin |

---

## 📊 Data Flow

### Bronze Layer
- **CDC:** Debezium đọc PostgreSQL WAL → Kafka Topic → Spark Structured Streaming → file JSON/Parquet trên MinIO `bronze-zone/`
- **Exchange Rate:** Airflow DAG gọi REST API lúc 8:00 SA → lưu JSON vào `bronze-zone/exchange_rates/`
- **Clickstream:** Simulation bot gửi event lên Kafka → Spark Streaming → `bronze-zone/clickstream/`

### Silver Layer
- **CDC → Delta Lake:** Spark Structured Streaming đọc file Bronze, chạy `MERGE INTO` để upsert vào các Delta table (`orders`, `products`, `users`). Checkpoint lưu offset đảm bảo exactly-once.
- **Exchange Rate → Delta:** PySpark batch đọc JSON → forward-fill giá trị thiếu → ghi Delta `exchange_rates`
- **Clickstream → Delta:** Spark Streaming đọc từ Kafka offset (Bronze checkpoint) → ghi Delta `clickstream`

### Gold Layer (dbt + Trino)
- **Trigger:** Airflow Datasets — DAG Gold chỉ chạy khi **cả hai** DAG CDC lẫn Clickstream signal hoàn tất
- **Staging views:** `stg_orders`, `stg_products`, `stg_users`, `stg_exchange_rates`, `stg_clickstream` — views ảo không lưu dữ liệu
- **5 Data Marts:** Tính toán incremental, chỉ xử lý dữ liệu mới, ghi bằng `delete+insert` với unique key

### Maintenance
- **Daily 3AM:** DAG `maintenance_pipeline` chạy `OPTIMIZE` (gom file nhỏ → 128MB) + `VACUUM` (xóa snapshot cũ 7 ngày)

---

## 🔧 Pain Points & Solutions

*Đây là những vấn đề kỹ thuật thực tế gặp phải khi xây dựng dự án:*

| # | Pain Point | Biểu hiện | Giải pháp |
|:--|:---|:---|:---|
| 1 | **Duplicate dữ liệu CDC** | Batch job 15 phút ghi lại toàn bộ → mỗi lần chạy = 1 bản duplicate | Chuyển sang Structured Streaming + `availableNow=True` + Delta `MERGE INTO` |
| 2 | **Gold DAG chạy trước Silver** | `ExternalTaskSensor` sync theo giờ cố định → race condition | Airflow Datasets (Event-driven): Gold chỉ chạy sau khi cả CDC và Clickstream signal xong |
| 3 | **DAG Gold success nhưng 0 rows** | `dbt --select marts/...` không match node nào trong dbt 1.12 | Dùng đường dẫn đầy đủ `models/marts/...` |
| 4 | **ShortCircuitOperator skip sai phạm vi** | Skip nhánh Customer 360 kéo theo skip luôn task cuối | Thêm `ignore_downstream_trigger_rules=False` |
| 5 | **dbt CLI flags sai thứ tự** | `dbt --profiles-dir ... run` → lỗi `No such option` trong Airflow | Đặt flags sau subcommand: `dbt run --profiles-dir ...` |
| 6 | **Checkpoint không nhất quán** | Tồn tại cả `_checkpoints/` và `checkpoints/` trên MinIO | Chuẩn hóa tất cả về `s3a://silver-zone/_checkpoints/` |
| 7 | **Small files problem** | 15-30 phút/batch × 24h = hàng nghìn file nhỏ → query Trino chậm dần | DAG `maintenance_pipeline` chạy `OPTIMIZE` + `VACUUM` lúc 3AM hàng ngày |

---

## 📈 Key Design Decisions

**Q: Tại sao chọn Trino + dbt thay vì PySpark thuần cho tầng Gold?**  
A: PySpark ở tầng Gold phải viết Python dài dòng cho từng transformation. Trino + dbt cho phép viết SQL ngắn gọn, có data tests tích hợp, documentation tự động (`dbt docs generate`), và Analytics Engineer (không phải Data Engineer) cũng có thể tự viết được.

**Q: Tại sao dùng Airflow Datasets thay vì ExternalTaskSensor?**  
A: ExternalTaskSensor kiểm tra theo `execution_date` — nếu 2 DAG chạy lệch múi giờ hoặc retry, rất dễ bị lệch. Dataset là event-driven thuần túy: "Khi có dữ liệu mới trong Silver" → trigger Gold, không phụ thuộc thời gian.

**Q: Tại sao dùng `availableNow=True` trong Spark Streaming cho CDC?**  
A: Để xử lý batch-on-streaming: mỗi lần trigger, Spark chỉ xử lý đúng lượng dữ liệu hiện có rồi tắt (không block mãi như streaming thực). Kết hợp với checkpoint → idempotent hoàn toàn.

---

## 🧪 Testing & Reliability

```bash
# Chạy dbt tests (Data Quality)
docker exec retailflow_airflow_webserver \
  dbt --no-use-colors test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt

# Kiểm thử Idempotency
pip install trino requests tabulate
python tests/test_idempotency.py
```

**Coverage hiện tại:**
- ✅ 49 dbt data tests (not_null, accepted_values, unique, accepted_range)
- ✅ Idempotency test: 5 bảng Gold, trigger 3 lần liên tiếp không duplicate
- ✅ Checkpoint-based exactly-once cho cả CDC và Clickstream

---

## 📁 Project Structure

```
retail-flow/
├── dags/                          # Airflow DAG definitions
│   ├── daily_exchange_rate_pipeline.py    # Bronze→Silver: tỷ giá (0 1 * * *)
│   ├── frequent_cdc_pipeline.py           # Bronze→Silver: CDC (*/15 * * * *)
│   ├── frequent_clickstream_pipeline.py   # Bronze→Silver: Clickstream (*/30 * * * *)
│   ├── gold_dbt_pipeline.py               # Silver→Gold: dbt + Trino (Dataset-triggered)
│   ├── maintenance_pipeline.py            # OPTIMIZE + VACUUM Delta Lake (0 3 * * *)
│   └── utils/                             # Shared callbacks, spark config
│
├── spark-jobs/                    # PySpark transformation scripts
│   ├── bronze/clickstream_streaming.py    # Kafka→Bronze Streaming
│   ├── silver/silver_cdc_processor.py     # Bronze→Silver CDC (Structured Streaming)
│   ├── silver/silver_clickstream.py       # Bronze→Silver Clickstream
│   └── silver/silver_exchange_rate.py     # Bronze→Silver Exchange Rate
│
├── dbt/                           # dbt project (Trino dialect)
│   ├── models/
│   │   ├── staging/               # Views: stg_orders, stg_clickstream, ...
│   │   └── marts/
│   │       ├── sales_analytics/   # gold_daily_sales_summary, gold_product_performance
│   │       ├── customer_360/      # gold_customer_snapshot (SCD Type 2)
│   │       └── funnel_analytics/  # gold_daily_funnel, gold_product_engagement
│   └── macros/rfm_segment.sql    # Reusable RFM segmentation macro
│
├── superset/                      # Apache Superset config & exports
│   ├── superset_config.py
│   ├── init_superset.sh
│   └── exports/                   # Dashboard JSON exports (importable)
│
├── docker/                        # Docker & infrastructure config
│   ├── docker-compose.yml
│   ├── trino/etc/                 # Trino catalog & JVM config
│   └── airflow/Dockerfile
│
├── scripts/setup/                 # One-time setup scripts
│   ├── init_trino_schemas.py      # Tạo schema trên Trino
│   └── register_delta_tables.py   # Đăng ký Delta tables với Hive Metastore
│
├── tests/
│   ├── test_idempotency.py        # Kiểm thử duplicate dữ liệu
│   └── idempotency_test_report.md # Kết quả kiểm thử
│
└── documents/                     # Tài liệu thiết kế và kế hoạch sprint
```

---

## 📄 License

MIT License — Free to use for learning and portfolio purposes.