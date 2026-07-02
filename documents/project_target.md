TÀI LIỆU TỔNG QUAN DỰ ÁN DATA ENGINEERING
Tên dự án: CartStream (RetailFlow Data Platform)
Vai trò: Data Engineer

1. Triết Lý Kiến Trúc (Kim Chỉ Nam)
Hệ thống được thiết kế dựa trên 4 nguyên tắc cốt lõi của Big Data hiện đại, hướng tới tiêu chuẩn Production:

Single Source of Truth (Nguồn chân lý duy nhất): Mọi luồng dữ liệu đều hội tụ tại Data Lakehouse và được phân mảnh chặt chẽ theo mô hình Medallion (Bronze - Silver - Gold).

Decoupling (Tách rời độc lập): Các tầng Compute (Spark), Storage (MinIO) và Ingestion (Kafka) được cô lập hoàn toàn. Sự cố ở một thành phần không làm gián đoạn toàn bộ hệ thống.

Idempotency & Fault Tolerance (Lũy đẳng & Chịu lỗi): Hệ thống có khả năng chạy lại luồng xử lý (Backfill) bao nhiêu lần tùy ý mà không gây nhân đôi dữ liệu nhờ cơ chế Upsert/Merge.

Kiến trúc Lambda: Tối ưu hóa chi phí và hiệu năng bằng cách rẽ nhánh xử lý: dùng Streaming cho luồng dữ liệu nóng (Clickstream) và Batch processing cho các báo cáo phân tích khối lượng lớn cuối ngày.

2. Chiến Lược Nguồn Dữ Liệu (Data Strategy)
Để giải quyết bài toán thiếu hụt dữ liệu thực tế về cả khối lượng lẫn tốc độ, dự án vận hành song song 3 nguồn dữ liệu:

Batch / Historical Data (Xử lý dữ liệu lớn): Tải dataset E-commerce khổng lồ (vài GB) từ Kaggle đẩy thẳng vào MinIO, tạo áp lực ép hệ thống phải cấu hình tối ưu hóa bộ nhớ khi xử lý Big Data.

CDC / Transactional Data (Dữ liệu giao dịch): Sử dụng Python Faker đóng vai trò Mock Backend, liên tục thực hiện lệnh INSERT/UPDATE vào PostgreSQL. Debezium sẽ đọc log (WAL) để bắt sự kiện thay đổi (CDC) theo thời gian thực.

Streaming / Clickstream Data (Dữ liệu hành vi): Viết script Python đóng vai trò Bot, liên tục sinh JSON payload giả lập thao tác lướt web và bắn với tốc độ cao trực tiếp vào Kafka Topic.

3. Năng Lực Kỹ Thuật Đạt Được
Dự án bao quát 5 nhóm kỹ năng cốt lõi, tương đương 1-2 năm kinh nghiệm thực chiến:

System Design: Thiết kế và vận hành kiến trúc Lambda/Kappa trên nền tảng Data Lakehouse (Medallion Architecture).

Big Data & Streaming: Quản lý luồng xử lý thời gian thực với Kafka/Debezium và làm chủ kỹ năng code PySpark (sử dụng Antigravity IDE) để giải quyết các bài toán hóc búa như lệch dữ liệu (Data Skew) và Shuffling.

Data Quality & Transformation: Làm sạch dữ liệu bằng SQL/dbt, áp dụng cơ chế Upsert nghiêm ngặt để đảm bảo tính lũy đẳng (Idempotency).

Automation & Deployment: Triển khai hạ tầng bằng Docker Compose, lên lịch luồng chạy tự động (DAGs) với Apache Airflow và thiết lập cảnh báo (Alerts).

Software Engineering: Ứng dụng kỹ năng tạo dữ liệu giả (Python Faker, Postman), truy vấn phân tích trên hệ thống OLAP (ClickHouse) và quản lý tiến độ qua GitHub/Linear.

4. Tiêu Chí Nghiệm Thu Khắc Nghiệt (Production-Ready Tests)
Để chứng minh đây là một hệ thống "thực chiến" thay vì dự án "ảo", hạ tầng phải vượt qua 5 bài test phá hủy:

Test Lũy đẳng (Idempotency): Kích hoạt Airflow chạy lại một Batch Job đã thành công của ngày hôm qua. Đạt yêu cầu: Dữ liệu trong ClickHouse giữ nguyên trạng thái nhờ cơ chế Merge, không bị nhân đôi.

Test Chịu lỗi (Fault Tolerance): Tắt "nóng" container Kafka hoặc Postgres trong 5 phút rồi bật lại. Đạt yêu cầu: Luồng dữ liệu không bị mất, Debezium và Spark tự động đọc tiếp từ offset bị đứt cuối cùng.

Test Dữ liệu Rác (Data Quality): Bắn các payload Clickstream bị lỗi (thiếu user_id hoặc giá tiền âm). Đạt yêu cầu: Pipeline không bị crash; hệ thống tự động rẽ nhánh, đẩy các dòng lỗi vào vùng cách ly (Quarantine).

Test Chịu tải (Performance): Nạp file 5GB từ Kaggle vào xử lý. Đạt yêu cầu: Các job PySpark không gặp lỗi Out of Memory (OOM) nhờ thiết kế chia Partition và xử lý Data Skew đúng chuẩn.

Test Triển khai (Reproducibility): Clone repo GitHub sang một máy tính hoàn toàn mới và chạy docker-compose up. Đạt yêu cầu: Toàn bộ cụm hệ thống tự động khởi chạy thành công từ A-Z mà không cần cấu hình thủ công.