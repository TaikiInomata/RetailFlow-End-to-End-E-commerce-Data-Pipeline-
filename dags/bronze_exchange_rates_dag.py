# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator
from datetime import timedelta
# pyrefly: ignore [missing-import]
import pendulum

DOC_MD="""## 📥 Bronze — Exchange Rates Daily

**Mục đích:** Kéo tỷ giá ngoại tệ từ REST API về vùng Bronze trên MinIO mỗi ngày.

**Lịch chạy:** `@daily` — 00:00 UTC

**Script thực thi:** `scripts/ingestion/fetch/fetch_exchange_rates.py`

**Output:** `s3://bronze-zone/exchange_rates/year=YYYY/month=MM/rates_YYYYMMDD.json`

**Lưu ý:** `catchup=False` và `max_active_runs=1` để tránh gọi API dư thừa và bị ban IP.
    """

# 1. Cấu hình mặc định (Default arguments) áp dụng cho mọi Task trong DAG
default_args = {
    'owner': 'novi',
    'depends_on_past': False,           # Job ngày hôm nay không cần đợi job hôm qua thành công
    'email_on_failure': False,          # Tắt gửi mail tạm thời để tránh rác log
    'retries': 3,                       # Thử lại tối đa 3 lần nếu API lỗi hoặc mạng chập chờn
    'retry_delay': timedelta(minutes=5), # Mỗi lần thử lại cách nhau 5 phút
}

# 2. Khởi tạo DAG
with DAG(
    dag_id='bronze_fetch_exchange_rates_daily',
    default_args=default_args,
    description='Kéo dữ liệu tỷ giá ngoại tệ từ REST API về vùng Bronze trên MinIO',
    schedule='@daily',                              # Chạy vào 00:00 UTC mỗi ngày
    start_date=pendulum.datetime(2026, 7, 20, tz="UTC"),  # pendulum xử lý timezone an toàn hơn datetime+tzinfo
    catchup=False,                                  # QUAN TRỌNG: Tắt chạy bù để tránh bị API ban IP
    max_active_runs=1,                              # Chỉ cho phép 1 run tại 1 thời điểm, tránh race condition với API
    tags=['bronze', 'ingestion', 'api'],            # Gắn tag để dễ lọc trên giao diện Airflow
    doc_md=DOC_MD,
) as dag:

    # 3. Định nghĩa Task
    # BashOperator: script chạy như subprocess tách biệt memory với Airflow Worker.
    # Phù hợp nhất cho context Docker Compose cục bộ — không gây Dependency Hell.
    fetch_api_task = BashOperator(
        task_id='run_fetch_rates_script',
        # Đường dẫn BÊN TRONG container — thư mục scripts được map qua volume bind mount
        bash_command='python /opt/airflow/scripts/ingestion/fetch/fetch_exchange_rates.py',
    )

    # DAG hiện tại chỉ có 1 task. Nếu mở rộng (vd: validate → upload → notify):
    # validate_task >> fetch_api_task >> notify_task
    fetch_api_task