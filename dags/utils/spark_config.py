"""
spark_config.py — Cấu hình Spark dùng chung cho tất cả Airflow DAGs.

Centralize toàn bộ đường dẫn và cấu hình để tránh hardcode lặp lại trong từng DAG.
"""

# ---------------------------------------------------------------
# RESOURCE POOL
# Tạo pool bằng CLI trước khi chạy DAG:
#   docker exec retailflow_airflow_scheduler \
#       airflow pools set spark_pool 3 "Spark resource pool"
# ---------------------------------------------------------------
SPARK_POOL = "spark_pool"

# Thư mục gốc chứa các Spark Job script BÊN TRONG container Airflow.
# Volume bind mount được khai báo trong docker-compose.yml:
#   ../spark-jobs:/opt/airflow/spark-jobs
SPARK_APP_DIR = "/opt/airflow/spark-jobs"

# Đường dẫn Python interpreter trong container Airflow
PYTHON_BIN = "python"

# ---------------------------------------------------------------
# SPARK SUBMIT COMMAND (Dùng chung cho mọi BashOperator)
# Pattern: python /opt/airflow/spark-jobs/silver/<script>.py [args...]
# ---------------------------------------------------------------
def spark_cmd(script_relative_path: str, *args: str) -> str:
    """
    Tạo câu lệnh bash để chạy Spark Job.

    Args:
        script_relative_path: Đường dẫn tương đối từ SPARK_APP_DIR.
                              VD: "silver/silver_exchange_rate.py"
        *args: Các tham số CLI truyền vào script. VD: "--table_name", "orders"

    Returns:
        Chuỗi bash command hoàn chỉnh.
    
    Example:
        spark_cmd("silver/silver_cdc_processor.py", "--table_name", "orders")
        → "python /opt/airflow/spark-jobs/silver/silver_cdc_processor.py --table_name orders"
    """
    args_str = " ".join(args)
    return f"{PYTHON_BIN} {SPARK_APP_DIR}/{script_relative_path} {args_str}".strip()
