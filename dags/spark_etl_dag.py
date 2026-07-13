from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Cấu hình mặc định cho các tác vụ trong DAG
default_args = {
    'owner': 'nguyen_dinh_cong',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 12), # Lùi lại 1 ngày so với hiện tại để DAG có thể kích hoạt ngay
    'retries': 1,
    'retry_delay': timedelta(minutes=3), # Thử lại sau 3 phút nếu có lỗi mạng
}

# Khởi tạo DAG
with DAG(
    'user_behavior_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline: Dữ liệu Kafka -> Làm sạch bằng Spark -> Nạp vào Postgres',
    schedule_interval='@daily', # Lịch chạy: mỗi ngày một lần vào lúc 00:00
    catchup=False,              # Bỏ qua các ngày trong quá khứ lúc hệ thống chưa chạy
    tags=['ETL', 'Spark', 'Kafka', 'Postgres']
) as dag:

    # Task 1: Nút đại diện cho quá trình Ingestion (Kafka Producer đang chạy ngầm 24/7)
    # Dùng EmptyOperator vì Airflow không cần khởi động lại Producer mỗi ngày
    ingestion_step = EmptyOperator(
        task_id='kafka_streaming_is_active'
    )

    # Task 2: Tác vụ Lõi - Kích hoạt PySpark (Gom batch, Transform và Load)
    # Sử dụng lệnh bash để gọi script Python mà chúng ta đã cấu hình Micro-batch
    spark_transform_and_load = BashOperator(
        task_id='spark_clean_and_load_postgres',
        bash_command='python /opt/airflow/spark_jobs/batch_processing.py',
    )

    # Task 3: Đánh dấu hoàn thành chu trình ETL
    pipeline_success = EmptyOperator(
        task_id='etl_completed_successfully'
    )

    # THIẾT LẬP LUỒNG CHẠY (Mũi tên điều phối)
    # Dữ liệu vào Kafka -> Xử lý bằng Spark & Đẩy Postgres -> Thành công
    ingestion_step >> spark_transform_and_load >> pipeline_success