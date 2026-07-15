from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'owner': 'nguyen_dinh_cong',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 14), 
    'retries': 1,
    'retry_delay': timedelta(minutes=3), 
}

def keep_latest_1000_records():
    # ... (Giữ nguyên nội dung hàm dọn dẹp bạn đã viết ở bước trước) ...
    pass

# HÀM MỚI: Kiểm toán và Báo cáo
def audit_and_report():
    try:
        conn = psycopg2.connect(
            host="postgres", port="5432", database="airflow", user="airflow", password="airflow"
        )
        cur = conn.cursor()
        
        # Đếm tổng số bản ghi hiện có
        cur.execute("SELECT COUNT(*) FROM wikimedia_edits;")
        total_rows = cur.fetchone()[0]
        
        # Đếm số lượng đóng góp của người thật vs Bot
        cur.execute("SELECT is_bot, COUNT(*) FROM wikimedia_edits GROUP BY is_bot;")
        stats = cur.fetchall()
        
        print("="*40)
        print("📊 BÁO CÁO PIPELINE THÀNH CÔNG")
        print(f"Tổng số bản ghi trong kho: {total_rows}")
        for stat in stats:
            user_type = "🤖 BOT" if stat[0] else "👤 NGƯỜI THẬT"
            print(f" - {user_type}: {stat[1]} lượt")
        print("="*40)
        
    except psycopg2.Error as e:
        print(f"Lỗi khi kiểm toán: {e}")
    finally:
        if 'conn' in locals() and conn:
            cur.close()
            conn.close()

with DAG(
    'user_behavior_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline: Health Check -> Spark -> Postgres -> Audit',
    schedule_interval='*/5 * * * *', 
    catchup=False,              
    tags=['ETL', 'Spark', 'Kafka', 'Postgres']
) as dag:

    # NÚT ĐẦU: Kiểm tra xem cổng 29092 của Kafka có đang mở không. 
    # Nếu Kafka chết, lệnh 'nc' (netcat) sẽ thất bại, Airflow sẽ dừng DAG ngay lập tức để báo lỗi.
    check_kafka_health = BashOperator(
        task_id='verify_kafka_is_alive',
        bash_command='nc -z kafka 29092',
    )

    spark_transform_and_load = BashOperator(
        task_id='spark_clean_and_load_postgres',
        bash_command='spark-submit --master spark://spark-master:7077 /opt/airflow/spark_jobs/batch_processing.py',
    )

    cleanup_old_data = PythonOperator(
        task_id='cleanup_old_postgres_data',
        python_callable=keep_latest_1000_records
    )

    # NÚT CUỐI: Kiểm toán kho dữ liệu
    audit_pipeline = PythonOperator(
        task_id='audit_and_report',
        python_callable=audit_and_report
    )

    # Luồng chạy cực kỳ chặt chẽ:
    check_kafka_health >> spark_transform_and_load >> cleanup_old_data >> audit_pipeline