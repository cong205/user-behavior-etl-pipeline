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

import os

def get_postgres_connection():
    # Sử dụng os.environ để lấy cấu hình kết nối thay vì Airflow Connections 
    # để tránh lỗi khi connection postgres_default chưa được tạo.
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"), 
        port=os.environ.get("POSTGRES_PORT", "5432"), 
        database=os.environ.get("POSTGRES_DB", "airflow"), 
        user=os.environ.get("POSTGRES_USER", "airflow"), 
        password=os.environ.get("POSTGRES_PASSWORD", "airflow")
    )

def cleanup_old_records():
    try:
        conn = get_postgres_connection()
        conn.autocommit = True 
        cur = conn.cursor()
        
        # Xóa dữ liệu cũ hơn 24 giờ dựa trên timestamp (giả định timestamp là số nguyên milliseconds)
        # Nếu timestamp là seconds, sẽ cần điều chỉnh / 1000, 
        # nhưng thông thường log sự kiện để timestamp ms.
        delete_query = """
        DELETE FROM wikimedia_edits
        WHERE timestamp < (EXTRACT(EPOCH FROM (NOW() - INTERVAL '1 DAY')) * 1000);
        """
        
        cur.execute(delete_query)
        deleted_rows = cur.rowcount
        
        print("="*40)
        print("🧹 TIẾN TRÌNH DỌN DẸP DỮ LIỆU CŨ")
        if deleted_rows > 0:
            print(f"Đã dọn dẹp thành công. Số dòng cũ (>24h) bị xóa: {deleted_rows}")
        else:
            print("Không có dữ liệu nào cũ hơn 24 giờ.")
        print("="*40)
        
    except psycopg2.Error as e:
        print(f"Lỗi SQL trong quá trình dọn dẹp: {e}")
    finally:
        if 'conn' in locals() and conn:
            cur.close()
            conn.close()

def audit_and_report():
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM wikimedia_edits;")
        total_rows = cur.fetchone()[0]
        
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
    schedule='*/5 * * * *',  
    catchup=False,              
    tags=['ETL', 'Spark', 'Kafka', 'Postgres']
) as dag:

    check_kafka_health = BashOperator(
        task_id='verify_kafka_is_alive',
        bash_command='nc -z kafka 29092',
    )

    spark_transform_and_load = BashOperator(
        task_id='spark_clean_and_load_postgres',
        bash_command=(
            'spark-submit '
            '--master spark://spark-master:7077 '
            '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 '
            '/opt/airflow/spark_jobs/batch_processing.py'
        )
    )

    cleanup_old_data = PythonOperator(
        task_id='cleanup_old_postgres_data',
        python_callable=cleanup_old_records
    )

    audit_pipeline = PythonOperator(
        task_id='audit_and_report',
        python_callable=audit_and_report
    )

    check_kafka_health >> spark_transform_and_load >> cleanup_old_data >> audit_pipeline