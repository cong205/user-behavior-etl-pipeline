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
    try:
        # 1. Khởi tạo kết nối tới Database (giống với cấu hình ở hàm audit)
        conn = psycopg2.connect(
            host="postgres", port="5432", database="airflow", user="airflow", password="airflow"
        )
        
        # Bật chế độ tự động lưu (commit) ngay sau khi chạy lệnh xóa
        conn.autocommit = True 
        cur = conn.cursor()
        
        # 2. Câu lệnh SQL dọn dẹp dữ liệu cũ
        # CHÚ Ý: Hãy thay 'timestamp' bằng tên cột thời gian hoặc cột ID tự tăng trong bảng của bạn.
        delete_query = """
        DELETE FROM wikimedia_edits
        WHERE ctid NOT IN (
            SELECT ctid
            FROM wikimedia_edits
            ORDER BY timestamp DESC
            LIMIT 1000
        );
        """
        
        # Thực thi lệnh xóa
        cur.execute(delete_query)
        
        # Lấy số lượng dòng đã bị xóa để ghi log
        deleted_rows = cur.rowcount
        
        print("="*40)
        print("🧹 TIẾN TRÌNH DỌN DẸP DỮ LIỆU")
        if deleted_rows > 0:
            print(f"Đã dọn dẹp thành công. Số dòng cũ bị xóa: {deleted_rows}")
        else:
            print("Kho dữ liệu hiện có dưới 1000 dòng. Không cần xóa thêm.")
        print("="*40)
        
    except psycopg2.Error as e:
        print(f"Lỗi SQL trong quá trình dọn dẹp: {e}")
    finally:
        # 3. Đóng kết nối an toàn để giải phóng tài nguyên
        if 'conn' in locals() and conn:
            cur.close()
            conn.close()

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
    schedule='*/5 * * * *',  # Đã sửa thành "schedule" cho Airflow 2.8+
    catchup=False,              
    tags=['ETL', 'Spark', 'Kafka', 'Postgres']
) as dag:

    # NÚT ĐẦU: Kiểm tra xem cổng 29092 của Kafka có đang mở không. 
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
        python_callable=keep_latest_1000_records
    )

    # NÚT CUỐI: Kiểm toán kho dữ liệu
    audit_pipeline = PythonOperator(
        task_id='audit_and_report',
        python_callable=audit_and_report
    )

    # Luồng chạy cực kỳ chặt chẽ:
    check_kafka_health >> spark_transform_and_load >> cleanup_old_data >> audit_pipeline