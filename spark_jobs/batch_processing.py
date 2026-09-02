import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

# 1. CẤU HÌNH KẾT NỐI DATABASE POSTGRES (Bên trong Docker)
# Lấy từ biến môi trường, fallback về giá trị mặc định cho an toàn
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "airflow")
PG_USER = os.environ.get("POSTGRES_USER", "airflow")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# Cấu hình đường dẫn lưu "Đánh dấu trang" (Checkpoint) 
CHECKPOINT_DIR = "/opt/airflow/spark_jobs/checkpoints/wikimedia_events"

schema = StructType([
    StructField("id", LongType(), True),
    StructField("timestamp", LongType(), True),
    StructField("type", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("title", StringType(), True),
    StructField("namespace", LongType(), True),
    StructField("user", StringType(), True),
    StructField("is_bot", BooleanType(), True),
    StructField("comment", StringType(), True),
    StructField("revision_new", LongType(), True),
    StructField("length_diff", LongType(), True)
])

def transform_kafka_data(kafka_df):
    """
    Hàm biến đổi dữ liệu Kafka: chuyển đổi chuỗi JSON thành DataFrame với schema cụ thể.
    Tách ra riêng để dễ dàng viết unit test.
    """
    return kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

def write_to_postgres(df, epoch_id):
    batch_count = df.count()
    print(f"🔄 Đang xử lý Batch {epoch_id} với {batch_count} bản ghi...")
    
    if batch_count > 0:
        # Tên bảng tạm
        temp_table = f"wikimedia_edits_staging_{epoch_id}"
        
        # 1. Ghi dữ liệu vào bảng tạm staging
        df.write \
            .format("jdbc") \
            .option("url", PG_URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", temp_table) \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .mode("overwrite") \
            .save()
            
        print(f"✅ Đã ghi vào bảng tạm {temp_table}.")
        
        # 2. Sử dụng psycopg2 để thực hiện UPSERT (Idempotent Write)
        conn = None
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            # Đảm bảo bảng chính đã tồn tại và id là Primary Key 
            # (Thực tế nên tạo PK sẵn trong script init DB, nhưng tạo đây cho an toàn)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wikimedia_edits (
                    id BIGINT PRIMARY KEY,
                    timestamp BIGINT,
                    type TEXT,
                    wiki TEXT,
                    title TEXT,
                    namespace BIGINT,
                    "user" TEXT,
                    is_bot BOOLEAN,
                    comment TEXT,
                    revision_new BIGINT,
                    length_diff BIGINT
                );
            """)
            
            # Đảm bảo có Index trên timestamp để tối ưu truy vấn dọn dẹp
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_wikimedia_edits_timestamp 
                ON wikimedia_edits(timestamp);
            """)
            
            # Thực thi MERGE / UPSERT
            upsert_query = f"""
                INSERT INTO wikimedia_edits 
                SELECT * FROM {temp_table}
                ON CONFLICT (id) DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    type = EXCLUDED.type,
                    wiki = EXCLUDED.wiki,
                    title = EXCLUDED.title,
                    namespace = EXCLUDED.namespace,
                    "user" = EXCLUDED."user",
                    is_bot = EXCLUDED.is_bot,
                    comment = EXCLUDED.comment,
                    revision_new = EXCLUDED.revision_new,
                    length_diff = EXCLUDED.length_diff;
            """
            cur.execute(upsert_query)
            
            # Xóa bảng tạm
            cur.execute(f"DROP TABLE {temp_table};")
            print(f"✅ Đã UPSERT thành công {batch_count} bản ghi vào wikimedia_edits.")
            
        except psycopg2.Error as e:
            print(f"❌ Lỗi khi thực hiện UPSERT: {e}")
        finally:
            if conn:
                cur.close()
                conn.close()
    else:
        print("Trống! Không có dữ liệu mới.")

def main():
    print("🚀 Đang khởi tạo Spark Session cho tác vụ Airflow...")
    spark = SparkSession.builder \
        .appName("Airflow_KafkaToPostgres_Batch") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("📡 Đang kết nối tới Kafka topic 'wikimedia-events'...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "wikimedia-events") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = transform_kafka_data(kafka_df)

    print("⚙️ Bắt đầu quét dữ liệu tồn đọng trong Kafka...")
    
    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()
    
    print("🎉 ĐÃ HOÀN TẤT! Toàn bộ dữ liệu mới đã được nạp. Spark chuẩn bị tắt...")

if __name__ == "__main__":
    main()