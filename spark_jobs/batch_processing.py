from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. CẤU HÌNH KẾT NỐI DATABASE POSTGRES (Bên trong Docker)
PG_URL = "jdbc:postgresql://postgres:5432/airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

# Cấu hình đường dẫn lưu "Đánh dấu trang" (Checkpoint) 
# Chú ý: Đường dẫn này nằm trong thư mục spark_jobs đã được ánh xạ (mount) ra ngoài Docker
CHECKPOINT_DIR = "/opt/airflow/spark_jobs/checkpoints/wikimedia_events"

# Khai báo cấu trúc bảng (Schema) tương ứng với dữ liệu JSON từ Producer
schema = StructType([
    StructField("user", StringType(), True),
    StructField("title", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("server_name", StringType(), True),
    StructField("wiki", StringType(), True)
])

def write_to_postgres(df, epoch_id):
    """
    Hàm này được gọi để ghi cụm dữ liệu (micro-batch) vào Postgres.
    """
    # Đếm số lượng bản ghi trong batch này để in ra log cho Airflow dễ theo dõi
    batch_count = df.count()
    print(f"🔄 Đang xử lý Batch {epoch_id} với {batch_count} bản ghi...")
    
    if batch_count > 0:
        df.write \
            .format("jdbc") \
            .option("url", PG_URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "wikimedia_edits") \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .mode("append") \
            .save()
        print(f"✅ Đã ghi thành công {batch_count} bản ghi vào Postgres.")
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
    # Đổi startingOffsets thành "earliest" để lần chạy ĐẦU TIÊN nó quét sạch dữ liệu cũ
    # Các lần chạy sau nó sẽ tự động dùng vị trí đã lưu trong Checkpoint
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "wikimedia-events") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    print("⚙️ Bắt đầu quét dữ liệu tồn đọng trong Kafka...")
    
    # THAY ĐỔI QUAN TRỌNG NHẤT DÀNH CHO AIRFLOW NẰM Ở ĐÂY
    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(availableNow=True) \
        .start()

    # Lệnh này sẽ block luồng cho đến khi xử lý hết dữ liệu "hiện có" thì tự ngắt
    query.awaitTermination()
    
    print("🎉 ĐÃ HOÀN TẤT! Toàn bộ dữ liệu mới đã được nạp. Spark chuẩn bị tắt...")

if __name__ == "__main__":
    main()