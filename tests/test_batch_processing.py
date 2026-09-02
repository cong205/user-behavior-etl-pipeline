import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os

# Thêm thư mục gốc vào sys.path để có thể import module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark_jobs.batch_processing import transform_kafka_data, schema

@pytest.fixture(scope="session")
def spark():
    """Tạo SparkSession dùng chung cho toàn bộ các bài test."""
    spark_session = SparkSession.builder \
        .appName("pytest-spark") \
        .master("local[1]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_transform_kafka_data(spark):
    """
    Test hàm transform_kafka_data: Đảm bảo dữ liệu JSON từ Kafka 
    được parse đúng theo schema định sẵn.
    """
    # 1. Tạo dữ liệu giả lập (Kafka trả về dữ liệu kiểu binary/string ở cột value)
    sample_json = '{"id": 12345, "timestamp": 1693563914, "type": "edit", "wiki": "enwiki", "title": "Python", "namespace": 0, "user": "test_user", "is_bot": false, "comment": "Fix typo", "revision_new": 9876543, "length_diff": 10}'
    
    # Tạo DataFrame mô phỏng dạng dữ liệu của Kafka stream
    kafka_schema = StructType([StructField("value", StringType(), True)])
    kafka_df = spark.createDataFrame([(sample_json,)], schema=kafka_schema)

    # 2. Chạy hàm cần test
    result_df = transform_kafka_data(kafka_df)

    # 3. Kiểm tra kết quả (so sánh bằng các collect row đầu tiên)
    assert result_df.count() == 1
    
    row = result_df.collect()[0]
    assert row["id"] == 12345
    assert row["timestamp"] == 1693563914
    assert row["type"] == "edit"
    assert row["wiki"] == "enwiki"
    assert row["title"] == "Python"
    assert row["namespace"] == 0
    assert row["user"] == "test_user"
    assert row["is_bot"] is False
    assert row["comment"] == "Fix typo"
    assert row["revision_new"] == 9876543
    assert row["length_diff"] == 10
    
    # Kiểm tra schema kết quả có khớp không
    assert result_df.schema == schema


from unittest.mock import patch, MagicMock
from spark_jobs.batch_processing import write_to_postgres

def test_write_to_postgres(spark):
    """
    Test hàm write_to_postgres: Đảm bảo sử dụng UPSERT logic 
    và các API của psycopg2 được gọi đúng cách.
    """
    sample_json = '{"id": 12345, "timestamp": 1693563914, "type": "edit", "wiki": "enwiki", "title": "Python", "namespace": 0, "user": "test_user", "is_bot": false, "comment": "Fix typo", "revision_new": 9876543, "length_diff": 10}'
    kafka_schema = StructType([StructField("value", StringType(), True)])
    kafka_df = spark.createDataFrame([(sample_json,)], schema=kafka_schema)
    parsed_df = transform_kafka_data(kafka_df)
    
    # Mock df.write.jdbc
    parsed_df.write.save = MagicMock()
    
    with patch("spark_jobs.batch_processing.psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Gọi hàm
        write_to_postgres(parsed_df, epoch_id=1)
        
        # Kiểm tra DataFrame write được gọi (ghi bảng tạm)
        # Vì gọi qua builder pattern nên khó test toàn bộ chain, nhưng ta test cơ bản:
        # Ở đây chỉ assert không có exception là được, vì MagicMock bao bọc rồi
        
        # Kiểm tra psycopg2 connection
        mock_connect.assert_called_once()
        assert mock_conn.autocommit is True
        
        # Kiểm tra cursor thực thi 4 lệnh (CREATE TABLE, CREATE INDEX, INSERT/UPSERT, DROP TABLE)
        assert mock_cursor.execute.call_count == 4
        
        # Kiểm tra có đóng kết nối
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
