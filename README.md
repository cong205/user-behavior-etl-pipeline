# User-Behavior-ETL-Pipeline

[![Apache Spark](https://img.shields.io/badge/Engine-Apache_Spark-E25A1B?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Broker-Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Orchestrator-Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Deployment-Docker_Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Language-Python_3-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)

Dự án triển khai hệ thống Data Pipeline tự động hóa nhằm thu thập và xử lý luồng sự kiện lớn theo hướng tiếp cận Micro-batching, giúp tối ưu hóa tài nguyên máy chủ:

Ingestion: Thu thập luồng sự kiện trực tiếp từ nguồn mở (Wikimedia EventStreams) và đưa vào hệ thống hàng đợi thông báo qua Apache Kafka.

Orchestration & Transform: Ứng dụng Apache Airflow làm trung tâm điều phối (Control Flow), định kỳ kích hoạt các tác vụ Apache Spark để đọc dữ liệu từ Kafka, thực hiện các quy trình ETL (làm sạch, chuẩn hóa định dạng, tính toán) theo từng lô.

Storage & Downstream: Nạp dữ liệu đã tinh chỉnh vào Cơ sở dữ liệu để phục vụ xuất báo cáo và làm đầu vào cho các thuật toán Machine Learning. Hệ thống được container hóa hoàn toàn bằng Docker, đảm bảo tính dễ dàng khi triển khai và mở rộng.
<img width="1367" height="339" alt="{008AD818-665B-4CE3-96A1-CC27763C3BA9}" src="https://github.com/user-attachments/assets/7b534eb3-9cd8-4521-9a3e-5cbbd52a4bd2" />

## Summary
### 1. System Overview
Hệ thống chịu trách nhiệm tự động hóa hoàn chỉnh luồng xử lý dữ liệu (**Data Pipeline**):
* Tự động trích xuất luồng dữ liệu sự kiện thời gian thực từ Kafka Producer.
* Biến đổi, làm phẳng dữ liệu JSON bán cấu trúc, áp dụng định nghĩa lược đồ chặt chẽ (`StructType`) và làm sạch dữ liệu bằng PySpark.
* Tải và đồng bộ hóa dữ liệu đã qua xử lý vào kho lưu trữ PostgreSQL.
* Quản lý vòng đời dữ liệu bằng cách tự động dọn dẹp các bản ghi cũ (giữ lại 1000 bản ghi mới nhất) để tối ưu hóa không gian lưu trữ.
* Tự động kiểm tra sức khỏe hệ thống (**health checks**) và sinh báo cáo kiểm toán số lượng bot/người thật thông qua Apache Airflow.

### 2. Technologies Used
* **Data Orchestration:** Apache Airflow
* **Data Processing:** Apache Spark (PySpark)
* **Message Broker (Streaming):** Apache Kafka, Zookeeper
* **Database:** PostgreSQL
* **Programming Language:** Python, SQL (psycopg2)
* **Containerization & Deployment:** Docker, Docker Compose
