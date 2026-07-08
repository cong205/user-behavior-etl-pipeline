# User-Behavior-ETL-Pipeline
Dự án triển khai hệ thống Data Pipeline tự động hóa nhằm thu thập và xử lý luồng sự kiện lớn theo hướng tiếp cận Micro-batching, giúp tối ưu hóa tài nguyên máy chủ:

Ingestion: Thu thập luồng sự kiện trực tiếp từ nguồn mở (Wikimedia EventStreams) và đưa vào hệ thống hàng đợi thông báo qua Apache Kafka.

Orchestration & Transform: Ứng dụng Apache Airflow làm trung tâm điều phối (Control Flow), định kỳ kích hoạt các tác vụ Apache Spark để đọc dữ liệu từ Kafka, thực hiện các quy trình ETL (làm sạch, chuẩn hóa định dạng, tính toán) theo từng lô.

Storage & Downstream: Nạp dữ liệu đã tinh chỉnh vào Cơ sở dữ liệu để phục vụ xuất báo cáo và làm đầu vào cho các thuật toán Machine Learning. Hệ thống được container hóa hoàn toàn bằng Docker, đảm bảo tính dễ dàng khi triển khai và mở rộng.
<img width="1367" height="339" alt="{008AD818-665B-4CE3-96A1-CC27763C3BA9}" src="https://github.com/user-attachments/assets/7b534eb3-9cd8-4521-9a3e-5cbbd52a4bd2" />
