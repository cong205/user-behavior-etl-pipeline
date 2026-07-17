FROM apache/airflow:2.8.1

# 1. Chuyển sang quyền root để cài đặt Java
USER root
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

# 2. Trở lại quyền airflow để cài PySpark đúng phiên bản 3.5.0
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.0