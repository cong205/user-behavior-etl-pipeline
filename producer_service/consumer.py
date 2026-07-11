import json
from kafka import KafkaConsumer

def main():
    print("Đang khởi động Consumer...")
    
    # Khởi tạo Kafka Consumer
    consumer = KafkaConsumer(
        'wikimedia-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest', # Tham số này giúp đọc lại toàn bộ dữ liệu từ đầu
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Đã kết nối! Đang lấy dữ liệu từ topic 'wikimedia-events'...\n")
    
    # Lắng nghe dữ liệu
    for message in consumer:
        data = message.value
        print(f"📥 [ĐÃ NHẬN] Bài viết: {data['title']} | User: {data['user']}")

if __name__ == "__main__":
    main()