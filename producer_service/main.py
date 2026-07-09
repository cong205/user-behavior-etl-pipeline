import json
from kafka import KafkaProducer
from sseclient import SSEClient as EventSource

# Cấu hình
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092'] # Lưu ý: nếu chạy ngoài Docker thì để localhost:9092
KAFKA_TOPIC = 'wikimedia-events'

def main():
    # Khởi tạo Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Producer đã kết nối tới Kafka.")

    # URL nguồn dữ liệu Wikimedia
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    
    # Lắng nghe luồng dữ liệu
    for event in EventSource(url):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
                
                # Chúng ta chỉ lấy một vài trường cần thiết để làm ví dụ
                filtered_event = {
                    "user": change.get("user"),
                    "title": change.get("title"),
                    "timestamp": change.get("timestamp"),
                    "server_name": change.get("server_name"),
                    "wiki": change.get("wiki")
                }
                
                # Đẩy vào Kafka
                producer.send(KAFKA_TOPIC, filtered_event)
                print(f"Sent event: {filtered_event['title']} by {filtered_event['user']}")
                
            except Exception as e:
                print(f"Lỗi khi parse dữ liệu: {e}")

if __name__ == "__main__":
    main()