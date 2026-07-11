import json
import requests
from kafka import KafkaProducer

# Cấu hình
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
KAFKA_TOPIC = 'wikimedia-events'

def main():
    # Khởi tạo Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Producer đã kết nối tới Kafka.")

    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }
    
    print("Đang kết nối tới Wikimedia stream...")
    try:
        response = requests.get(url, stream=True, headers=headers, timeout=10)
        
        print(f"Mã trạng thái HTTP từ Wikipedia: {response.status_code}")
        
        if response.status_code != 200:
            print("Lỗi: Máy chủ Wikipedia từ chối kết nối!")
            return

        print("Đang xả dữ liệu xuống Kafka...")
        
        # 3. Tự đọc luồng dữ liệu 
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                
                if decoded_line.startswith('data: '):
                    data_str = decoded_line[6:] 
                    
                    try:
                        change = json.loads(data_str)
                        
                        filtered_event = {
                            "user": change.get("user"),
                            "title": change.get("title"),
                            "timestamp": change.get("timestamp"),
                            "server_name": change.get("server_name"),
                            "wiki": change.get("wiki")
                        }
                        
                        producer.send(KAFKA_TOPIC, filtered_event)
                        producer.flush() 
                        print(f"Sent event: {filtered_event['title']} by {filtered_event['user']}", flush=True)
                        
                    except json.JSONDecodeError:
                        continue
                        
    except Exception as e:
        print(f"Lỗi mạng hoặc kết nối bị ngắt: {e}")

if __name__ == "__main__":
    main()