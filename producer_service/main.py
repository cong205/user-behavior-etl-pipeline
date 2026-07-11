import json
import requests
from kafka import KafkaProducer

# Cấu hình
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'wikimedia-events'

def main():
    # Khởi tạo Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Producer đã kết nối tới Kafka.")

    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    
    # 1. Bổ sung User-Agent để Wikipedia không chặn kết nối
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }
    
    print("Đang kết nối tới Wikimedia stream...")
    try:
        # 2. Thêm timeout để không bị treo vĩnh viễn nếu mạng kẹt
        response = requests.get(url, stream=True, headers=headers, timeout=10)
        
        # In ra mã trạng thái để dễ bắt bệnh (200 là thành công)
        print(f"Mã trạng thái HTTP từ Wikipedia: {response.status_code}")
        
        if response.status_code != 200:
            print("Lỗi: Máy chủ Wikipedia từ chối kết nối!")
            return

        print("Đang xả dữ liệu xuống Kafka...")
        
        # 3. Tự đọc luồng dữ liệu (bỏ qua sseclient)
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                
                # Trong chuẩn SSE, dữ liệu thực tế luôn nằm sau chữ 'data: '
                if decoded_line.startswith('data: '):
                    data_str = decoded_line[6:] # Cắt bỏ chữ 'data: ' ở đầu
                    
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
                        producer.flush() # Ép đẩy ngay
                        print(f"Sent event: {filtered_event['title']} by {filtered_event['user']}", flush=True)
                        
                    except json.JSONDecodeError:
                        # Bỏ qua nếu có dòng nào không phải là JSON hợp lệ
                        continue
                        
    except Exception as e:
        print(f"Lỗi mạng hoặc kết nối bị ngắt: {e}")

if __name__ == "__main__":
    main()