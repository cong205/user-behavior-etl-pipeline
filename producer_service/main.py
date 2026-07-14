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
        
        # Tự đọc luồng dữ liệu 
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                
                if decoded_line.startswith('data: '):
                    data_str = decoded_line[6:] 
                    
                    try:
                        change = json.loads(data_str)
                        
                        # Trích xuất độ dài (Xử lý an toàn nếu 'length' bị null)
                        length_dict = change.get("length") or {}
                        length_old = length_dict.get("old") or 0
                        length_new = length_dict.get("new") or 0
                        
                        # Trích xuất revision
                        revision_dict = change.get("revision") or {}
                        
                        # Tạo payload với các trường được làm giàu dữ liệu
                        filtered_event = {
                            "id": change.get("id"),
                            "timestamp": change.get("timestamp"),
                            "type": change.get("type"),           # 'edit', 'new', 'log'...
                            "wiki": change.get("wiki"),           # VD: 'viwiki', 'enwiki'
                            
                            # Dữ liệu phục vụ Recommend & Topic Modeling
                            "title": change.get("title"),
                            "namespace": change.get("namespace"), # = 0 là bài viết bách khoa
                            "user": change.get("user"),
                            "is_bot": change.get("bot", False),
                            
                            # Dữ liệu văn bản phục vụ Topic Modeling
                            "comment": change.get("comment", ""),
                            "revision_new": revision_dict.get("new"),
                            
                            # Dữ liệu phục vụ Trending (Cường độ cập nhật)
                            "length_diff": length_new - length_old
                        }
                        
                        # Gửi data vào Kafka (Không dùng flush trong vòng lặp để tận dụng cơ chế batching)
                        producer.send(KAFKA_TOPIC, filtered_event)
                        
                        # In log để theo dõi tiến độ
                        print(f"Sent event: {filtered_event['title']} by {filtered_event['user']}")
                        
                    except json.JSONDecodeError:
                        continue
                        
    except Exception as e:
        print(f"Lỗi mạng hoặc kết nối bị ngắt: {e}")
    finally:
        # Đảm bảo xả hết dữ liệu còn đọng trong buffer trước khi đóng kết nối
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()