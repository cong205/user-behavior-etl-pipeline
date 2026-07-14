import json
from kafka import KafkaConsumer

# Cấu hình
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092'] 
KAFKA_TOPIC = 'wikimedia-events'

def main():
    print("Đang khởi động Consumer...")
    
    try:
        # Khởi tạo Kafka Consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest', # Đổi từ earliest sang latest
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"Đã kết nối! Đang lấy dữ liệu từ topic '{KAFKA_TOPIC}'...\n")
        print("-" * 60)
        
        # Lắng nghe dữ liệu
        for message in consumer:
            data = message.value
            
            # Sử dụng "or" để an toàn tuyệt đối với các giá trị null/None
            title = data.get('title') or 'Unknown'
            user = data.get('user') or 'Unknown'
            wiki = data.get('wiki') or 'N/A'
            event_type = data.get('type') or 'N/A'
            is_bot = "🤖 BOT" if data.get('is_bot') else "👤 NGƯỜI"
            length_diff = data.get('length_diff') or 0
            
            # Định dạng hiển thị mức độ thay đổi
            diff_str = f"+{length_diff}" if length_diff > 0 else str(length_diff)
            
            print(f"📥 [{wiki.upper()}] {event_type.upper()} | {title}")
            print(f"   ┣━ Tác giả: {user} ({is_bot})")
            print(f"   ┗━ Thay đổi: {diff_str} ký tự")
            print("-" * 60)
            
    except KeyboardInterrupt:
        print("\nĐã dừng Consumer (Ctrl+C).")
    except Exception as e:
        print(f"Lỗi hệ thống: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            print("Đã đóng kết nối Consumer an toàn.")

if __name__ == "__main__":
    main()