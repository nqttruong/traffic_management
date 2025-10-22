import requests
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
from pymongo import MongoClient

# ==== Cấu hình ====
TOMTOM_API_KEY = 'BFAjKpzNZ8LJYXxEGQ0oLjaK8tQhQKBT'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = "traffic_data"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "traffic_monitor"
MONGO_COLLECTION = "tomtom_data"

# ==== Kết nối MongoDB ====
try:
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    mongo_col = mongo_db[MONGO_COLLECTION]
    print("✅ Đã kết nối MongoDB.")
except Exception as e:
    print("❌ Lỗi kết nối MongoDB:", e)
    exit(1)

# ==== Khởi tạo Producer Kafka ====
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# === Hàm lấy dữ liệu từ TomTom ===
def fetch_traffic_data():
    points = [
        (10.7758, 106.7004)  # ví dụ trung tâm TP.HCM
    ]
    for lat, lon in points:
        url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={lat},{lon}&unit=KMPH&key={TOMTOM_API_KEY}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                data["timestamp"] = datetime.utcnow().isoformat()
                data["location"] = {"lat": lat, "lon": lon}
                return data
            else:
                print(f"❌ Failed to fetch TomTom data: {response.status_code}")
                return None
        except Exception as e:
            print("❌ Exception khi gọi API:", e)
            return None

# ==== Callback xử lý gửi thành công/thất bại ====
def on_send_success(record_metadata):
    print(f"✔️ Sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    print("❌ Send failed:", excp)

# ==== Gửi dữ liệu mỗi 10 giây ====
if __name__ == "__main__":
    print("🚀 Bắt đầu gửi dữ liệu từ TomTom vào Kafka và MongoDB...")
    while True:
        traffic_data = fetch_traffic_data()
        if traffic_data:
            # 1️⃣ Gửi lên Kafka
            future = producer.send(KAFKA_TOPIC, value=traffic_data)
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)
            producer.flush()

            # 2️⃣ Lưu vào MongoDB
            try:
                mongo_col.insert_one(traffic_data)
                print(f"💾 Đã lưu dữ liệu vào MongoDB ({traffic_data['location']})")
            except Exception as e:
                print("❌ Lỗi khi lưu MongoDB:", e)
        else:
            print("⚠️ Không có dữ liệu để gửi.")
        time.sleep(10)
