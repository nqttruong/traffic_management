from kafka import KafkaConsumer
import json
import pandas as pd
import time
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg') 

# ===============================
# Kafka Consumer Cấu hình
# ===============================
consumer = KafkaConsumer(
    'traffic_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

data_list = []

print("🚦 Listening to traffic_data topic...")

# ===============================
# Nhận dữ liệu Kafka và phân tích
# ===============================
for message in consumer:
    msg = message.value

    try:
        segment = msg.get("flowSegmentData", {})
        current_speed = segment.get("currentSpeed")
        free_speed = segment.get("freeFlowSpeed")
        current_time = segment.get("currentTravelTime")
        free_time = segment.get("freeFlowTravelTime")
        confidence = segment.get("confidence")
        road_closed = segment.get("roadClosure", False)
        lat = segment.get("latitude")  # fallback nếu không có
        lon = segment.get("longitude")
        timestamp = time.time()

        # Bỏ qua nếu thiếu thông tin quan trọng
        if current_speed is None or free_speed in (0, None):
            continue

        # Lưu bản ghi
        data_list.append({
            "timestamp": pd.to_datetime(timestamp, unit='s'),
            "current_speed": current_speed,
            "free_speed": free_speed,
            "current_time": current_time,
            "free_time": free_time,
            "confidence": confidence,
            "road_closed": road_closed,
            "lat": lat,
            "lon": lon,
        })

        # Tạo DataFrame
        df = pd.DataFrame(data_list)

        # Phân tích tắc nghẽn
        df["congestion"] = 1 - (df["current_speed"] / df["free_speed"])
        df["status"] = pd.cut(
            df["congestion"],
            bins=[-0.1, 0.2, 0.5, 0.8, 1.1],
            labels=["Thông thoáng", "Tăng nhẹ", "Đông", "Tắc nghẽn"]
        )

        # In bản ghi mới nhất
        print("\n📊 Bản ghi mới nhất:")
        print(df.tail(5)[["lat", "lon", "timestamp", "current_speed", "free_speed", "congestion", "status"]].to_string(index=False))

    except Exception as e:
        print("⚠️ Lỗi khi xử lý message:", e)
