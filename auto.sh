#!/bin/bash

# Dừng mọi thứ khi Ctrl+C
trap 'echo "🔴 Stopping services..."; docker compose down; pkill -f producer.py; pkill -f app.py; exit 0' INT

echo "🚀 Starting Docker containers (Kafka, Zookeeper...)"
docker compose up -d

# Chờ Kafka & Zookeeper sẵn sàng (tùy máy, có thể điều chỉnh)
echo "⏳ Waiting for Kafka to be ready..."
sleep 10

# Chạy Producer ở nền
echo "📡 Starting Kafka producer (TomTom API)"
python3 scripts/producer.py &

# Chạy Spark Consumer
echo "⚙️ Starting Spark consumer..."
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  spark/test_kafka.py &


## Chạy Flask Dashboard
#echo "📊 Starting Flask dashboard..."
#python3 app.py &

# Chờ vô hạn để giữ script hoạt động cho tới khi Ctrl+C
wait
