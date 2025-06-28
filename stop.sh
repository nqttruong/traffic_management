#!/bin/bash

echo "[STOP] Dừng Spark (nếu còn)"
pkill -f consumer_spark.py

echo "[STOP] Dừng producer"
pkill -f producer.py

echo "[STOP] Dừng Docker containers"
docker compose down
