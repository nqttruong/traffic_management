@echo off
title Traffic Stream System
echo ============================================================
echo 🚀 Starting Docker containers (Kafka, Zookeeper...)
echo ============================================================

:: Khởi động Docker Compose
docker compose up -d

echo.
echo ⏳ Waiting for Kafka to be ready...
timeout /t 10 /nobreak >nul

:: Khởi động Kafka Producer (TomTom API)
echo.
echo 📡 Starting Kafka producer (TomTom API)
start "Producer" python scripts\producer.py

:: Khởi động Spark Consumer
echo.
echo ⚙️ Starting Spark consumer...
start "Spark Consumer" spark-submit ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 ^
  spark\test_kafka.py

:: Khởi động Streamlit Dashboard
echo.
echo 📊 Starting Streamlit dashboard...
start "Streamlit App" streamlit run dashboard_streamlit.py --server.port 8501

echo.
echo ✅ All services are running. Press Ctrl+C to stop.
echo ============================================================

:LOOP
timeout /t 5 >nul
goto LOOP
