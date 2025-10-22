import streamlit as st
import pandas as pd
import altair as alt
import json
from kafka import KafkaConsumer
import pydeck as pdk
from datetime import datetime
import time
import os
import sys
sys.path.append(os.path.join(os.getcwd(), "spark"))
from predict_traffic_from_mongo import predict_traffic
# ===============================
# Cấu hình Streamlit
# ===============================
st.set_page_config(page_title="🚦 Traffic Analytics & Forecast", layout="wide")

st.title("🚗 Real-time Traffic Dashboard + 15-minute Forecast")

# Dùng 2 cột chính: Trái = Realtime | Phải = Dự đoán
col1, col2 = st.columns([2, 1])

# ===============================
# CỘT 1 — REAL-TIME (Kafka)
# ===============================
with col1:
    st.header("📡 Luồng Dữ liệu Giao thông Thời gian thực (Kafka)")

    placeholder_realtime = st.empty()
    data_list = []

    consumer = KafkaConsumer(
        'traffic_data',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    for message in consumer:
        msg = message.value
        segment = msg.get("flowSegmentData", {})
        coords = segment.get("coordinates", {}).get("coordinate", [])

        if not coords:
            continue

        current_speed = segment.get("currentSpeed")
        free_speed = segment.get("freeFlowSpeed")
        confidence = segment.get("confidence")
        road_closed = segment.get("roadClosure", False)
        timestamp = datetime.now()
        lat = coords[0].get("latitude", 21.03)
        lon = coords[0].get("longitude", 105.85)

        if current_speed is None or free_speed in (0, None):
            continue

        data_list.append({
            "timestamp": timestamp,
            "current_speed": current_speed,
            "free_speed": free_speed,
            "confidence": confidence,
            "road_closed": road_closed,
            "lat": lat,
            "lon": lon
        })

        df = pd.DataFrame(data_list)
        df["congestion"] = 1 - (df["current_speed"] / df["free_speed"])
        df["status"] = pd.cut(
            df["congestion"],
            bins=[-0.1, 0.2, 0.5, 0.8, 1.1],
            labels=["Thông thoáng", "Tăng nhẹ", "Đông", "Tắc nghẽn"]
        )

        with placeholder_realtime.container():
            st.subheader("📈 Biểu đồ tốc độ")
            chart = alt.Chart(df).mark_line().encode(
                x="timestamp:T",
                y=alt.Y("current_speed:Q", title="Tốc độ (km/h)"),
                tooltip=["timestamp", "current_speed", "free_speed", "congestion"]
            ).properties(height=250)
            st.altair_chart(chart, use_container_width=True)

            st.subheader("📍 Bản đồ vị trí mới nhất")
            tooltip = {
                "html": "Trạng thái: {status}<br>Vĩ độ: {lat}<br>Kinh độ: {lon}",
                "style": {"backgroundColor": "steelblue", "color": "white"}
            }
            st.pydeck_chart(pdk.Deck(
                map_style='mapbox://styles/mapbox/streets-v12',
                initial_view_state=pdk.ViewState(
                    latitude=df["lat"].mean(),
                    longitude=df["lon"].mean(),
                    zoom=12,
                    pitch=40,
                ),
                layers=[
                    pdk.Layer(
                        'ScatterplotLayer',
                        data=df.tail(10),
                        get_position='[lon, lat]',
                        get_color='[200, 30, 0, 160]',
                        get_radius=200,
                        pickable=True
                    ),
                ],
                tooltip=tooltip
            ))

            st.subheader("📊 Dữ liệu mới nhất")
            st.dataframe(df.tail(5)[["timestamp", "current_speed", "free_speed", "confidence", "status"]])

        # gọi cập nhật dự đoán song song (mỗi vài vòng lặp)
        if len(data_list) % 10 == 0:
            st.session_state["update_forecast"] = True

# ===============================
# CỘT 2 — FORECAST (MongoDB)
# ===============================
with col2:
    st.header("🔮 Dự đoán Giao thông 15 phút tới")

    placeholder_forecast = st.empty()

    if "update_forecast" not in st.session_state:
        st.session_state["update_forecast"] = True

    if st.session_state["update_forecast"]:
        result = predict_traffic()

        if not result:
            st.warning("⚠️ Chưa có dữ liệu trong MongoDB.")
        else:
            current = result["current_density"]
            pred = result["predicted_density"]
            last = result["last_update"]

            if pred > 0.8:
                status = "🚨 Tắc đường"
                color = "red"
            elif pred > 0.4:
                status = "⚠️ Giao thông trung bình"
                color = "orange"
            else:
                status = "✅ Thông thoáng"
                color = "green"

            with placeholder_forecast.container():
                st.markdown(f"**Cập nhật:** {last}")
                st.metric(label="Mật độ hiện tại", value=f"{current:.2f}")
                st.metric(label="Dự đoán 15 phút tới", value=f"{pred:.2f}")
                st.markdown(f"<h3 style='color:{color}'>{status}</h3>", unsafe_allow_html=True)
                st.progress(pred)
                st.info("Dữ liệu cập nhật tự động từ MongoDB.")
        
        st.session_state["update_forecast"] = False

