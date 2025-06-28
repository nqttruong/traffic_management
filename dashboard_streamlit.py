import streamlit as st
import pandas as pd
import altair as alt
import json
from kafka import KafkaConsumer
import pydeck as pdk
from datetime import datetime

st.set_page_config(layout="wide")
st.title("🚦 Real-time Traffic Dashboard")

placeholder = st.empty()

# ===============================
# Kafka Consumer
# ===============================
consumer = KafkaConsumer(
    'traffic_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

data_list = []

# ===============================
# Dashboard Loop
# ===============================
for message in consumer:
    msg = message.value
    segment = msg.get("flowSegmentData", {})
    coords = segment.get("coordinates", {}).get("coordinate", [])

    if not coords:
        continue  # bỏ qua nếu không có tọa độ

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

    # ===============================
    # Hiển thị Dashboard
    # ===============================
    with placeholder.container():
        st.subheader("📈 Biểu đồ tốc độ giao thông")
        chart = alt.Chart(df).mark_line().encode(
            x="timestamp:T",
            y=alt.Y("current_speed:Q", title="Tốc độ hiện tại (km/h)"),
            tooltip=["timestamp", "current_speed", "free_speed", "congestion"]
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)

        st.subheader("📍 Bản đồ vị trí mới nhất")

        tooltip = {
            "html": "Trạng thái: {status}<br>Vĩ độ: {lat}<br>Kinh độ: {lon}",
            "style": {
                "backgroundColor": "steelblue",
                "color": "white"
            }
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
