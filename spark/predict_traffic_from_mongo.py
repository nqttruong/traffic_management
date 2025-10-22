# predict_traffic_from_mongo.py
from pymongo import MongoClient
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
from datetime import datetime, timedelta

def predict_traffic():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["traffic_db"]
    collection = db["traffic_data"]

    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)
    cursor = collection.find({"timestamp": {"$gte": one_hour_ago, "$lte": now}})
    data = list(cursor)

    if not data:
        return None

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")
    df["minutes"] = (df["timestamp"] - df["timestamp"].min()).dt.total_seconds() / 60

    X = df[["minutes"]]
    y = df["traffic_density"]

    model = LinearRegression()
    model.fit(X, y)

    future_time = df["minutes"].max() + 15
    future_pred = model.predict(np.array([[future_time]]))

    return {
        "current_density": float(y.iloc[-1]),
        "predicted_density": float(future_pred[0]),
        "last_update": df["timestamp"].iloc[-1]
    }
