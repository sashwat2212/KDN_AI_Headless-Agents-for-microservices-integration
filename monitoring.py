# import streamlit as st
# import pandas as pd
# import plotly.express as px
# from kafka import KafkaConsumer
# import redis
# import json
# import time


# KAFKA_BROKER = "localhost:9093"
# TOPICS = ["validated_tax_data", "fraud_alerts"]  

# REDIS_HOST = "localhost"
# REDIS_PORT = 6379
# REDIS_DB = 0


# consumer = KafkaConsumer(
#     *TOPICS,
#     bootstrap_servers=KAFKA_BROKER,
#     auto_offset_reset="earliest",
#     enable_auto_commit=True,
#     value_deserializer=lambda m: json.loads(m.decode("utf-8"))
# )


# redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


# st.set_page_config(page_title="Microservices Dashboard", layout="wide")
# st.title("📊 Microservices Integration Dashboard")
# st.markdown("This dashboard visualizes Kafka message flow, Redis deduplication, and microservices activity.")


# if "messages" not in st.session_state:
#     st.session_state.messages = []


# st.subheader("🔄 Kafka Message Stream")
# message_table = st.empty()


# st.subheader("📊 Message Distribution Across Topics")
# topic_chart_container = st.empty()


# st.subheader("⚡ Processing Speed by Service")
# speed_chart_container = st.empty()


# st.subheader("⏳ Messages Over Time")
# time_chart_container = st.empty()


# st.subheader("🛠 Redis Deduplication Status")
# dedup_table_container = st.empty()


# for message in consumer:
#     msg = {
#         "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(message.timestamp / 1000)),
#         "topic": message.topic,
#         #"key": message.key.decode("utf-8") if message.key else "N/A",
#         #"value": message.value
#     }
#     st.session_state.messages.append(msg)


#     if len(st.session_state.messages) > 20:
#         st.session_state.messages.pop(0)


#     df = pd.DataFrame(st.session_state.messages)


#     if not df.empty:
#         message_table.dataframe(df)


#         topic_counts = df["topic"].value_counts().reset_index()
#         topic_counts.columns = ["Topic", "Messages"]
#         topic_fig = px.pie(topic_counts, names="Topic", values="Messages", title="Message Distribution")
#         topic_chart_container.plotly_chart(topic_fig, key="topic_chart")  


#         service_counts = df["topic"].value_counts().reset_index()
#         service_counts.columns = ["Service", "Processed Messages"]
#         speed_fig = px.bar(service_counts, x="Service", y="Processed Messages", title="Processing Speed")
#         speed_chart_container.plotly_chart(speed_fig, key="speed_chart")  


#         df["timestamp"] = pd.to_datetime(df["timestamp"])
#         time_fig = px.line(df, x="timestamp", y="topic", title="Messages Over Time")
#         time_chart_container.plotly_chart(time_fig, key="time_chart")  


#     keys = redis_client.keys("*")
#     dedup_table = [{"Message ID": key, "Processed": redis_client.get(key)} for key in keys]

#     if dedup_table:
#         df_dedup = pd.DataFrame(dedup_table)
#         dedup_table_container.dataframe(df_dedup)
#     else:
#         dedup_table_container.write("No deduplicated messages found.")


#     time.sleep(2)
    
#     st.rerun()

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
from kafka import KafkaConsumer
import redis
import json
import time


KAFKA_BROKER = "localhost:9093"
TOPICS = ["validated_tax_data", "fraud_alerts"]
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0


consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)


redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


st.set_page_config(page_title="Microservices Dashboard", layout="wide")
st.title("📊 Microservices Integration Dashboard")
st.markdown("This dashboard visualizes Kafka message flow, Redis deduplication, and microservices activity.")


if "messages" not in st.session_state:
    st.session_state.messages = []


try:
    partitions = consumer.partitions_for_topic(TOPICS[0])
    st.success("✅ Kafka Consumer Connected!")
except Exception:
    st.error("❌ Kafka Consumer Disconnected!")


st.subheader("🔄 Kafka Message Stream")
topic_filter = st.selectbox("Filter by Topic", options=["All"] + TOPICS, index=0)
search_query = st.text_input("Search in Messages", "")
message_table = st.empty()

st.subheader("📊 Message Distribution Across Topics")
topic_chart_container = st.empty()

st.subheader("⚡ Processing Speed by Service")
speed_chart_container = st.empty()

st.subheader("⏳ Messages Over Time")
time_chart_container = st.empty()

st.subheader("🛠 Redis Deduplication Status")
dedup_table_container = st.empty()
st.metric("🔄 Deduplicated Messages", len(redis_client.keys("*")))
if st.button("🗑 Clear Redis Cache"):
    redis_client.flushdb()
    st.success("Redis cache cleared successfully!")

st.subheader("🔥 Fraud Alerts")
fraud_alert_container = st.empty()


for message in consumer:
    msg = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(message.timestamp / 1000)),
        "topic": message.topic,
    }
    st.session_state.messages.append(msg)

    if len(st.session_state.messages) > 20:
        st.session_state.messages.pop(0)


    df = pd.DataFrame(st.session_state.messages)

    if not df.empty:

        if topic_filter != "All":
            df = df[df["topic"] == topic_filter]
        if search_query:
            df = df[df.apply(lambda row: search_query.lower() in str(row).lower(), axis=1)]


        message_table.dataframe(df)


        topic_counts = df["topic"].value_counts().reset_index()
        topic_counts.columns = ["Topic", "Messages"]
        topic_fig = px.pie(topic_counts, names="Topic", values="Messages", title="Message Distribution")
        topic_chart_container.plotly_chart(topic_fig, key="topic_chart")


        service_counts = df["topic"].value_counts().reset_index()
        service_counts.columns = ["Service", "Processed Messages"]
        speed_fig = px.bar(service_counts, x="Service", y="Processed Messages", title="Processing Speed")
        speed_chart_container.plotly_chart(speed_fig, key="speed_chart")


        df["timestamp"] = pd.to_datetime(df["timestamp"])
        time_fig = px.line(df, x="timestamp", y="topic", title="Messages Over Time")
        time_chart_container.plotly_chart(time_fig, key="time_chart")


        fraud_alerts = df[df["topic"] == "fraud_alerts"]
        if not fraud_alerts.empty:
            fraud_alert_container.warning("⚠️ Fraud Alert Detected!")
            fraud_alert_container.dataframe(fraud_alerts)


        latest_message_time = df["timestamp"].max()
        consumer_lag = (pd.Timestamp.now() - latest_message_time).total_seconds()
        st.metric("⏳ Consumer Lag (seconds)", round(consumer_lag, 2))


        keys = redis_client.keys("*")
        dedup_table = [{"Message ID": key, "Processed": redis_client.get(key)} for key in keys]
        if dedup_table:
            df_dedup = pd.DataFrame(dedup_table)
            dedup_table_container.dataframe(df_dedup)
        else:
            dedup_table_container.write("No deduplicated messages found.")


    auto_refresh = st.toggle("🔄 Auto Refresh", value=True)
    if auto_refresh:
        time.sleep(2)
        st.rerun()
