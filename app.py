import streamlit as st
import pandas as pd
import time
import json
import certifi
from collections import Counter
from confluent_kafka import Consumer

# ============================================================
# 1. STREAMLIT CONFIG
# ============================================================

st.set_page_config(
    page_title="Wikipedia Streaming Analytics",
    page_icon="📡",
    layout="wide"
)

# ============================================================
# 2. KAFKA CONFIG
# ============================================================

BOOTSTRAP_SERVERS = st.secrets["KAFKA_BOOTSTRAP_SERVERS"]
SASL_USERNAME = st.secrets["KAFKA_USERNAME"]
OCI_AUTH_TOKEN = st.secrets["KAFKA_PASSWORD"]
TOPIC = st.secrets.get(
    "KAFKA_TOPIC",
    "DemoStreamingFashion"
)

# ============================================================
# 3. KAFKA CONSUMER
# ============================================================

@st.cache_resource
def create_consumer():

    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": SASL_USERNAME,
        "sasl.password": OCI_AUTH_TOKEN,
        "ssl.ca.location": certifi.where(),

        "client.id": "streamlit-wikipedia-dashboard",
        "group.id": "streamlit-dashboard-group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])

    return consumer


consumer = create_consumer()

# ============================================================
# 4. SESSION STATE
# ============================================================

if "events" not in st.session_state:
    st.session_state.events = []

if "total_events" not in st.session_state:
    st.session_state.total_events = 0

if "start_time" not in st.session_state:
    st.session_state.start_time = time.time()


# ============================================================
# 5. READ KAFKA STREAM
# ============================================================

def read_stream():

    new_events = []

    # Đọc nhiều message trong một lần refresh
    for _ in range(30):

        msg = consumer.poll(0.05)

        if msg is None:
            continue

        if msg.error():
            st.session_state.kafka_error = str(msg.error())
            continue

        try:

            event = json.loads(
                msg.value().decode("utf-8")
            )

            new_events.append(event)

        except Exception:
            continue

    return new_events


new_events = read_stream()

# ============================================================
# 6. UPDATE SESSION DATA
# ============================================================

if new_events:

    st.session_state.events.extend(new_events)

    st.session_state.total_events += len(new_events)

    # Chỉ giữ 1000 event gần nhất
    if len(st.session_state.events) > 1000:

        st.session_state.events = (
            st.session_state.events[-1000:]
        )


events = st.session_state.events


# ============================================================
# 7. ANALYTICS
# ============================================================

actor_counts = Counter()

for event in events:

    actor = event.get(
        "actor",
        "Không xác định"
    )

    actor_counts[actor] += 1


elapsed = time.time() - st.session_state.start_time

if elapsed > 0:

    event_rate = (
        st.session_state.total_events
        / elapsed
    )

else:

    event_rate = 0

# ============================================================
# KAFKA ERROR DISPLAY
# ============================================================

if "kafka_error" in st.session_state:
    st.error(
        f"Kafka Error: {st.session_state.kafka_error}"
    )
# ============================================================
# 8. HEADER
# ============================================================

st.title(
    "📡 REAL-TIME WIKIPEDIA STREAMING ANALYTICS"
)

st.caption(
    "Wikimedia Recent Changes → Kafka / OCI Streaming → Streamlit"
)

st.divider()


# ============================================================
# 9. STATUS
# ============================================================

col_status1, col_status2 = st.columns(2)

with col_status1:

    st.success(
        "🟢 Hệ thống Streaming đang hoạt động"
    )

with col_status2:

    st.info(
        f"Kafka Topic: {TOPIC}"
    )


# ============================================================
# 10. KPI
# ============================================================

col1, col2, col3, col4 = st.columns(4)

with col1:

    st.metric(
        "Tổng Events",
        f"{st.session_state.total_events:,}"
    )

with col2:

    st.metric(
        "Events đang hiển thị",
        f"{len(events):,}"
    )

with col3:

    st.metric(
        "Event Rate",
        f"{event_rate:.2f} evt/s"
    )

with col4:

    st.metric(
        "Actor Classes",
        len(actor_counts)
    )


st.divider()


# ============================================================
# 11. ACTOR DISTRIBUTION
# ============================================================

left, right = st.columns([1, 1])


with left:

    st.subheader(
        "📊 Phân bố Actor"
    )

    if actor_counts:

        actor_df = pd.DataFrame(
            {
                "Actor": list(actor_counts.keys()),
                "Events": list(actor_counts.values())
            }
        )

        actor_df = actor_df.sort_values(
            "Events",
            ascending=False
        )

        st.bar_chart(
            actor_df.set_index("Actor")
        )

    else:

        st.info(
            "Đang chờ dữ liệu streaming..."
        )


# ============================================================
# 12. ACTOR PERCENTAGE
# ============================================================

with right:

    st.subheader(
        "📈 Tỷ trọng Actor"
    )

    if actor_counts:

        total = sum(actor_counts.values())

        percentage_df = pd.DataFrame(
            {
                "Actor": list(actor_counts.keys()),
                "Percentage": [
                    value / total * 100
                    for value in actor_counts.values()
                ]
            }
        )

        percentage_df = percentage_df.sort_values(
            "Percentage",
            ascending=False
        )

        st.dataframe(
            percentage_df.style.format(
                {
                    "Percentage": "{:.2f}%"
                }
            ),
            use_container_width=True,
            hide_index=True
        )

    else:

        st.info(
            "Chưa có dữ liệu."
        )


st.divider()


# ============================================================
# 13. LIVE TRANSACTION LOG
# ============================================================

st.subheader(
    "🔴 Live Transaction Logs"
)

if events:

    recent_events = events[-10:]

    display_rows = []

    for event in reversed(recent_events):

        display_rows.append(
            {
                "Payload":
                    event.get(
                        "title",
                        "N/A"
                    ),

                "Actor":
                    event.get(
                        "actor",
                        "N/A"
                    ),

                "Run ID":
                    event.get(
                        "run_id",
                        "N/A"
                    )
            }
        )

    log_df = pd.DataFrame(
        display_rows
    )

    st.dataframe(
        log_df,
        use_container_width=True,
        hide_index=True
    )

else:

    st.info(
        "Đang chờ dữ liệu từ Kafka..."
    )


# ============================================================
# 14. RAW DATA
# ============================================================

with st.expander(
    "🔎 Xem dữ liệu streaming"
):

    if events:

        raw_df = pd.DataFrame(events)

        st.dataframe(
            raw_df.tail(50),
            use_container_width=True,
            hide_index=True
        )

    else:

        st.write(
            "Chưa có dữ liệu."
        )


# ============================================================
# 15. AUTO REFRESH
# ============================================================

time.sleep(1)

st.rerun()
