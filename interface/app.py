import json
import os
import uuid

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import streamlit as st
from kafka import KafkaProducer


KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions"),
}


def load_file(uploaded_file_):
    try:
        return pd.read_csv(uploaded_file_, dtype={"post_code": str})
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {e!s}")
        return None


def send_to_kafka(df, topic, bootstrap_servers):
    try:
        if df.empty:
            st.warning("CSV не содержит транзакций")
            return False

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, allow_nan=False).encode("utf-8"),
            security_protocol="PLAINTEXT",
        )

        try:
            progress_bar = st.progress(0)
            total_rows = len(df)

            for idx, row in df.iterrows():
                transaction_id = str(uuid.uuid4())

                producer.send(
                    topic,
                    value={
                        "transaction_id": transaction_id,
                        "data": {
                            key: None if pd.isna(value) else value
                            for key, value in row.to_dict().items()
                        },
                    },
                )

                progress_bar.progress((idx + 1) / total_rows)

            producer.flush()
        finally:
            producer.close()

        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {e!s}")
        return False


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "fraud_db"),
        user=os.getenv("POSTGRES_USER", "fraud_user"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud_password"),
    )


def load_fraud_transactions(limit=10):
    conn = get_db_connection()

    try:
        query = """
            SELECT transaction_id, score, fraud_flag, model_version, created_at
            FROM scores
            WHERE fraud_flag = 1
            ORDER BY created_at DESC, id DESC
            LIMIT %s
        """

        return pd.read_sql(query, conn, params=(limit,))
    finally:
        conn.close()


def load_scores(limit=100):
    conn = get_db_connection()

    try:
        query = """
            SELECT score
            FROM scores
            ORDER BY created_at DESC, id DESC
            LIMIT %s
        """

        return pd.read_sql(query, conn, params=(limit,))
    finally:
        conn.close()


if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}


st.title("📤 Fraud Detection Service")


uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"],
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    df = load_file(uploaded_file)

    if df is not None:
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": df,
        }
        st.success(f"Файл {uploaded_file.name} успешно загружен!")

if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")

    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])

        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")

        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"],
                        )

                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")


if st.button("Посмотреть результаты"):
    st.subheader("Последние 10 фродовых транзакций:")

    try:
        fraud_df = load_fraud_transactions(limit=10)

        if not fraud_df.empty:
            st.dataframe(
                fraud_df[["transaction_id", "score", "fraud_flag", "model_version", "created_at"]]
            )
        else:
            st.write("Нет записей с fraud_flag == 1")

        st.subheader("Гистограмма скоров последних транзакций:")

        score_df = load_scores(limit=100)

        if not score_df.empty:
            fig, ax = plt.subplots()
            ax.hist(score_df["score"], bins=40, range=(0, 1))
            ax.set_title("Распределение скоров")
            ax.set_xlabel("Score")
            ax.set_ylabel("Частота")
            ax.grid(ls=":")
            st.pyplot(fig)
            plt.close(fig)
        else:
            st.write("Нет записей в базе для построения гистограммы")

    except Exception as e:
        st.error(f"Ошибка получения результатов: {e!s}")
