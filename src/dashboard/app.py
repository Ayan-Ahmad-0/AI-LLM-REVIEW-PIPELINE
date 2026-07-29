import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="Review Sentiment Dashboard", layout="wide")
st.title("Review Sentiment Dashboard")

DB_CONN = os.environ.get("APP_DB_CONN")

if not DB_CONN:
    st.warning("APP_DB_CONN is not set. Check the environment variable in docker-compose.yml.")
else:
    try:
        engine = create_engine(DB_CONN)
        df = pd.read_sql(
            """
            SELECT
                rr.app_name,
                rr.review_text     AS review,
                rs.sentiment_score AS sentiment,
                rs.category,
                rs.model_used,
                rs.processed_at
            FROM review_sentiment rs
            JOIN raw_reviews rr ON rr.review_id = rs.review_id
            ORDER BY rs.processed_at DESC
            LIMIT 200;
            """,
            engine,
        )
        if df.empty:
            st.info("No sentiment results yet — run the pipeline first.")
        else:
            st.dataframe(df)
    except Exception as e:
        st.error(f"Could not read from the database: {e}")