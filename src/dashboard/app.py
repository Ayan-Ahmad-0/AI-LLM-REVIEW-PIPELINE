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
        df = pd.read_sql("SELECT * FROM review_sentiment ORDER BY processed_at DESC LIMIT 100;", engine)
        if df.empty:
            st.info("No sentiment results yet — run the pipeline first.")
        else:
            st.dataframe(df)
    except Exception as e:
        st.error(f"Could not read from the database: {e}")