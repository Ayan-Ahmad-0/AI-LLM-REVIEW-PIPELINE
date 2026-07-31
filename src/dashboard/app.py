"""
Review Sentiment Dashboard
Reads live from Postgres (raw_reviews JOIN review_sentiment, per sql/init.sql)
via APP_DB_CONN and renders an interactive Streamlit + Plotly dashboard: KPI
cards, sentiment donut, review-volume trend, rating distribution, top
categories, and a raw-data explorer with pandas styling.

No dependency on Airflow — this container only ever reads.
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# ==============================================================================
# PAGE CONFIG
# ==============================================================================
st.set_page_config(
    page_title="Review Sentiment Dashboard",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ==============================================================================
# PALETTE — warm red and orange accents
# ==============================================================================
POSITIVE = "#FFB347"
NEUTRAL = "#780AA0"  # Adjusted to a warmer yellow
NEGATIVE = "#DC0404"
RED = "#FF4B4B"
ORANGE = "#FF8C00"
AMBER = "#FFB347"
SEQ = [RED, ORANGE, POSITIVE, AMBER, NEGATIVE, "#FF6B6B", "#FFA07A"]
SENTIMENT_COLOR_MAP = {"Positive": POSITIVE, "Neutral": NEUTRAL, "Negative": NEGATIVE}

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, -apple-system, sans-serif", size=13),
    margin=dict(t=30, b=10, l=10, r=10),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
)

# ==============================================================================
# CUSTOM CSS — hide chrome, add card styling, warm theme sidebar, custom expander
# ==============================================================================
st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
        html, body, [class*="css"] { font-family: 'Inter', -apple-system, sans-serif; }

        /* Hide default Streamlit chrome for a cleaner, product-like look */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}

        .block-container { padding-top: 1.4rem; padding-bottom: 3rem; max-width: 1400px; }

        /* Hero banner - Red to Orange gradient */
        .hero {
            background: linear-gradient(120deg, #FF4B4B 0%, #FF8C00 100%);
            border-radius: 18px;
            padding: 26px 30px;
            margin-bottom: 22px;
            box-shadow: 0 10px 30px -14px rgba(255, 75, 75, 0.55);
        }
        .hero h1 { color: white; font-size: 1.9rem; font-weight: 800; margin: 0; letter-spacing: -0.4px; }
        .hero p { color: rgba(255,255,255,0.9); margin: 6px 0 0 0; font-size: 0.95rem; }

        /* KPI cards - Warmer tints */
        .kpi-card {
            background: rgba(255, 75, 75, 0.05);
            border: 1px solid rgba(255, 140, 0, 0.2);
            border-left: 4px solid var(--accent, #FF4B4B);
            border-radius: 16px;
            padding: 18px 20px;
            box-shadow: 0 4px 18px -12px rgba(0,0,0,0.35);
            transition: transform 0.15s ease, box-shadow 0.15s ease;
            height: 120px;
            overflow: hidden;
        }
        .kpi-card:hover { transform: translateY(-3px); box-shadow: 0 10px 24px -12px rgba(0,0,0,0.45); }
        .kpi-label { font-size: 0.76rem; font-weight: 600; opacity: 0.65; text-transform: uppercase; letter-spacing: 0.05em; }
        .kpi-value { font-size: 1.85rem; font-weight: 800; margin-top: 4px; }
        
        /* Class for long model names */
        .kpi-value.small-text { 
            font-size: 1.1rem !important; 
            margin-top: 10px;
            white-space: nowrap;
            text-overflow: ellipsis;
            overflow: hidden;
        }
        
        .kpi-sub { font-size: 0.78rem; opacity: 0.55; margin-top: 3px; }

        /* Section card wrapping each chart */
        .section-card {
            background: rgba(255, 140, 0, 0.03);
            border: 1px solid rgba(255, 140, 0, 0.15);
            border-radius: 16px;
            padding: 18px 20px 8px 20px;
            margin-bottom: 18px;
        }
        .section-title { font-weight: 700; font-size: 1.02rem; margin-bottom: 2px; }
        .section-sub { font-size: 0.8rem; opacity: 0.6; margin-bottom: 6px; }

        /* --------------------------------------------------- */
        /* EXPANDER (RAW DATA PANEL) STYLING */
        /* --------------------------------------------------- */
        div[data-testid="stExpander"] {
            border-radius: 16px !important;
            border: 1px solid rgba(255, 140, 0, 0.3) !important;
            background-color: rgba(255, 75, 75, 0.03) !important;
            box-shadow: 0 6px 16px -8px rgba(255, 75, 75, 0.2) !important;
            overflow: hidden;
            margin-top: 15px;
        }

        /* Make the clickable header chunkier and themed */
        div[data-testid="stExpander"] details summary {
            padding: 16px 20px !important; 
            background: linear-gradient(120deg, rgba(255, 75, 75, 0.12) 0%, rgba(255, 140, 0, 0.12) 100%) !important;
            transition: all 0.2s ease;
        }
        
        div[data-testid="stExpander"] details summary:hover {
            background: linear-gradient(120deg, rgba(255, 75, 75, 0.2) 0%, rgba(255, 140, 0, 0.2) 100%) !important;
        }

        /* Expander text styling */
        div[data-testid="stExpander"] details summary p {
            font-size: 1.08rem !important;
            font-weight: 700 !important;
            color: #FF8C00 !important; /* Bold orange text */
        }
        
        div[data-testid="stExpander"] .streamlit-expanderContent {
            padding: 20px !important;
        }

        /* --------------------------------------------------- */
        /* SIDEBAR & WIDGET STYLING */
        /* --------------------------------------------------- */
        
        /* Darker Sidebar Background */
        [data-testid="stSidebar"] {
            background-color: #120e0d !important; /* Very dark brownish/reddish black */
            border-right: 1px solid rgba(255, 140, 0, 0.15);
        }
        
        /* Multiselect Dropdown Box */
        .stMultiSelect [data-baseweb="select"] {
            border-radius: 10px;
            border: 1px solid rgba(255, 140, 0, 0.25);
            background-color: rgba(255, 75, 75, 0.05);
        }
        
        /* Multiselect Tags (Pills) - Warm gradient */
        .stMultiSelect [data-baseweb="tag"] {
            background: linear-gradient(120deg, #FF4B4B, #FF8C00);
            color: white;
            border: none;
            border-radius: 6px;
            font-weight: 600;
        }
        .stMultiSelect [data-baseweb="tag"] span {
            color: white !important; 
        }

        /* Refresh Button Styling */
        .stButton > button {
            background: rgba(255, 140, 0, 0.08);
            border: 1px solid rgba(255, 140, 0, 0.3);
            border-radius: 10px;
            color: inherit;
            transition: all 0.2s ease;
            font-weight: 600;
        }
        /* Refresh Button Hover State */
        .stButton > button:hover {
            background: linear-gradient(120deg, #FF4B4B 0%, #FF8C00 100%);
            border-color: transparent;
            color: white;
            box-shadow: 0 6px 16px -4px rgba(255, 75, 75, 0.4);
            transform: translateY(-2px);
        }
        /* --------------------------------------------------- */
    </style>
    """,
    unsafe_allow_html=True,
)

# ==============================================================================
# DATA LOADING — live from Postgres (raw_reviews JOIN review_sentiment)
# ==============================================================================
DB_CONN = os.environ.get("APP_DB_CONN")

QUERY = """
    SELECT
        r.review_id,
        r.app_name,
        r.rating,
        r.review_text   AS review,
        s.summary,
        s.sentiment_score AS sentiment,
        s.category,
        r.source_posted_at AS review_date,
        r.app_version,
        r.likes_count,
        s.model_used,
        s.processed_at
    FROM review_sentiment s
    JOIN raw_reviews r ON r.review_id = s.review_id
    ORDER BY r.source_posted_at DESC;
"""

@st.cache_data(ttl=300, show_spinner="Loading reviews from Postgres...")
def load_data(conn_str: str) -> pd.DataFrame:
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        df = pd.read_sql(text(QUERY), conn)

    if df.empty:
        return df

    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["sentiment"] = pd.to_numeric(df["sentiment"], errors="coerce")
    df["app_version"] = df["app_version"].fillna("Unknown")
    df["likes_count"] = pd.to_numeric(df["likes_count"], errors="coerce").fillna(0).astype(int)
    df = df.dropna(subset=["review_date"])

    if not df.empty:
        smin, smax = df["sentiment"].min(), df["sentiment"].max()
        mid = (smin + smax) / 2
        span = max(smax - smin, 1e-9)

        def label(score):
            if pd.isna(score):
                return "Unknown"
            if score >= mid + 0.05 * span:
                return "Positive"
            if score <= mid - 0.05 * span:
                return "Negative"
            return "Neutral"

        df["sentiment_label"] = df["sentiment"].apply(label)

    return df

if not DB_CONN:
    st.warning("`APP_DB_CONN` is not set. Check the environment variable in docker-compose.yml.")
    st.stop()

try:
    df_all = load_data(DB_CONN)
except Exception as e:
    st.error(f"Could not read from the database: {e}")
    st.stop()

if df_all.empty:
    st.info("No sentiment results yet — run the pipeline first.")
    st.stop()

# ==============================================================================
# SIDEBAR — global filters
# ==============================================================================
with st.sidebar:
    st.markdown("### 🎛️ Filters")

    apps = sorted(df_all["app_name"].dropna().unique().tolist())
    selected_apps = st.multiselect("App", apps, default=apps)

    labels = sorted(df_all["sentiment_label"].dropna().unique().tolist())
    selected_labels = st.multiselect("Sentiment", labels, default=labels)

    min_date = df_all["review_date"].min().date()
    max_date = df_all["review_date"].max().date()
    if min_date == max_date:
        st.caption(f"All reviews are from {min_date}")
        start_date, end_date = min_date, max_date
    else:
        start_date, end_date = st.slider(
            "Review date range",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="YYYY-MM-DD",
        )

    st.markdown("---")
    st.caption(f"{len(df_all):,} total reviews · cached 5 min")
    if st.button("🔄 Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

df = df_all[
    df_all["app_name"].isin(selected_apps)
    & df_all["sentiment_label"].isin(selected_labels)
    & (df_all["review_date"].dt.date >= start_date)
    & (df_all["review_date"].dt.date <= end_date)
]

# ==============================================================================
# HERO HEADER
# ==============================================================================
st.markdown(
    f"""
    <div class="hero">
        <h1>📱 Review Sentiment Dashboard</h1>
        <p>{len(df):,} reviews across {df['app_name'].nunique() if not df.empty else 0} apps ·
            LLM-scored sentiment, category, and summaries</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if df.empty:
    st.warning("No reviews match the current filters. Try widening your selection in the sidebar.")
    st.stop()

# ==============================================================================
# KPI CARDS
# ==============================================================================
total_reviews = len(df)
avg_rating = df["rating"].mean()
avg_sentiment = df["sentiment"].mean()
top_model = df["model_used"].mode().iloc[0] if not df["model_used"].dropna().empty else "n/a"

sentiment_min = float(df["sentiment"].min())
sentiment_max = float(df["sentiment"].max())
sentiment_mid = (sentiment_min + sentiment_max) / 2
sentiment_span = max(sentiment_max - sentiment_min, 1e-9)

sentiment_color = (
    POSITIVE if avg_sentiment >= sentiment_mid + 0.1 * sentiment_span
    else NEGATIVE if avg_sentiment <= sentiment_mid - 0.1 * sentiment_span
    else NEUTRAL
)

kpi_specs = [
    ("Total Reviews", f"{total_reviews:,}", f"{df['app_name'].nunique()} apps", RED),
    ("Average Rating", f"{avg_rating:.1f} ⭐", "out of 5", AMBER),
    ("Avg Sentiment Score", f"{avg_sentiment:.2f}", f"scale: {sentiment_min:.1f} to {sentiment_max:.1f}", sentiment_color),
    ("Primary Model", top_model, "most-used pipeline model", ORANGE),
]

cols = st.columns(4)
for col, (label, value, sub, color) in zip(cols, kpi_specs):
    with col:
        val_class = "kpi-value small-text" if label == "Primary Model" else "kpi-value"
        
        st.markdown(
            f"""
            <div class="kpi-card" style="--accent:{color}">
                <div class="kpi-label">{label}</div>
                <div class="{val_class}" title="{value}">{value}</div>
                <div class="kpi-sub">{sub}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.write("")

# ==============================================================================
# ROW 1 — sentiment donut + review volume trend
# ==============================================================================
row1_left, row1_right = st.columns(2)

with row1_left:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Sentiment Distribution</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Share of Positive / Neutral / Negative reviews</div>', unsafe_allow_html=True)

    label_counts = df["sentiment_label"].value_counts().reset_index()
    label_counts.columns = ["sentiment_label", "count"]

    fig_donut = px.pie(
        label_counts,
        names="sentiment_label",
        values="count",
        hole=0.6,
        color="sentiment_label",
        color_discrete_map=SENTIMENT_COLOR_MAP,
    )
    fig_donut.update_traces(textinfo="percent+label", textfont_size=12)
    fig_donut.update_layout(
        **CHART_LAYOUT,
        height=360,
        showlegend=False,
        annotations=[dict(text=f"{total_reviews:,}<br>reviews", x=0.5, y=0.5, font_size=15, showarrow=False)],
    )
    st.plotly_chart(fig_donut, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with row1_right:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Review Volume Over Time</div>', unsafe_allow_html=True)

    span = df["review_date"].max() - df["review_date"].min()
    span_hours = span.total_seconds() / 3600

    if span_hours <= 48:
        freq, freq_label = "h", "hour"
    elif span_hours <= 60 * 24:
        freq, freq_label = "D", "day"
    else:
        freq, freq_label = "W", "week"

    st.markdown(f'<div class="section-sub">Reviews per {freq_label}, by app</div>', unsafe_allow_html=True)

    volume = (
        df.set_index("review_date")
        .groupby([pd.Grouper(freq=freq), "app_name"])
        .size()
        .unstack("app_name", fill_value=0)
        .stack()
        .reset_index(name="review_count")
    )

    if volume["review_date"].nunique() <= 1:
        st.info("Not enough of a time spread yet to draw a trend line — all filtered reviews land in one bucket.")
    else:
        APP_COLORS = {
            "Instagram": "#DC0404", # Red
            "Spotify": "#098A1C",   # Amber
            "Duolingo": "#FF8C00"   # Orange
        }
        fig_trend = px.line(
            volume,
            x="review_date",
            y="review_count",
            color="app_name",
            markers=True,
            color_discrete_map=APP_COLORS,
            labels={"review_date": "", "review_count": "Reviews", "app_name": ""},
        )
        fig_trend.update_traces(mode="lines+markers", line_width=2.5, marker=dict(size=6), connectgaps=True)
        fig_trend.update_layout(**CHART_LAYOUT, height=360)
        fig_trend.update_xaxes(showgrid=False)
        fig_trend.update_yaxes(gridcolor="rgba(255,140,0,0.1)")
        st.plotly_chart(fig_trend, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

# ==============================================================================
# ROW 2 — rating distribution + top categories
# ==============================================================================
row2_left, row2_right = st.columns(2)

with row2_left:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Rating Distribution</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Count of reviews per star rating</div>', unsafe_allow_html=True)

    rating_counts = (
        df.dropna(subset=["rating"])
        .assign(rating=lambda d: d["rating"].astype(int))
        .groupby("rating")
        .size()
        .reindex(range(1, 6), fill_value=0)
        .reset_index(name="count")
    )
    fig_rating = px.bar(
        rating_counts,
        x="rating",
        y="count",
        color="count",
        color_continuous_scale=[RED, ORANGE, AMBER],
        labels={"rating": "Star rating", "count": "Reviews"},
    )
    fig_rating.update_traces(marker_line_width=0)
    fig_rating.update_layout(**CHART_LAYOUT, height=340, coloraxis_showscale=False)
    fig_rating.update_xaxes(dtick=1, showgrid=False)
    fig_rating.update_yaxes(gridcolor="rgba(255,140,0,0.1)")
    st.plotly_chart(fig_rating, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with row2_right:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Top 5 Feedback Categories</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Most common LLM-assigned categories</div>', unsafe_allow_html=True)

    top_categories = (
        df["category"].value_counts().head(5).sort_values(ascending=True).reset_index()
    )
    top_categories.columns = ["category", "count"]

    fig_categories = px.bar(
        top_categories,
        x="count",
        y="category",
        orientation="h",
        color="count",
        color_continuous_scale=[ORANGE, RED],
        labels={"count": "Reviews", "category": ""},
    )
    fig_categories.update_traces(marker_line_width=0)
    fig_categories.update_layout(**CHART_LAYOUT, height=340, coloraxis_showscale=False)
    fig_categories.update_xaxes(showgrid=False)
    st.plotly_chart(fig_categories, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

# ==============================================================================
# RAW DATA EXPLORER
# ==============================================================================
with st.expander("📄 View Raw Pipeline Data", expanded=False):
    st.caption(f"{len(df):,} rows after filters — sentiment column highlighted by intensity.")

    display_df = df.copy()
    display_df["review_date"] = display_df["review_date"].dt.strftime("%Y-%m-%d %H:%M")
    display_df["processed_at"] = display_df["processed_at"].dt.strftime("%Y-%m-%d %H:%M")

    ordered_cols = [
        "app_name", "rating", "sentiment_label", "sentiment", "category", "review",
        "summary", "review_date", "app_version", "likes_count", "model_used", "processed_at",
    ]
    display_df = display_df[[c for c in ordered_cols if c in display_df.columns]]

    styled = (
        display_df.style
        .background_gradient(subset=["sentiment"], cmap="RdYlGn", vmin=sentiment_min, vmax=sentiment_max)
        .format({"sentiment": "{:.2f}", "rating": "{:.0f}"})
    )
    st.dataframe(styled, use_container_width=True, height=420)