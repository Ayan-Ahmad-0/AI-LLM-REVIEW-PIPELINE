# 📊 AI/LLM Review Sentiment Pipeline

An end-to-end, containerized data engineering pipeline that scrapes real Google Play Store reviews, streams them through Kafka, runs them through a Gemini LLM for sentiment/category/summary extraction, validates the output with Pydantic, lands it in Postgres, and visualizes it in a live Streamlit dashboard — all orchestrated by Airflow.

Built as a portfolio project to demonstrate practical, production-style data engineering: streaming ingestion, idempotent batch processing, LLM-in-the-pipeline patterns, schema validation/dead-lettering, and container orchestration — not just a notebook demo.

<p align="left">
  <img src="https://img.shields.io/badge/python-3.9-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.9"/>
  <img src="https://img.shields.io/badge/docker-compose-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker Compose"/>
  <img src="https://img.shields.io/badge/apache%20airflow-2.5.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Apache Airflow"/>
  <img src="https://img.shields.io/badge/apache%20kafka-streaming-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Apache Kafka"/>
  <img src="https://img.shields.io/badge/postgresql-database-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL"/>
  <img src="https://img.shields.io/badge/fastapi-service-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI"/>
  <img src="https://img.shields.io/badge/streamlit-dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit"/>
  <img src="https://img.shields.io/badge/gemini-LLM-8E75B2?style=for-the-badge&logo=googlegemini&logoColor=white" alt="Google Gemini"/>
  <img src="https://img.shields.io/badge/pydantic-validated-E92063?style=for-the-badge&logo=pydantic&logoColor=white" alt="Pydantic"/>
  <img src="https://img.shields.io/badge/status-active%20development-yellow?style=for-the-badge" alt="Status"/>
  <img src="https://img.shields.io/badge/license-MIT-green?style=for-the-badge" alt="License MIT"/>
</p>

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Pipeline Stages](#pipeline-stages)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Dashboard](#dashboard)
- [Design Decisions & Lessons Learned](#design-decisions--lessons-learned)
- [Roadmap](#roadmap)
- [License](#license)

---

## Overview

This pipeline continuously ingests reviews for three mobile apps — **Spotify**, **Instagram**, and **Duolingo** — from the Google Play Store, and turns raw star-rated text reviews into structured sentiment intelligence:

- 🔍 **Sentiment score** (-1.0 to +1.0)
- 🏷️ **Category** (e.g. `ad_complaint`, `feature_praise`, `general_praise`)
- 📝 **One-line LLM-generated summary**

No source API keys or paid signups required — reviews are pulled via a lightweight scraper wrapped behind a FastAPI service, so the rest of the pipeline treats it like any other REST data source.

## Architecture

```mermaid
flowchart TB
    subgraph SRC["Source"]
        PS["Google Play Store<br/>(Spotify · Instagram · Duolingo)"]
    end

    subgraph ING["Ingestion"]
        API["FastAPI wrapper<br/>(google-play-scraper)"]
        PROD["Kafka Producer<br/>dedup vs raw_reviews"]
    end

    subgraph STREAM["Streaming"]
        TOPIC[("Kafka topic<br/>reviews.raw")]
    end

    subgraph PROC["Processing"]
        CONS["Consumer / Batcher<br/>token-aware batching"]
        RAW[("Postgres<br/>raw_reviews")]
    end

    subgraph AI["LLM Layer"]
        LLM["Gemini LLM<br/>structured output"]
        VAL["Pydantic validation<br/>(strict schema)"]
    end

    subgraph STORE["Storage"]
        SENT[("review_sentiment")]
        FAIL[("failed_batches")]
        USAGE[("llm_usage")]
    end

    subgraph ORCH["Orchestration"]
        AF["Airflow DAG<br/>(Celery executor)"]
    end

    subgraph VIZ["Presentation"]
        ST["Streamlit Dashboard"]
    end

    PS --> API --> PROD --> TOPIC --> CONS
    CONS -->|"insert (idempotent)"| RAW
    CONS --> LLM --> VAL
    VAL -->|"valid"| SENT
    VAL -->|"invalid / missing"| FAIL
    LLM -->|"token usage"| USAGE

    AF -.orchestrates.-> PROD
    AF -.orchestrates.-> CONS
    AF -.orchestrates.-> LLM

    SENT --> ST
    USAGE --> ST
    FAIL --> ST
```

**Flow in words:** the producer polls the Play Store wrapper, skips reviews it's already seen, and publishes new ones to Kafka → the consumer inserts each review into `raw_reviews` (idempotent, `ON CONFLICT DO NOTHING`) and groups them into token-bounded batches → each batch goes to Gemini for structured sentiment extraction → results are validated with Pydantic and split into `review_sentiment` (success) or `failed_batches` (schema failures / missing reviews) → token usage is logged to `llm_usage` → Streamlit reads directly from Postgres to render the live dashboard → Airflow schedules and retries the producer, consumer, and LLM steps as a DAG.

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Orchestration | Apache Airflow 2.5.1 (Celery executor) | Scheduled, retryable, dynamically-mapped batch processing |
| Streaming | Apache Kafka | Decouples ingestion from processing; replayable, durable |
| Ingestion API | FastAPI + `google-play-scraper` | No signup/API key required; normal REST semantics |
| Database | PostgreSQL | Landing zone, sentiment store, dead-letter table, usage ledger |
| LLM | Google Gemini (`google-generativeai`, free tier) | Structured output support; no billing required |
| Validation | Pydantic v2 | Schema enforcement independent of what the LLM actually returns |
| Retry logic | Tenacity | Exponential backoff on transient LLM/API errors |
| Dashboard | Streamlit | Fast, Python-native visualization directly on Postgres |
| Containerization | Docker Compose | Reproducible multi-service local environment |

## Project Structure

```
ai-llm-review-pipeline/
├── docker-compose.yml
├── .env.example
├── sql/
│   └── init.sql                  # raw_reviews, review_sentiment, failed_batches, llm_usage
├── dags/
│   └── review_sentiment_pipeline.py   # Airflow DAG: producer → consumer/batcher → LLM
├── src/
│   ├── playstore_api/            # FastAPI wrapper around google-play-scraper
│   ├── extract/
│   │   └── producer.py           # Polls Play Store, dedups, publishes to Kafka
│   ├── batching/
│   │   └── build_batches.py      # Consumes reviews.raw, inserts + batches by token count
│   ├── llm/
│   │   ├── schema.py             # Strict + Gemini-safe Pydantic models
│   │   ├── prompts.py            # System prompt + batch prompt builder
│   │   ├── client.py             # Gemini call with structured output + retries
│   │   └── test_analyze.py       # Standalone LLM smoke test (no Kafka needed)
│   ├── pipeline/
│   │   └── process_batches.py    # Wires batches → LLM → validate → Postgres
│   └── dashboard/
│       └── app.py                # Streamlit dashboard
└── README.md
```

## Data Model

| Table | Purpose |
|---|---|
| `raw_reviews` | Immutable landing table — every ingested review, deduplicated by `review_id` |
| `review_sentiment` | One row per successfully analyzed review: score, category, summary, model, batch |
| `failed_batches` | Dead-letter table — schema validation failures, LLM errors, or reviews the LLM silently dropped |
| `llm_usage` | One row per processed batch — input/output token counts, model used, review count |

## Pipeline Stages

| # | Stage | Status |
|---|---|---|
| 1 | Environment setup (Docker Compose: Postgres ×2, Kafka, Airflow, Streamlit) | ✅ |
| 2 | Postgres schema (`raw_reviews`, `review_sentiment`, `failed_batches`, `llm_usage`) | ✅ |
| 3 | Producer — Play Store scraper → Kafka (`reviews.raw`), with dedup | ✅ |
| 4 | Consumer/batching — idempotent insert + token-aware batching | ✅ |
| 5 | LLM integration — Gemini structured output, validated with Pydantic | ✅ |
| 6 | Wire consumer → LLM → validate → Postgres (`process_batches.py`) | ✅ |
| 7 | Airflow DAG — scheduled, dynamically-mapped, retryable | ✅ |
| 8 | Streamlit dashboard — sentiment trends, category breakdown, worst performers | ✅ |
| 9 | README + polish | ✅ (you're reading it) |

## Getting Started

### Prerequisites

- Docker Desktop (with Docker Compose v2)
- A free [Google AI Studio](https://aistudio.google.com/) Gemini API key (no credit card required)

### Setup

```bash
git clone <your-repo-url>
cd ai-llm-review-pipeline

# copy env template and fill in your Gemini key
cp .env.example .env
```

Edit `.env`:

```env
AIRFLOW_UID=50000
GEMINI_API_KEY=your-real-key-here
APP_DB_CONN=postgresql+psycopg2://app_user:app_password@app-postgres:5432/reviews
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_RAW_REVIEWS=reviews.raw
```

> ⚠️ Never commit `.env` (or a filled-in `.env.example`) — both are already git-ignored in this repo.

### Bring the stack up

```bash
docker compose up -d --build
```

This starts: `app-postgres`, Airflow's metadata Postgres, `kafka` (+ Kafka UI), the `airflow-webserver`/`scheduler`/`worker` trio, the `playstore-api` FastAPI service, and `streamlit`.

## Running the Pipeline

**Trigger a full run manually** (producer → consumer/batcher → LLM → Postgres), bypassing the container's Airflow CLI entrypoint:

```bash
docker compose run --rm --entrypoint bash airflow-worker -c \
  "cd /opt/airflow && python -m src.extract.producer"

docker compose run --rm --entrypoint bash airflow-worker -c \
  "cd /opt/airflow && python -m src.pipeline.process_batches"
```

**Or let Airflow run it on schedule** — open the webserver UI and unpause the DAG:

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Kafka UI | http://localhost:8090 |
| Streamlit Dashboard | http://localhost:8501 |

> ⚠️ Ports above match this project's compose file as configured during development — double-check `docker-compose.yml` if you've changed any host port mappings.

## Dashboard

The Streamlit dashboard reads directly from Postgres and shows:

- Sentiment trend over time, per app
- Category breakdown (ad complaints, feature praise, bug reports, etc.)
- Worst-scoring reviews/products, for triage
- Token usage / batch throughput from `llm_usage`

## Design Decisions & Lessons Learned

A few choices worth calling out (the project hit real, non-trivial issues getting here — this isn't a from-scratch idealized build):

- **Play Store over Best Buy/Reddit** — Best Buy's API blocks free/`.edu` email signups; Reddit would've needed an invented rating + product-id concept. Play Store reviews map onto the schema naturally with zero signup friction.
- **`google-generativeai` over `google-genai`** — the newer SDK requires Python 3.10+, but the Airflow base image is pinned to 3.9. Used the legacy (deprecated but functional) SDK instead.
- **Two Pydantic schema variants** — Gemini's legacy SDK converts schemas to protobuf, which doesn't support JSON Schema `minimum`/`maximum`. A constraint-free schema is used only for `response_schema`; the strict schema still validates the real response afterward.
- **Manual Kafka offset commits, only after the Postgres insert succeeds** — a crash mid-batch just reprocesses safely (the insert is idempotent) instead of silently dropping messages.
- **Token-based batching, not just review-count batching** — long reviews can't silently blow past the LLM's context window.
- **Batch-level atomicity in `process_batches.py`** — each batch's sentiment rows + usage row commit together, or the whole batch is logged to `failed_batches`; one bad batch never blocks the rest of the run.
- **Partial-response detection** — if Gemini returns fewer results than reviews sent, the missing ones are explicitly logged rather than silently vanishing.

## Roadmap

- [ ] Alerting on repeated `failed_batches` growth (Slack/email via Airflow)
- [ ] Cost tracking in `llm_usage` (estimated USD per batch)
- [ ] Swap the free-tier scraper source for a second, contrasting data source
- [ ] CI pipeline to run `test_analyze.py` on every push

## License

MIT — see [LICENSE](LICENSE) for details.

---

*Built by Ayan as a self-directed portfolio project demonstrating streaming ingestion, LLM-in-the-pipeline design, and container orchestration.*