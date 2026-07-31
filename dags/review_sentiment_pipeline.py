"""
Airflow DAG: orchestrates the review sentiment pipeline end-to-end.

extract (producer: poll Play Store -> Kafka)
    -> build_batches (consume Kafka -> raw_reviews -> token-aware batches)
    -> call_llm (dynamic task mapping -- ONE task instance per batch)

Dynamic mapping on call_llm means each batch's LLM call, validation, and
Postgres write is its own task instance in the Airflow UI: if one batch's
LLM response fails validation, only that instance retries/fails -- it never
blocks or fails the other batches from the same run. Validated results land
in review_sentiment, validation failures land in failed_batches, and token
usage is logged to llm_usage -- all inside process_batch() (see
src/batching/build_batches.py).

Runs hourly, no catchup (we only care about the current state going forward,
not backfilling every hour since epoch).

RATE LIMITING: call_llm batches run against the `gemini_api_pool` Airflow
pool instead of max_active_tis_per_dag=1. That old approach serialized
EVERY batch globally -- including ones that fell back to local Ollama and
never touched Gemini's quota at all. A pool sized to the actual Gemini
free-tier limit (5 req/min) throttles just the calls that need it.

Create the pool once (UI: Admin -> Pools, or CLI):
    airflow pools set gemini_api_pool 5 "Gemini free-tier rate limit (5 req/min)"

KNOWN GAP: this DAG's build_batches only sees reviews still unread on the
Kafka topic. A review whose Kafka offset already committed but whose
call_llm instance never completed (worker restart, etc.) will NOT be
retried here -- see src/batching/build_batches.py module docstring, and
review_sentiment_backfill.py, which catches that gap on its own schedule.
"""
from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta

if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")

from airflow.decorators import dag, task

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="review_sentiment_pipeline",
    description="Poll Play Store reviews -> Kafka -> batch -> LLM sentiment -> Postgres",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["review-pipeline"],
)
def review_sentiment_pipeline():

    @task
    def extract() -> None:
        from src.extract.producer import main as producer_main
        producer_main()

    @task
    def build_batches() -> list:
        from src.batching.build_batches import consume_and_build_batches
        return consume_and_build_batches()

    @task(pool="gemini_api_pool")
    def call_llm(batch: list) -> None:
        from src.batching.build_batches import process_batch
        batch_id = str(uuid.uuid4())
        process_batch(batch_id, batch)

    extracted = extract()
    batches = build_batches()
    extracted >> batches

    call_llm.expand(batch=batches)


review_sentiment_pipeline()
