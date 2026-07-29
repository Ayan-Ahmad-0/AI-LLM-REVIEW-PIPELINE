"""
Airflow DAG: safety-net for the main review_sentiment_pipeline DAG.

build_backfill_batches (query Postgres for raw_reviews rows with no
review_sentiment match) -> call_llm (dynamic mapping, same as main DAG)

WHY THIS EXISTS: the main pipeline's Kafka offset commits as soon as a
review lands in raw_reviews -- not once it's actually been scored. So a
review whose batch never finished processing (worker restart, task stuck
behind the pool, etc.) has no way to be re-picked-up by Kafka, since its
offset is already committed and gone. This DAG treats Postgres itself as
the source of truth for "what still needs scoring," independent of Kafka
state, and catches anything the main DAG missed.

See src/batching/build_batches.py -- consume_and_build_backfill_batches()
for the full explanation and query.

Runs every 4 hours, deliberately less frequent than the main hourly DAG,
since this is a catch-up/safety-net job, not the primary path. Shares the
same gemini_api_pool as the main DAG so both compete fairly for the same
Gemini quota rather than each assuming they own the full 5 slots.
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
    dag_id="review_sentiment_backfill",
    description="Safety net: catches raw_reviews rows that never got scored by the main pipeline",
    schedule_interval="0 */4 * * *",  # every 4 hours
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["review-pipeline", "backfill"],
)
def review_sentiment_backfill():

    @task
    def build_backfill_batches() -> list:
        from src.batching.build_batches import consume_and_build_backfill_batches
        return consume_and_build_backfill_batches()

    @task(pool="gemini_api_pool")
    def call_llm(batch: list) -> None:
        from src.batching.build_batches import process_batch
        batch_id = str(uuid.uuid4())
        process_batch(batch_id, batch)

    call_llm.expand(batch=build_backfill_batches())


review_sentiment_backfill()