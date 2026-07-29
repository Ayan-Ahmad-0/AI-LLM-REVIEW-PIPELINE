"""
Consumer + batching: reads reviews off the 'reviews.raw' Kafka topic, inserts
each into raw_reviews (idempotent), and groups reviews into token-aware
batches.

Refactored for Airflow (step 7): consuming/batching and LLM-processing are now
SEPARATE functions, not one loop. This lets Airflow treat them as separate
tasks -- build_batches produces a list of batches, and call_llm is dynamically
mapped over that list (one task instance per batch), so one bad batch retries
or fails independently in the Airflow UI instead of blocking the rest of the run.

Manual Kafka offset commit happens ONLY after a message's raw_reviews insert
succeeds -- same durability guarantee as before: a crash never loses or
duplicates a message, it just gets re-consumed and re-inserted
(ON CONFLICT DO NOTHING) on the next run.

IMPORTANT CAVEAT (discovered after backlog investigation): the offset commit
above only guarantees Kafka -> raw_reviews durability. It does NOT guarantee
raw_reviews -> review_sentiment durability -- once a message's offset is
committed, Kafka will never redeliver it, even if that review's batch never
makes it through call_llm to completion (task never finishes, worker
restarts, etc). That gap is what consume_and_build_backfill_batches() below
exists to catch, by treating Postgres (not Kafka offsets) as the source of
truth for "what still needs scoring." See review_sentiment_backfill DAG.

Run manually for testing (still does everything in one process, like before):
    python -m src.batching.build_batches

Stops automatically after ~10s of no new messages (consumer_timeout_ms) --
this matches how Airflow triggers it periodically, rather than running as an
always-on service.
"""
import json
import logging
import os
import uuid

import psycopg2
from kafka import KafkaConsumer

from ..llm.client import LLMValidationError, analyze_batch
from ..storage.upsert import insert_failed_batch, insert_llm_usage, upsert_review_sentiment

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW_REVIEWS = os.environ.get("KAFKA_TOPIC_RAW_REVIEWS", "reviews.raw")
CONSUMER_GROUP_ID = "review-batcher"
APP_DB_CONN = os.environ.get(
    "APP_DB_CONN", "postgresql+psycopg2://app_user:app_password@app-postgres:5432/reviews"
)

# Batch sizing -- tune based on the LLM's context window and desired cost per call
MAX_BATCH_TOKENS = 3000
MAX_BATCH_REVIEWS = 15


def estimate_tokens(text: str) -> int:
    """Rough token estimate (~4 chars/token). Good enough for batch sizing;
    swap for a real tokenizer if you need precision."""
    return max(1, len(text) // 4)


def insert_raw_review(cur, review: dict) -> None:
    cur.execute(
        """
        INSERT INTO raw_reviews (review_id, product_id, app_name, review_text, reviewer_name, rating, source, source_posted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (review_id) DO NOTHING;
        """,
        (
            review["review_id"],
            review["product_id"],
            review["app_name"],
            review["review_text"],
            review.get("reviewer_name"),
            review.get("rating"),
            review["source"],
            review.get("source_posted_at"),
        ),
    )


def _batch_reviews(reviews: list) -> list:
    """
    Shared token/count-aware batching logic used by both the Kafka-driven
    consume_and_build_batches() and the Postgres-driven
    consume_and_build_backfill_batches(), so the two paths can never drift
    out of sync on batch sizing rules.
    """
    batches: list = []
    current_batch: list = []
    current_tokens = 0

    def flush():
        nonlocal current_batch, current_tokens
        if current_batch:
            batches.append(current_batch)
            current_batch = []
            current_tokens = 0

    for review in reviews:
        tokens = estimate_tokens(review["review_text"])
        if current_batch and (
            current_tokens + tokens > MAX_BATCH_TOKENS or len(current_batch) >= MAX_BATCH_REVIEWS
        ):
            flush()
        current_batch.append(review)
        current_tokens += tokens

    flush()
    return batches


def consume_and_build_batches() -> list:
    """
    Consumes everything currently available on 'reviews.raw', inserts each
    review into raw_reviews (idempotent), and groups reviews into token-aware
    batches. Returns the list of batches (each batch = list of review dicts)
    WITHOUT calling the LLM -- that's a separate step now (process_batch /
    the call_llm Airflow task), mapped once per batch.

    NOTE: the Kafka offset commits as soon as a review lands in raw_reviews,
    not once it's actually been scored -- so this function's job is strictly
    "get new reviews durably into Postgres." Whether they actually get scored
    afterward is consume_and_build_backfill_batches()'s job to double check.
    """
    dsn = APP_DB_CONN.replace("postgresql+psycopg2://", "postgresql://")

    consumer = KafkaConsumer(
        KAFKA_TOPIC_RAW_REVIEWS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=10000,  # stop after 10s of no new messages
    )
    conn = psycopg2.connect(dsn)

    reviews: list = []

    try:
        for message in consumer:
            review = message.value

            with conn.cursor() as cur:
                insert_raw_review(cur, review)
            conn.commit()

            # Manual offset commit only after the raw_reviews insert succeeds.
            # See module docstring: this guarantees Kafka -> raw_reviews
            # durability, NOT raw_reviews -> review_sentiment durability.
            consumer.commit()

            reviews.append(review)
    finally:
        conn.close()
        consumer.close()

    batches = _batch_reviews(reviews)
    logger.info("Built %d batches from this consume run.", len(batches))
    return batches


def consume_and_build_backfill_batches() -> list:
    """
    Treats Postgres (not Kafka offsets) as the source of truth for "what
    still needs scoring": finds every raw_reviews row with no matching
    review_sentiment row and re-batches them, catching anything orphaned by
    the Kafka-offset-vs-scoring durability gap described in the module
    docstring.
    """
    dsn = APP_DB_CONN.replace("postgresql+psycopg2://", "postgresql://")
    conn = psycopg2.connect(dsn)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.review_id, r.product_id, r.app_name, r.review_text, r.reviewer_name,
                       r.rating, r.source, r.source_posted_at
                FROM raw_reviews r
                LEFT JOIN review_sentiment s ON r.review_id = s.review_id
                WHERE s.review_id IS NULL
                ORDER BY r.source_posted_at ASC;
                """
            )
            columns = [
                "review_id", "product_id", "app_name", "review_text",
                "reviewer_name", "rating", "source", "source_posted_at",
            ]
            unscored_reviews = [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()

    # source_posted_at comes back from psycopg2 as a native datetime object,
    # which Airflow's XCom JSON serializer cannot handle when this function's
    # return value is pushed to XCom for call_llm.expand() to consume. Kafka
    # messages don't hit this because they're already JSON-native strings by
    # the time they're deserialized -- this path pulls straight from Postgres,
    # so needs an explicit conversion.
    for review in unscored_reviews:
        if review["source_posted_at"] is not None:
            review["source_posted_at"] = review["source_posted_at"].isoformat()

    logger.info("Found %d unscored review(s) in raw_reviews with no review_sentiment match.", len(unscored_reviews))

    batches = _batch_reviews(unscored_reviews)
    logger.info("Built %d backfill batch(es) from %d unscored review(s).", len(batches), len(unscored_reviews))
    return batches


def process_batch(batch_id: str, reviews: list) -> None:
    """
    Send one batch through the LLM, then write results (or failure) to Postgres.
    Opens and closes its own DB connection -- called once per batch, either
    from the standalone run() loop below, or from Airflow's dynamically-mapped
    call_llm task, where each mapped task instance runs as its own process.
    """
    dsn = APP_DB_CONN.replace("postgresql+psycopg2://", "postgresql://")
    conn = psycopg2.connect(dsn)
    try:
        try:
            result = analyze_batch(reviews)
        except LLMValidationError as e:
            with conn.cursor() as cur:
                insert_failed_batch(cur, batch_id, error_message=str(e), raw_llm_response=e.raw_response)
            conn.commit()
            return
        except Exception as e:
            # Catch-all for genuinely unexpected errors (e.g. network issues after
            # tenacity's retries are exhausted, or all three fallback tiers failing)
            # -- still logged, never crashes the run
            with conn.cursor() as cur:
                insert_failed_batch(cur, batch_id, error_message=f"Unexpected error: {e}", raw_llm_response="")
            conn.commit()
            return

        # model_used comes back per-call now, not a single hardcoded constant --
        # with 3 fallback tiers (Flash-Lite / Flash / Ollama), which model
        # actually served a given batch varies, and llm_usage / review_sentiment
        # should reflect the real one.
        model_used = result["model_used"]

        with conn.cursor() as cur:
            upsert_review_sentiment(cur, batch_id, model_used, result["results"])
            insert_llm_usage(
                cur,
                batch_id=batch_id,
                review_count=len(reviews),
                input_tokens=result["input_tokens"],
                output_tokens=result["output_tokens"],
                model_used=model_used,
                estimated_cost_usd=0.0,  # Gemini free tier / local Ollama -- adjust if you switch to a paid model
            )
        conn.commit()
        logger.info("Batch %s processed successfully: %d reviews (model: %s)", batch_id, len(reviews), model_used)
    finally:
        conn.close()


def run() -> None:
    """
    Standalone manual-testing entrypoint: consumes, batches, AND processes
    each batch through the LLM in one run -- same end-to-end behavior as
    before the Airflow refactor. NOTE: Airflow itself does NOT call this
    function; the DAG calls consume_and_build_batches() and process_batch()
    as two separate tasks (see dags/review_sentiment_pipeline.py).
    """
    batches = consume_and_build_batches()
    for reviews in batches:
        batch_id = str(uuid.uuid4())
        logger.info("Batch %s ready: %d reviews -- sending to LLM", batch_id, len(reviews))
        process_batch(batch_id, reviews)

    logger.info("Done. Processed %d batches this run.", len(batches))


if __name__ == "__main__":
    run()