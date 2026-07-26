"""
Consumer: reads reviews off the 'reviews.raw' Kafka topic, inserts each into
the raw_reviews table (idempotent — ON CONFLICT DO NOTHING), and groups
reviews into batches sized by an approximate token count, ready for the LLM
step (added in the next stage).

Manual offset commit happens ONLY after a message's Postgres insert has
succeeded, so a crash mid-run just reprocesses the same message safely
(the insert is idempotent) rather than silently dropping it.

Run manually for testing:
    python build_batches.py

Stops automatically after ~10s of no new messages (consumer_timeout_ms) —
this matches how Airflow will trigger it periodically, rather than running
as an always-on service.
"""
import json
import logging
import os
import uuid

import psycopg2
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW_REVIEWS = os.environ.get("KAFKA_TOPIC_RAW_REVIEWS", "reviews.raw")
CONSUMER_GROUP_ID = "review-batcher"
APP_DB_CONN = os.environ.get(
    "APP_DB_CONN", "postgresql+psycopg2://app_user:app_password@app-postgres:5432/reviews"
)

# Batch sizing — tune these based on your LLM's context window and desired cost per call
MAX_BATCH_TOKENS = 3000
MAX_BATCH_REVIEWS = 25


def estimate_tokens(text: str) -> int:
    """Rough token estimate (~4 chars/token). Good enough for batch sizing;
    swap for a real tokenizer (e.g. anthropic's count_tokens) if you need precision."""
    return max(1, len(text) // 4)


def insert_raw_review(cur, review: dict) -> None:
    cur.execute(
        """
        INSERT INTO raw_reviews (review_id, product_id, review_text, reviewer_name, rating, source, source_posted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (review_id) DO NOTHING;
        """,
        (
            review["review_id"],
            review["product_id"],
            review["review_text"],
            review.get("reviewer_name"),
            review.get("rating"),
            review["source"],
            review.get("source_posted_at"),
        ),
    )


def build_batches() -> list[dict]:
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

    batches: list[dict] = []
    current_batch: list[dict] = []
    current_tokens = 0

    def flush_batch():
        nonlocal current_batch, current_tokens
        if not current_batch:
            return
        batch_id = str(uuid.uuid4())
        logger.info("Batch %s ready: %d reviews, ~%d tokens", batch_id, len(current_batch), current_tokens)
        batches.append({"batch_id": batch_id, "reviews": current_batch})
        current_batch = []
        current_tokens = 0

    try:
        with conn.cursor() as cur:
            for message in consumer:
                review = message.value
                insert_raw_review(cur, review)
                conn.commit()

                # Manual offset commit only after the DB write succeeds
                consumer.commit()

                tokens = estimate_tokens(review["review_text"])
                if current_batch and (
                    current_tokens + tokens > MAX_BATCH_TOKENS or len(current_batch) >= MAX_BATCH_REVIEWS
                ):
                    flush_batch()

                current_batch.append(review)
                current_tokens += tokens

        flush_batch()  # flush whatever's left after the loop ends
    finally:
        conn.close()
        consumer.close()

    logger.info("Done. Built %d batches from this run.", len(batches))
    return batches


if __name__ == "__main__":
    result_batches = build_batches()
    for b in result_batches:
        print(f"batch_id={b['batch_id']} reviews={len(b['reviews'])}")