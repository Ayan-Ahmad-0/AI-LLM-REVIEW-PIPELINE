"""
Producer: polls the playstore-api wrapper service for reviews of a fixed list
of apps, and publishes each NEW review as a JSON message to the Kafka topic
'reviews.raw'. Dedup is done by checking review_id against Postgres
(raw_reviews table) before publishing, so reruns don't republish everything.

Run manually for testing:
    python producer.py

In production this is triggered by an Airflow task, not run standalone forever —
Airflow calls main() once per scheduled interval (micro-batch style), rather
than this script polling in an infinite loop itself.
"""
import json
import logging
import os

import psycopg2
import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PLAYSTORE_API_URL = os.environ.get("PLAYSTORE_API_URL", "http://playstore-api:8000")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW_REVIEWS = os.environ.get("KAFKA_TOPIC_RAW_REVIEWS", "reviews.raw")
APP_DB_CONN_PARTS = os.environ.get(
    "APP_DB_CONN", "postgresql+psycopg2://app_user:app_password@app-postgres:5432/reviews"
)

# Apps to pull reviews for — extend this list as needed.
# package_id -> display name, attached to every review as app_name before
# it's published to Kafka, so it's available on raw_reviews without ever
# having to be inferred/joined back from product_id downstream.
APP_NAME_MAP = {
    "com.spotify.music": "Spotify",
    "com.instagram.android": "Instagram",
    "com.duolingo": "Duolingo",
}

REVIEWS_PER_APP = 50


def get_existing_review_ids(review_ids: list[str]) -> set[str]:
    """Check which of these review_ids already exist in raw_reviews, to avoid republishing."""
    if not review_ids:
        return set()

    # psycopg2 needs a plain postgresql:// DSN, not the SQLAlchemy-style postgresql+psycopg2://
    dsn = APP_DB_CONN_PARTS.replace("postgresql+psycopg2://", "postgresql://")

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT review_id FROM raw_reviews WHERE review_id = ANY(%s);",
                (review_ids,),
            )
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def fetch_reviews(package_id: str, count: int = REVIEWS_PER_APP) -> list[dict]:
    resp = requests.get(
        f"{PLAYSTORE_API_URL}/reviews",
        params={"package_id": package_id, "count": count},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["reviews"]


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    total_published = 0

    for package_id, app_name in APP_NAME_MAP.items():
        logger.info("Fetching reviews for %s (%s)", app_name, package_id)
        try:
            fetched = fetch_reviews(package_id)
        except requests.RequestException as e:
            logger.error("Failed to fetch reviews for %s: %s", package_id, e)
            continue

        review_ids = [r["review_id"] for r in fetched]
        existing_ids = get_existing_review_ids(review_ids)
        new_reviews = [r for r in fetched if r["review_id"] not in existing_ids]

        logger.info(
            "%s: fetched %d, %d already known, %d new",
            package_id, len(fetched), len(existing_ids), len(new_reviews),
        )

        for review in new_reviews:
            # Attach app_name here, once, at the point where we actually know
            # which app this review came from — every downstream consumer
            # (raw_reviews insert, dashboard) just reads it off the review
            # dict from here on, no re-derivation from product_id needed.
            review["app_name"] = app_name

            producer.send(
                KAFKA_TOPIC_RAW_REVIEWS,
                key=review["review_id"],
                value=review,
            )
            total_published += 1

    producer.flush()
    producer.close()
    logger.info("Done. Published %d new reviews to %s", total_published, KAFKA_TOPIC_RAW_REVIEWS)


if __name__ == "__main__":
    main()