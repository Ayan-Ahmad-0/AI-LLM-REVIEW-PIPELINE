"""
Standalone test: pulls a handful of real reviews already sitting in
raw_reviews (from your earlier producer/consumer runs) and sends them
through the LLM client, printing the validated results.

This lets you check prompt quality and output shape BEFORE wiring the
LLM call into the full consumer pipeline.

Run:
    python -m src.llm.test_analyze
"""
import os

import psycopg2

from .client import LLMValidationError, analyze_batch

APP_DB_CONN = os.environ.get(
    "APP_DB_CONN", "postgresql+psycopg2://app_user:app_password@app-postgres:5432/reviews"
)


def fetch_sample_reviews(limit: int = 5) -> list[dict]:
    dsn = APP_DB_CONN.replace("postgresql+psycopg2://", "postgresql://")
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT review_id, product_id, review_text, rating FROM raw_reviews LIMIT %s;",
                (limit,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {"review_id": r[0], "product_id": r[1], "review_text": r[2], "rating": r[3]}
        for r in rows
    ]


def main():
    reviews = fetch_sample_reviews(limit=5)
    if not reviews:
        print("No reviews found in raw_reviews — run the producer and consumer first.")
        return

    print(f"Testing LLM analysis on {len(reviews)} sample reviews...\n")
    try:
        result = analyze_batch(reviews)
    except LLMValidationError as e:
        print(f"Validation failed: {e}")
        print(f"Raw response was: {e.raw_response}")
        return

    print(f"Tokens used: {result['input_tokens']} in / {result['output_tokens']} out\n")
    for item in result["results"]:
        print(f"- {item.review_id}: score={item.sentiment_score:+.2f} category={item.category!r}")
        print(f"    summary: {item.summary}")


if __name__ == "__main__":
    main()