"""
Idempotent write functions for review_sentiment, failed_batches, and llm_usage.
Called once per batch, right after the LLM step returns (or fails).
"""
import logging
from typing import Optional 

logger = logging.getLogger(__name__)


def upsert_review_sentiment(cur, batch_id: str, model_used: str, results: list) -> None:
    """
    Upsert one row per review result.
    ON CONFLICT (review_id) DO UPDATE keeps this idempotent -- reprocessing
    the same review overwrites the existing row instead of duplicating it.
    """
    for r in results:
        cur.execute(
            """
            INSERT INTO review_sentiment (review_id, sentiment_score, category, summary, model_used, batch_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (review_id) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                category = EXCLUDED.category,
                summary = EXCLUDED.summary,
                model_used = EXCLUDED.model_used,
                batch_id = EXCLUDED.batch_id,
                processed_at = now();
            """,
            (r.review_id, r.sentiment_score, r.category, r.summary, model_used, batch_id),
        )
    logger.info("Upserted %d sentiment rows for batch %s", len(results), batch_id)


def insert_failed_batch(
    cur, batch_id: str, error_message: str, raw_llm_response: str, review_id: Optional[str] = None
) -> None:
    cur.execute(
        """
        INSERT INTO failed_batches (batch_id, review_id, raw_llm_response, error_message)
        VALUES (%s, %s, %s, %s);
        """,
        (batch_id, review_id, raw_llm_response, error_message),
    )
    logger.warning("Batch %s failed validation: %s", batch_id, error_message)


def insert_llm_usage(
    cur,
    batch_id: str,
    review_count: int,
    input_tokens: int,
    output_tokens: int,
    model_used: str,
    estimated_cost_usd: float = 0.0,
) -> None:
    cur.execute(
        """
        INSERT INTO llm_usage (batch_id, review_count, input_tokens, output_tokens, model_used, estimated_cost_usd)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (batch_id) DO NOTHING;
        """,
        (batch_id, review_count, input_tokens, output_tokens, model_used, estimated_cost_usd),
    )