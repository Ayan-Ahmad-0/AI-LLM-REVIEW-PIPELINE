"""
src/llm/client.py

LLM client for the review sentiment pipeline. Two-tier fallback chain:

1. gemini-3.5-flash-lite  (primary — higher free-tier RPD, good enough for
   classification/extraction/routing-style tasks like this one)
2. gemini-3.5-flash       (fallback — separate RPD quota bucket; quota is
   tracked per-model, not account-wide, so Flash-Lite exhausting its quota
   still leaves Flash available)

NOTE (2026-07-29): Ollama removed as a last-resort tier for now. If both
Gemini models exhaust today's quota, analyze_batch() raises
LLMAllProvidersFailedError and the caller should write the batch to
failed_batches — there is currently no further fallback. Revisit later if
this turns out to be a real problem in practice.

analyze_batch() returns a plain dict (not a dataclass) to match how
build_batches.process_batch() consumes it:
    {
        "results": [ {review_id, sentiment_score, category, summary}, ... ],
        "input_tokens": int,
        "output_tokens": int,
        "model_used": str,   # whichever tier actually served this batch
    }

Uses the legacy `google-generativeai` SDK (not `google-genai`) because the
Airflow image is pinned to Python 3.9 and `google-genai` requires 3.10+.
"""

import logging
import os

import google.generativeai as genai
from pydantic import BaseModel, ValidationError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from src.llm.prompts import SYSTEM_PROMPT, build_batch_prompt
from src.llm.schema import BatchSentimentResults

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

MODEL_NAME_PRIMARY = "gemini-3.5-flash-lite"
MODEL_NAME_FALLBACK = "gemini-3.5-flash"

genai.configure(api_key=os.environ["GEMINI_API_KEY"])


# ---------------------------------------------------------------------------
# Constraint-free schema pair for Gemini's response_schema only.
#
# The legacy google-generativeai SDK's protobuf-based schema converter
# doesn't support JSON Schema `minimum`/`maximum` keywords, which
# pydantic.Field(ge=..., le=...) generates on the strict models in schema.py.
# These mirror ReviewSentimentResult / BatchSentimentResults field-for-field
# but with no range/length constraints, purely so the SDK can build a valid
# protobuf Schema. The strict models are still used afterward to actually
# validate the response.
# ---------------------------------------------------------------------------


class GeminiReviewSentimentResult(BaseModel):
    review_id: str
    sentiment_score: float
    category: str
    summary: str


class GeminiBatchSentimentResults(BaseModel):
    results: list[GeminiReviewSentimentResult]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class LLMValidationError(Exception):
    """Raised when a response fails pydantic validation even after Gemini's
    own schema enforcement. Carries the raw (malformed) response text so
    callers can log it to failed_batches.raw_llm_response for debugging."""

    def __init__(self, message: str, raw_response: str = ""):
        super().__init__(message)
        self.raw_response = raw_response


class LLMAllProvidersFailedError(Exception):
    """Raised when both Flash-Lite and Flash have exhausted today's quota.
    The caller should write the batch to failed_batches."""


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

_QUOTA_MARKERS = ("429", "quota", "rate limit", "resourceexhausted")
_TRANSIENT_MARKERS = ("503", "service unavailable", "internal", "deadline exceeded", "500")


def is_quota_exhausted(exc: Exception) -> bool:
    """429 / daily-quota errors — retrying within a short backoff window
    won't help, since the quota won't reset for hours. Skip straight to the
    next tier instead of burning retry time."""
    text = str(exc).lower()
    return any(marker in text for marker in _QUOTA_MARKERS)


def is_transiently_retryable(exc: Exception) -> bool:
    """503 / transient server errors — genuinely worth a short retry."""
    text = str(exc).lower()
    return any(marker in text for marker in _TRANSIENT_MARKERS)


# ---------------------------------------------------------------------------
# Gemini call, takes model_name as a parameter so the same function serves
# both the primary and fallback calls.
# ---------------------------------------------------------------------------

MAX_SCHEMA_VALIDATION_ATTEMPTS = 2


@retry(
    retry=retry_if_exception(is_transiently_retryable),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, max=20),
    reraise=True,
)
def _call_gemini(model_name: str, prompt: str) -> tuple[GeminiBatchSentimentResults, int, int]:
    """Call a single Gemini model with structured output. Retries a bounded
    number of times on transient (503-style) errors only — quota errors are
    not retried here, since is_quota_exhausted() routes those to the caller
    for fallback handling instead.

    Separately: response_schema enforcement on the legacy SDK is best-effort,
    not a hard guarantee -- observed to occasionally omit a required field
    (e.g. category) from a result even though the pydantic schema has no
    default for it. This looks intermittent rather than deterministic (most
    calls succeed with all fields present), so a same-model retry is
    attempted up to MAX_SCHEMA_VALIDATION_ATTEMPTS times before giving up
    and raising LLMValidationError with the last raw response attached.
    """
    model = genai.GenerativeModel(model_name)

    last_exc = None
    last_raw_text = ""

    for attempt in range(1, MAX_SCHEMA_VALIDATION_ATTEMPTS + 1):
        response = model.generate_content(
            [SYSTEM_PROMPT, prompt],
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                response_schema=GeminiBatchSentimentResults,
                max_output_tokens=8192,
            ),
        )

        tokens_in = response.usage_metadata.prompt_token_count
        tokens_out = response.usage_metadata.candidates_token_count

        try:
            parsed = GeminiBatchSentimentResults.model_validate_json(response.text)
            return parsed, tokens_in, tokens_out
        except ValidationError as exc:
            last_exc = exc
            last_raw_text = response.text
            logger.warning(
                "%s omitted a required field on attempt %d/%d, retrying...",
                model_name, attempt, MAX_SCHEMA_VALIDATION_ATTEMPTS,
            )

    # Capture the raw response here -- this is the point where response.text
    # is in scope. Letting this propagate unwrapped meant process_batch's
    # generic except Exception caught it as "Unexpected error" with an empty
    # raw_llm_response, losing the one piece of data that actually explains
    # what Gemini returned instead of a valid category/etc.
    raise LLMValidationError(str(last_exc), raw_response=last_raw_text) from last_exc


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def analyze_batch(reviews: list) -> dict:
    """Run a batch of reviews through the fallback chain:
    Gemini Flash-Lite -> Gemini Flash.

    Returns:
        {
            "results": [ {review_id, sentiment_score, category, summary}, ... ],
            "input_tokens": int,
            "output_tokens": int,
            "model_used": str,
        }

    Raises:
        LLMAllProvidersFailedError: both tiers failed (quota exhaustion).
        LLMValidationError: a response failed pydantic validation.
    """
    prompt = build_batch_prompt(reviews)

    for model_name in (MODEL_NAME_PRIMARY, MODEL_NAME_FALLBACK):
        try:
            raw_result, tokens_in, tokens_out = _call_gemini(model_name, prompt)
        except Exception as exc:
            if is_quota_exhausted(exc):
                logger.warning(
                    "Quota exhausted on %s, trying next tier...", model_name, exc_info=False
                )
                continue
            # Transient errors already retried inside _call_gemini and
            # re-raised past stop_after_attempt. Surface these directly
            # rather than falling through the chain, so a real bug on
            # Flash-Lite doesn't get masked by a "successful" Flash call —
            # only quota exhaustion routes to the next tier.
            logger.error("Non-quota error calling %s: %s", model_name, exc)
            raise

        try:
            validated = BatchSentimentResults.model_validate(raw_result.model_dump())
        except ValidationError as exc:
            logger.error("Validation failed for response from %s: %s", model_name, exc)
            raise LLMValidationError(str(exc), raw_response=raw_result.model_dump_json()) from exc

        return {
            # Pydantic objects, not dicts -- upsert_review_sentiment reads
            # these via attribute access (r.review_id, r.sentiment_score, ...).
            "results": validated.results,
            "input_tokens": tokens_in,
            "output_tokens": tokens_out,
            "model_used": model_name,
        }

    # Both Gemini tiers exhausted their quota — no further fallback for now.
    raise LLMAllProvidersFailedError(
        f"{MODEL_NAME_PRIMARY} and {MODEL_NAME_FALLBACK} both exhausted today's "
        f"quota. Batch should be written to failed_batches."
    )