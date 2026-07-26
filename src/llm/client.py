"""
Wraps the Gemini API call for one batch of reviews.

NOTE: Using the legacy `google-generativeai` package (not `google-genai`)
because this project's Airflow image (apache/airflow:2.5.1) is pinned to
Python 3.9, and `google-genai` requires Python 3.10+. `google-generativeai`
is deprecated upstream but still installable and functional on 3.9.
If the Airflow base image is ever upgraded to a 3.10+-compatible version,
this should be migrated back to `google-genai`.

Uses Gemini's structured-output mode (response_schema) to force valid,
schema-matching JSON directly -- the SDK accepts our pydantic model as the
schema.

We still re-validate with pydantic explicitly (belt-and-suspenders): if
Gemini's own enforcement ever slips, we still catch it before anything
reaches Postgres.

Retries with exponential backoff on rate limits / transient errors via tenacity.
"""
import logging
import os

import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
from pydantic import ValidationError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .prompts import SYSTEM_PROMPT, build_batch_prompt
from .schema import BatchSentimentResults, GeminiBatchSentimentResults

logger = logging.getLogger(__name__)

MODEL_NAME = "gemini-3.5-flash"  # free tier model (gemini-2.5-flash deprecated for new users)

genai.configure(api_key=os.environ["GEMINI_API_KEY"])

_model = genai.GenerativeModel(
    model_name=MODEL_NAME,
    system_instruction=SYSTEM_PROMPT,
)


class LLMValidationError(Exception):
    """Raised when Gemini's response doesn't match our expected schema."""

    def __init__(self, message: str, raw_response: str):
        super().__init__(message)
        self.raw_response = raw_response


@retry(
    retry=retry_if_exception_type(
        (google_exceptions.ServerError, google_exceptions.ResourceExhausted, google_exceptions.ServiceUnavailable)
    ),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _call_gemini(prompt: str):
    return _model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(
            response_mime_type="application/json",
            response_schema=GeminiBatchSentimentResults,
        ),
    )


def analyze_batch(reviews: list[dict]) -> dict:
    """
    Send one batch of reviews to Gemini for sentiment analysis.

    Returns a dict:
        {
            "results": [ReviewSentimentResult, ...],   # validated pydantic objects
            "input_tokens": int,
            "output_tokens": int,
        }

    Raises LLMValidationError if the response doesn't match the expected schema --
    caller should catch this and write to failed_batches.
    """
    prompt = build_batch_prompt(reviews)
    response = _call_gemini(prompt)

    try:
        raw_text = response.text
    except (ValueError, AttributeError) as e:
        raise LLMValidationError(f"Gemini returned no usable text: {e}", raw_response=str(response)) from None

    try:
        validated = BatchSentimentResults.model_validate_json(raw_text)
    except ValidationError as e:
        raise LLMValidationError(str(e), raw_response=raw_text) from e

    usage = response.usage_metadata
    return {
        "results": validated.results,
        "input_tokens": usage.prompt_token_count if usage else 0,
        "output_tokens": usage.candidates_token_count if usage else 0,
    }