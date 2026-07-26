"""
Pydantic models used to validate the LLM's structured output before it's
allowed anywhere near Postgres. If a result fails validation, it goes to
failed_batches instead -- never silently dropped, never inserted malformed.

NOTE: two versions of the result model exist:
- ReviewSentimentResult / BatchSentimentResults: the STRICT model, with
  range/length constraints. Used to validate Gemini's response after the
  fact (belt-and-suspenders check in client.py).
- GeminiReviewSentimentResult / GeminiBatchSentimentResults: a
  constraint-free version passed to Gemini as `response_schema`. The
  legacy `google-generativeai` SDK converts the pydantic schema into a
  protobuf Schema message that does NOT support JSON Schema's
  "minimum"/"maximum" keywords (which Field(ge=..., le=...) produces) --
  passing those raises a ValueError deep in the proto marshalling code.
  So Gemini only sees plain types; the real range/length enforcement
  still happens via the strict model's validators after parsing.
"""
from pydantic import BaseModel, Field, field_validator


class ReviewSentimentResult(BaseModel):
    review_id: str
    sentiment_score: float = Field(..., ge=-1.0, le=1.0, description="-1.0 (very negative) to 1.0 (very positive)")
    category: str
    summary: str

    @field_validator("category")
    @classmethod
    def category_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("category must not be empty")
        return v.strip()

    @field_validator("summary")
    @classmethod
    def summary_length(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("summary must not be empty")
        if len(v) > 300:
            raise ValueError("summary must be under 300 characters")
        return v


class BatchSentimentResults(BaseModel):
    results: list[ReviewSentimentResult]


class GeminiReviewSentimentResult(BaseModel):
    """Constraint-free mirror of ReviewSentimentResult, safe to hand to
    Gemini's response_schema on the legacy SDK. No Field(ge=/le=/max_length=)."""

    review_id: str
    sentiment_score: float = Field(..., description="-1.0 (very negative) to 1.0 (very positive)")
    category: str
    summary: str


class GeminiBatchSentimentResults(BaseModel):
    results: list[GeminiReviewSentimentResult]