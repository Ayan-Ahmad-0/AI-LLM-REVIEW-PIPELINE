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

NOTE (2026-07-28): gemini-3.1-flash-lite occasionally fails to populate the
`category` field and instead appends a "category: xxx" tag to the end of
`summary`. sanitize_summary strips that leaked tag as a safety net -- the
real fix is tightening prompts.py so the model stops doing this in the
first place. This regex does NOT recover the lost category value itself;
it only prevents the leaked text from polluting summary going forward.
"""
import re

from pydantic import BaseModel, Field, field_validator

# Matches a trailing "category: some_label" (optionally with a trailing
# period) that the model sometimes appends to the end of the summary text.
_LEAKED_CATEGORY_TAG = re.compile(r"\s*category:\s*[a-z_]+\.?\s*$", re.IGNORECASE)


class ReviewSentimentResult(BaseModel):
    review_id: str
    sentiment_score: float = Field(..., ge=-1.0, le=1.0, description="-1.0 (very negative) to 1.0 (very positive)")
    category: str = Field(default="Uncategorized")
    summary: str

    @field_validator("category", mode="before")
    @classmethod
    def sanitize_category(cls, v: str) -> str:
        # If Gemini drops the field, passes None, or returns an empty string
        if not v or not isinstance(v, str) or not v.strip():
            return "Uncategorized"
        return v.strip()

    @field_validator("summary", mode="before")
    @classmethod
    def sanitize_summary(cls, v: str) -> str:
        if not v or not isinstance(v, str) or not v.strip():
            return "No summary provided."
        v = v.strip()
        # Strip a leaked "category: xxx" tag if the model appended one
        v = _LEAKED_CATEGORY_TAG.sub("", v).strip()
        # Automatically truncate to 300 chars instead of throwing a ValueError
        if len(v) > 300:
            return v[:297] + "..."
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