"""
System prompt + user-prompt builder for the sentiment analysis LLM call.
Structured output is enforced via Gemini's response_schema (see client.py),
not a tool-use schema — this file only holds the prompt text itself.
"""

SYSTEM_PROMPT = (
    "You are a review analysis assistant. For each review you are given, return "
    "ALL of these fields for every review_id: review_id, sentiment_score, category, "
    "and summary.\n"
    "sentiment_score must be from -1.0 (very negative) to 1.0 (very positive).\n"
    "category must be a short snake_case label (e.g. 'bug_report', 'feature_praise', "
    "'general_complaint') placed ONLY in the category field.\n"
    "summary must be one concise plain-English sentence under 300 characters describing "
    "what the reviewer actually said. The summary must NEVER contain the word 'category', "
    "a colon-separated label, or any snake_case tag — categorization belongs exclusively "
    "in the category field, never appended to or mentioned within the summary text.\n"
    "Be consistent in category naming across reviews in the same batch."
)


def build_batch_prompt(reviews: list[dict]) -> str:
    """Turn a list of raw review dicts into the prompt text sent to Gemini."""
    lines = ["Analyze the following reviews and return one result per review_id.\n"]
    for r in reviews:
        lines.append(f"review_id: {r['review_id']}")
        lines.append(f"rating (if available): {r.get('rating', 'n/a')}")
        lines.append(f"text: {r['review_text']}")
        lines.append("")  # blank line between reviews
    return "\n".join(lines)