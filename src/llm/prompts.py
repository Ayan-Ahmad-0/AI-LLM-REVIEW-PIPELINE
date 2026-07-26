"""
System prompt + user-prompt builder for the sentiment analysis LLM call.
Structured output is enforced via Gemini's response_schema (see client.py),
not a tool-use schema — this file only holds the prompt text itself.
"""

SYSTEM_PROMPT = (
    "You are a review analysis assistant. For each review you are given, "
    "determine a sentiment score from -1.0 (very negative) to 1.0 (very positive), "
    "assign it to a short category label (e.g. 'bug_report', 'feature_praise', "
    "'pricing_complaint', 'ui_feedback', 'performance_issue', 'general_praise', "
    "'general_complaint'), and write a one-sentence summary of what the reviewer "
    "actually said. Be concise and consistent in category naming across reviews "
    "in the same batch."
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