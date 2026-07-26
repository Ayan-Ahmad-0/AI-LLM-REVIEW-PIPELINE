-- Schema for the "reviews" app database (app-postgres in docker-compose.yml).
-- This file is auto-run once, on first container startup, via the
-- docker-entrypoint-initdb.d mount in docker-compose.yml.
-- NOTE: it only runs the FIRST time the app-postgres volume is created.
-- If you edit this file later, you must `docker-compose down -v` (drops the
-- volume) and re-up for changes to take effect — or apply changes manually
-- with psql/a migration tool.

-- =========================================================
-- raw_reviews: immutable landing table for incoming reviews
-- =========================================================
CREATE TABLE IF NOT EXISTS raw_reviews (
    review_id           TEXT PRIMARY KEY,        -- stable ID from the source API (or a hash if the source has none)
    product_id          TEXT NOT NULL,
    review_text         TEXT NOT NULL,
    reviewer_name        TEXT,
    rating              SMALLINT,                -- original star rating from source, if available (1-5), nullable
    source               TEXT NOT NULL,           -- e.g. 'best_buy', 'play_store', 'reddit'
    source_posted_at     TIMESTAMPTZ,             -- when the review was actually posted, per the source
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT now()  -- when our producer picked it up
);

CREATE INDEX IF NOT EXISTS idx_raw_reviews_product_id ON raw_reviews (product_id);
CREATE INDEX IF NOT EXISTS idx_raw_reviews_ingested_at ON raw_reviews (ingested_at);

-- =========================================================
-- review_sentiment: LLM output, one row per review, upserted
-- =========================================================
CREATE TABLE IF NOT EXISTS review_sentiment (
    review_id       TEXT PRIMARY KEY REFERENCES raw_reviews (review_id),
    sentiment_score NUMERIC(4, 3) NOT NULL,       -- e.g. -1.000 to 1.000, or 0.000 to 1.000 — pick one convention and stick to it
    category        TEXT NOT NULL,                -- e.g. 'shipping_complaint', 'product_praise', 'pricing_feedback'
    summary         TEXT NOT NULL,                -- one-line LLM-generated summary
    model_used      TEXT NOT NULL,                -- e.g. 'claude-sonnet-4-6'
    batch_id        TEXT NOT NULL,                -- links back to the batch this was processed in (see llm_usage)
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_review_sentiment_category ON review_sentiment (category);
CREATE INDEX IF NOT EXISTS idx_review_sentiment_processed_at ON review_sentiment (processed_at);

-- =========================================================
-- failed_batches: anything that failed schema validation
-- =========================================================
CREATE TABLE IF NOT EXISTS failed_batches (
    id              SERIAL PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    review_id       TEXT,                          -- nullable: sometimes a whole batch fails before per-review parsing
    raw_llm_response TEXT,                          -- the actual (malformed) response, for debugging
    error_message   TEXT NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_failed_batches_batch_id ON failed_batches (batch_id);

-- =========================================================
-- llm_usage: token/cost tracking per batch
-- =========================================================
CREATE TABLE IF NOT EXISTS llm_usage (
    batch_id         TEXT PRIMARY KEY,
    review_count     INTEGER NOT NULL,
    input_tokens     INTEGER NOT NULL,
    output_tokens    INTEGER NOT NULL,
    model_used       TEXT NOT NULL,
    estimated_cost_usd NUMERIC(10, 6),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);