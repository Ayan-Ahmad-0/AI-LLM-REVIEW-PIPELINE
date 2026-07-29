"""
Tiny FastAPI wrapper around google-play-scraper.
Makes Play Store reviews behave like a normal live REST API that our
Airflow producer can poll, instead of importing the scraper directly
into Airflow's own container.

Endpoint:
  GET /reviews?package_id=com.spotify.music&count=50

Returns a list of reviews in a shape close to our raw_reviews schema.
"""
from fastapi import FastAPI, HTTPException, Query
from google_play_scraper import Sort, reviews

app = FastAPI(title="Play Store Reviews API Wrapper")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/reviews")
def get_reviews(
    package_id: str = Query(..., description="Play Store app package ID, e.g. com.spotify.music"),
    count: int = Query(50, ge=1, le=200, description="Number of reviews to fetch"),
):
    try:
        result, _ = reviews(
            package_id,
            lang="en",
            country="us",
            sort=Sort.NEWEST,
            count=count,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch reviews for {package_id}: {e}")

    # Normalize field names to match what our producer/schema expects
    normalized = [
        {
            "review_id": r["reviewId"],
            "product_id": package_id,
            "review_text": r["content"],
            "reviewer_name": r.get("userName"),
            "rating": r.get("score"),
            "app_version": r.get("reviewCreatedVersion"),   # NEW
            "likes_count": r.get("thumbsUpCount"),
            "source": "play_store",
            "source_posted_at": r["at"].isoformat() if r.get("at") else None,
        }
        for r in result
    ]
    return {"package_id": package_id, "count": len(normalized), "reviews": normalized}