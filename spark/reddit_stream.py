import os, json, concurrent.futures, time, random, sys, re, html, threading
from xml.etree import ElementTree as ET
import requests as http_requests
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Reddit Data Ingestion Strategy (Upstream-First):
#
#   Priority 1: PRAW (Official Reddit API) — requires CLIENT_ID + SECRET
#   Priority 2: Reddit RSS Feed — NO credentials needed, publicly available
#               at https://www.reddit.com/r/{sub}/.rss
#   Priority 3: Live Simulation — absolute last resort when network is down
#
# Root Cause History:
#   - snscrape: deprecated CLI tool, never installed in Docker (removed)
#   - JSON endpoint: Reddit blocked unauthenticated access (403) since May 2026
#   - RSS Feed: currently the ONLY stable, no-auth method for real Reddit data
# ---------------------------------------------------------------------------

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USE_PRAW = REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET

if USE_PRAW:
    import praw
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent="thesis-reddit-stream/1.0"
    )
else:
    reddit = None

# --- Kafka connection with Docker/local fallback ---
try:
    import socket
    s = socket.socket()
    s.settimeout(2)
    s.connect(("kafka", 9092))
    s.close()

    producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=5000,
        api_version=(0, 10)
    )
    print("Connected to Kafka at kafka:9092")
except Exception:
    try:
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
            api_version=(0, 10)
        )
        print("Connected to Kafka at localhost:9092")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        raise e

SUBREDDITS = ["python", "datascience", "bigdata", "machinelearning", "technology"]
if len(sys.argv) > 1:
    SUBREDDITS = [s.strip() for s in sys.argv[1].split(",") if s.strip()]

# --- RSS Atom namespace ---
ATOM_NS = "{http://www.w3.org/2005/Atom}"
RSS_HEADERS = {
    "User-Agent": "thesis-reddit-stream/1.0 (educational data pipeline project)"
}

# Global rate limiter: Reddit allows ~10 req/min for unauthenticated users.
# This lock ensures only 1 RSS request fires at a time across all threads,
# with a mandatory 2-second gap between requests.
_rss_lock = threading.Lock()
_last_rss_request_time = 0.0
MAX_RSS_RETRIES = 5
RSS_MIN_INTERVAL = 10.0  # seconds between requests (Reddit is strict with unauth)


def strip_html(text):
    """Remove HTML tags and decode entities from RSS content."""
    clean = re.sub(r'<[^>]+>', '', text)
    return html.unescape(clean).strip()


def extract_score_from_content(content_html):
    """Try to extract score/upvotes from RSS HTML content (not always available)."""
    # RSS content sometimes contains "submitted by ... X points"
    match = re.search(r'(\d+)\s+points?', content_html)
    return int(match.group(1)) if match else 0


def _rate_limited_get(url):
    """
    Thread-safe, rate-limited HTTP GET with retry + exponential backoff for 429.
    Ensures all threads share a single request queue to avoid Reddit rate limits.
    """
    global _last_rss_request_time
    for attempt in range(MAX_RSS_RETRIES):
        with _rss_lock:
            # Enforce minimum interval between any two RSS requests
            elapsed = time.time() - _last_rss_request_time
            if elapsed < RSS_MIN_INTERVAL:
                time.sleep(RSS_MIN_INTERVAL - elapsed)
            _last_rss_request_time = time.time()

        resp = http_requests.get(url, headers=RSS_HEADERS, timeout=15)
        if resp.status_code == 429:
            wait = (2 ** attempt) * 5 + random.uniform(2, 5)
            print(f"[RATE-LIMIT] 429 received for {url}, waiting {wait:.1f}s (attempt {attempt+1}/{MAX_RSS_RETRIES})...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    # All retries exhausted
    raise Exception(f"Rate limited after {MAX_RSS_RETRIES} retries for {url}")


def fetch_reddit_rss(sub):
    """
    Fetch real posts from Reddit's public RSS/Atom feed.
    URL: https://www.reddit.com/r/{subreddit}/.rss
    No authentication required. Returns ~25 most recent posts per call.
    Uses rate-limited GET with automatic retry on 429.
    """
    url = f"https://www.reddit.com/r/{sub}/.rss"
    resp = _rate_limited_get(url)

    root = ET.fromstring(resp.text)
    posts = []

    for entry in root.findall(f"{ATOM_NS}entry"):
        post_id_raw = entry.findtext(f"{ATOM_NS}id", "")
        # Extract Reddit post ID from the full URL (e.g. /r/python/comments/abc123/...)
        id_match = re.search(r'/comments/([a-z0-9]+)', post_id_raw)
        post_id = id_match.group(1) if id_match else post_id_raw

        title = entry.findtext(f"{ATOM_NS}title", "")
        updated = entry.findtext(f"{ATOM_NS}updated", "")
        link_el = entry.find(f"{ATOM_NS}link[@rel='alternate']")
        if link_el is None:
            link_el = entry.find(f"{ATOM_NS}link")
        permalink = link_el.get("href", "") if link_el is not None else ""

        # Author
        author_el = entry.find(f"{ATOM_NS}author")
        author = ""
        if author_el is not None:
            author = author_el.findtext(f"{ATOM_NS}name", "")
            # Remove /u/ prefix
            author = author.replace("/u/", "")

        # Content (HTML)
        content_html = entry.findtext(f"{ATOM_NS}content", "")
        content_text = strip_html(content_html)[:500] if content_html else ""
        score = extract_score_from_content(content_html)

        # Parse timestamp to epoch
        created_utc = 0
        if updated:
            try:
                from datetime import datetime, timezone
                # Parse ISO 8601 format: 2026-07-03T05:14:09+00:00
                dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                created_utc = dt.timestamp()
            except Exception:
                created_utc = time.time()

        posts.append({
            "subreddit": sub,
            "id": post_id,
            "title": title,
            "selftext": content_text,
            "created_utc": created_utc,
            "author": author,
            "score": score,
            "permalink": permalink,
        })

    return posts


def fetch_and_send(sub):
    """
    Fetches real Reddit data and pushes to Kafka.
    Falls back through: PRAW → RSS Feed → Simulation
    """
    # --- Priority 1: PRAW (if credentials are configured) ---
    if USE_PRAW:
        try:
            print(f"[{sub}] Using PRAW (Official Reddit API)...")
            for submission in reddit.subreddit(sub).stream.submissions(pause_after=-1):
                if submission is None:
                    break
                payload = {
                    "subreddit": sub,
                    "id": submission.id,
                    "title": submission.title,
                    "created_utc": submission.created_utc,
                    "author": str(submission.author),
                    "score": submission.score,
                }
                producer.send("reddit_raw", value=payload)
                print(f"[{sub}] Ingested real post (PRAW): {submission.id}")
            return
        except Exception as e:
            print(f"[{sub}] PRAW failed ({e}), falling back to RSS feed...")

    # --- Priority 2: Reddit RSS Feed (no API key needed) ---
    try:
        print(f"[{sub}] Using Reddit RSS feed (no API key needed)...")
        posts = fetch_reddit_rss(sub)
        if not posts:
            raise Exception("No posts returned from RSS feed")

        for post in posts:
            producer.send("reddit_raw", value=post)
            print(f"[{sub}] Ingested real post (RSS): {post['id']} — {post['title'][:60]}")

        print(f"[{sub}] ✓ Successfully ingested {len(posts)} real posts from Reddit RSS")

        # Continue polling for new posts at intervals
        seen_ids = {p["id"] for p in posts}
        consecutive_empty = 0
        while True:
            # Poll every 60-90 seconds (Reddit strict rate limit for unauth)
            time.sleep(random.uniform(60.0, 90.0))
            try:
                new_posts = fetch_reddit_rss(sub)
                new_count = 0
                for post in new_posts:
                    if post["id"] not in seen_ids:
                        seen_ids.add(post["id"])
                        producer.send("reddit_raw", value=post)
                        new_count += 1
                        print(f"[{sub}] Ingested new real post (RSS): {post['id']} — {post['title'][:60]}")
                if new_count > 0:
                    print(f"[{sub}] Polled {new_count} new posts")
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1
                    if consecutive_empty % 10 == 0:
                        print(f"[{sub}] No new posts in {consecutive_empty} polls (normal for slow subreddits)")
                # Cap the seen_ids set to prevent memory leak
                if len(seen_ids) > 5000:
                    seen_ids = set(list(seen_ids)[-2500:])
            except Exception as poll_err:
                print(f"[{sub}] Poll error: {poll_err}, retrying in 30s...")
                time.sleep(30)
        return
    except Exception as e:
        print(f"[{sub}] Reddit RSS feed failed ({e}). Falling back to simulation...")

    # --- Priority 3: Simulation (absolute last resort — no internet) ---
    print(f"[{sub}] WARNING: Using simulated data. No real Reddit data available.")
    authors = ["data_ninja", "spark_hero", "kafka_master", "bigdata_guy", "python_coder", "tech_dev"]
    titles = [
        "Distributed computing with Apache Spark",
        "Building streaming pipeline with Kafka",
        "Data quality assurance in Hadoop HDFS",
        "Observability metrics for pipeline governance",
        "Linear regression on quality scorecard data",
        "Scaling microservices on Docker network"
    ]
    while True:
        payload = {
            "subreddit": sub,
            "id": f"sim_{sub}_{random.randint(100000, 999999)}",
            "title": random.choice(titles),
            "created_utc": time.time(),
            "author": random.choice(authors),
            "score": random.randint(1, 1000)
        }
        producer.send("reddit_raw", value=payload)
        print(f"[{sub}] Ingested simulated post: {payload['id']}")
        time.sleep(random.uniform(0.2, 1.5))


def main():
    # Stagger thread starts: launch each subreddit 3 seconds apart
    # to avoid simultaneous RSS requests triggering Reddit 429 rate limits
    threads = []
    for i, sub in enumerate(SUBREDDITS):
        t = threading.Thread(target=fetch_and_send, args=(sub,), daemon=True)
        t.start()
        threads.append(t)
        if i < len(SUBREDDITS) - 1:
            print(f"[MAIN] Started {sub}, waiting 12s before next subreddit...")
            time.sleep(12.0)

    # Wait for all threads (they run indefinitely in polling mode)
    for t in threads:
        t.join()
    producer.flush()

if __name__ == "__main__":
    main()
