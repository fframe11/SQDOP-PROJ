import os, json, concurrent.futures, subprocess
from kafka import KafkaProducer

# Determine whether to use PRAW (Reddit API) or fallback to snscrape CLI
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USE_PRAW = REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET

if USE_PRAW:
    import praw
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent="thesis-reddit-stream"
    )
else:
    # No API credentials – we'll invoke snscrape via subprocess
    reddit = None  # placeholder

try:
    # Try kafka:9092 first (inside Docker network)
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
        # Fallback to localhost:9092 (on host machine)
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

def fetch_and_send(sub):
    if USE_PRAW:
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
    else:
        # Use snscrape CLI to fetch recent submissions (limit 100) or fallback to simulation
        import time, random
        try:
            cmd = ["snscrape", "--jsonl", "reddit-search", f"subreddit:{sub}", "--limit", "100"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0 or not result.stdout.strip():
                raise Exception("snscrape failed or empty")
            for line in result.stdout.splitlines():
                try:
                    submission = json.loads(line)
                    payload = {
                        "subreddit": sub,
                        "id": submission.get("id"),
                        "title": submission.get("title"),
                        "created_utc": submission.get("created_utc"),
                        "author": submission.get("author"),
                        "score": submission.get("score"),
                    }
                    producer.send("reddit_raw", value=payload)
                except json.JSONDecodeError:
                    continue
        except Exception as e:
            print(f"snscrape failed for {sub} ({e}). Starting live real-time simulation instead...")
            # Live simulation fallback
            authors = ["data_ninja", "spark_hero", "kafka_master", "bigdata_guy", "python_coder", "tech_dev"]
            titles = [
                "Distributed computing with Apache Spark",
                "Building streaming pipeline with Kafka",
                "Data quality assurance in Hadoop HDFS",
                "Observability metrics for pipeline governance",
                "Linear regression on quality scorecard data",
                "Scaling microservices on Docker network"
            ]
            # Stream simulated posts indefinitely to test real-time flows
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(SUBREDDITS)) as pool:
        pool.map(fetch_and_send, SUBREDDITS)
    producer.flush()

if __name__ == "__main__":
    main()
