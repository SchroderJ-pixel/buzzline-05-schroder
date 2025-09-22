"""
consumers/kafka_consumer_schroder.py  (rename the file to match your run command)

Kafka consumer that reads JSON messages and stores a per-message
fitness insight into SQLite: one row per message (when fitness-related).

Insight per message:
- fitness_flag: 1 if the text mentions fitness concepts (keywords)
- fitness_tag: RUN / LIFT / CARDIO / STEPS / SWIM / BIKE / PROTEIN / MEAL / OTHER
- effort_score: 0-100 using sentiment + intensity words

Run:
    python -m consumers.kafka_consumer_schroder
"""

# ============== Imports ==============
import json
import os
import pathlib
import sys
import time
from typing import Dict, Optional

# external
from kafka import KafkaConsumer  # used indirectly via helper

# Local utils
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

# Ensure project root on path for relative imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ============== SQLite helpers (local to this file) ==============
import sqlite3


def _safe_json_deserializer(x: bytes):
    """Best-effort JSON loader. Returns None for empty/invalid payloads."""
    try:
        s = x.decode("utf-8", errors="ignore").strip()
        if not s:
            return None
        return json.loads(s)
    except Exception:
        snippet = (s[:120] + "...") if "s" in locals() and len(s) > 120 else (s if "s" in locals() else "")
        logger.warning(f"Skipping non-JSON message on topic: {snippet!r}")
        return None


def _connect(db_path: pathlib.Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(db_path))


def init_db(db_path: pathlib.Path) -> None:
    """Create fitness_events table if not exists."""
    with _connect(db_path) as con:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fitness_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                author TEXT NOT NULL,
                fitness_flag INTEGER NOT NULL,
                fitness_tag TEXT NOT NULL,
                effort_score INTEGER NOT NULL,
                message TEXT NOT NULL,
                raw_category TEXT,
                raw_keyword TEXT,
                raw_sentiment REAL,
                message_length INTEGER
            );
            """
        )
        con.commit()


def insert_event(db_path: pathlib.Path, row: Dict) -> None:
    with _connect(db_path) as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO fitness_events
            (ts, author, fitness_flag, fitness_tag, effort_score,
             message, raw_category, raw_keyword, raw_sentiment, message_length)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                row["timestamp"],
                row["author"],
                int(row["fitness_flag"]),
                row["fitness_tag"],
                int(row["effort_score"]),
                row["message"],
                row.get("category"),
                row.get("keyword_mentioned"),
                float(row.get("sentiment", 0.0)),
                int(row.get("message_length", 0)),
            ),
        )
        con.commit()


# ============== Fitness parsing ==============

FITNESS_KEYWORDS = {
    "RUN":     ["run", "ran", "jog", "jogged", "treadmill", "5k", "10k"],
    "LIFT":    ["lift", "lifting", "gym", "bench", "squat", "deadlift", "press", "hypertrophy"],
    "CARDIO":  ["cardio", "elliptical", "hit", "hiit", "spin", "row", "rowing"],
    "STEPS":   ["steps", "walk", "walked", "walking", "hike", "hiked", "hiking"],
    "SWIM":    ["swim", "swam", "pool", "laps"],
    "BIKE":    ["bike", "biked", "cycling", "cycle", "peloton"],
    "PROTEIN": ["protein", "shake", "whey"],
    "MEAL":    ["calorie", "calories", "meal prep", "meal-prep", "macro", "macros"],
}

INTENSITY_WORDS = [
    "hard", "heavy", "killer", "crushed", "intense", "burn", "sweat", "sweaty", "brutal", "easy", "light"
]

REQUIRED_FIELDS = [
    "message", "author", "timestamp", "category",
    "sentiment", "keyword_mentioned", "message_length",
]


def _first_bucket_hit(text_lc: str) -> Optional[str]:
    for tag, words in FITNESS_KEYWORDS.items():
        for w in words:
            if w in text_lc:
                return tag
    return None


def _effort_score(sentiment: float, text_lc: str) -> int:
    base = max(0.0, min(1.0, float(sentiment))) * 80.0
    has_intensity = any(w in text_lc for w in INTENSITY_WORDS)
    score = base + (10.0 if has_intensity else 0.0)
    return int(max(0, min(100, round(score))))


def derive_fitness_insight(msg: Dict) -> Optional[Dict]:
    for k in REQUIRED_FIELDS:
        if k not in msg:
            logger.debug(f"Skipping message missing {k}: {msg}")
            return None

    try:
        sentiment = float(msg["sentiment"])
    except Exception:
        sentiment = 0.0

    text = str(msg["message"])
    text_lc = text.lower()

    tag = _first_bucket_hit(text_lc)
    if not tag:
        return None

    return {
        "timestamp": str(msg["timestamp"]),
        "author": str(msg["author"]),
        "fitness_flag": True,
        "fitness_tag": tag,
        "effort_score": _effort_score(sentiment, text_lc),
        "message": text,
        "category": str(msg.get("category", "")),
        "keyword_mentioned": str(msg.get("keyword_mentioned", "")),
        "sentiment": sentiment,
        "message_length": int(msg.get("message_length", 0)),
    }


def process_message(raw: Dict) -> Optional[Dict]:
    if not isinstance(raw, dict):
        return None

    try:
        if not isinstance(raw.get("message_length", 0), int):
            raw["message_length"] = int(raw["message_length"])
    except Exception:
        raw["message_length"] = 0

    try:
        if not isinstance(raw.get("sentiment", 0.0), (int, float)):
            raw["sentiment"] = float(raw["sentiment"])
    except Exception:
        raw["sentiment"] = 0.0

    for s in ["message", "author", "category", "keyword_mentioned", "timestamp"]:
        if s in raw and not isinstance(raw[s], str):
            raw[s] = str(raw[s])

    return derive_fitness_insight(raw)


def consume(topic: str, group: str, db_path: pathlib.Path, interval_secs: int) -> None:
    logger.info("Verify Kafka services…")
    if not verify_services(strict=False):
        logger.warning("Kafka might be down; if so, start it or run a file-based approach.")
    logger.info("Kafka check complete.")

    logger.info(f"Ensure topic '{topic}' exists…")
    if not is_topic_available(topic):
        raise RuntimeError(f"Kafka topic '{topic}' not available. Start producer or create topic.")

    consumer = create_kafka_consumer(
        topic,
        group,
        value_deserializer_provided=_safe_json_deserializer,  # SAFE: handles empty/bad lines
    )

    logger.info("Begin polling… Ctrl+C to stop.")
    while True:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            time.sleep(0.05)
            continue

        for tp, msgs in records.items():
            for m in msgs:
                try:
                    if m.value is None:
                        # empty or invalid JSON -> ignore
                        continue
                    row = process_message(m.value)
                    if row:
                        insert_event(db_path, row)
                except Exception as e:
                    logger.error(f"Message handling error: {e}")

        if interval_secs and interval_secs > 0:
            time.sleep(min(interval_secs, 1))


def main():
    logger.info("Starting consumer_schroder (Fitness)")
    topic = config.get_kafka_topic()
    group_id = config.get_kafka_consumer_group_id()
    sqlite_path: pathlib.Path = config.get_sqlite_path()
    interval_secs: int = config.get_message_interval_seconds_as_int()

    init_db(sqlite_path)

    try:
        consume(topic=topic, group=group_id, db_path=sqlite_path, interval_secs=interval_secs)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    finally:
        logger.info("consumer_schroder shutting down.")


if __name__ == "__main__":
    main()
