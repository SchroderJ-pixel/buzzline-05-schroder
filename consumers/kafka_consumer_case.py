"""
kafka_consumer_case.py

Consume JSON messages from a Kafka topic and insert the processed
messages into a SQLite database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions live in consumers/sqlite_consumer_case.py.
Environment variables are read via utils/utils_config.
"""

#####################################
# Imports
#####################################

# Standard library
import json
import os
import pathlib
import sys
import time
from typing import Dict, Optional

# External
from kafka import KafkaConsumer

# Local utilities
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

# Make sure parent directory is importable (for consumers.* imports)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.sqlite_consumer_case import init_db, insert_message  # noqa: E402


#####################################
# Message Processing
#####################################

REQUIRED_FIELDS = [
    "message",
    "author",
    "timestamp",
    "category",
    "sentiment",
    "keyword_mentioned",
    "message_length",
]


def process_message(msg: Dict) -> Optional[Dict]:
    """
    Validate / normalize an incoming message dict.

    Returns:
        - dict ready for insert_message()
        - None to skip invalid records
    """
    if not isinstance(msg, dict):
        logger.warning(f"Skipping non-dict message: {msg!r}")
        return None

    # Ensure required fields exist
    for key in REQUIRED_FIELDS:
        if key not in msg:
            logger.warning(f"Skipping message missing '{key}': {msg}")
            return None

    # Light normalization / type safety (best-effort, non-fatal)
    try:
        # Ensure sentiment is a float
        if not isinstance(msg["sentiment"], (int, float)):
            msg["sentiment"] = float(msg["sentiment"])

        # Ensure message_length is an int
        if not isinstance(msg["message_length"], int):
            msg["message_length"] = int(msg["message_length"])

        # Coerce simple string fields
        for s in ["message", "author", "category", "keyword_mentioned", "timestamp"]:
            if not isinstance(msg[s], str):
                msg[s] = str(msg[s])

    except Exception as e:
        logger.warning(f"Normalization issue, skipping message {msg}: {e}")
        return None

    return msg


#####################################
# Core Consumer Loop
#####################################

def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sql_path: pathlib.Path,
    interval_secs: int,
):
    """
    Consume new messages from a Kafka topic and process them.
    Each message is expected to be JSON-formatted.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   topic={topic}")
    logger.info(f"   kafka_url={kafka_url}")
    logger.info(f"   group={group}")
    logger.info(f"   sql_path={sql_path}")
    logger.info(f"   interval_secs={interval_secs}")

    # Step 1. Verify Kafka services.
    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    # Step 2. Create a Kafka consumer.
    logger.info("Step 2. Create a Kafka consumer.")
    consumer = None
    try:
        consumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
        logger.info("Kafka consumer created successfully.")
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(12)

    # Step 3. Verify topic exists.
    logger.info("Step 3. Verify topic exists.")
    try:
        if not is_topic_available(topic):
            raise RuntimeError(
                f"Topic '{topic}' not found. Start the producer or create the topic."
            )
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(
            f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}"
        )
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass
        sys.exit(13)

    # Step 4. Process messages (block and poll)
    logger.info("Step 4. Process messages.")
    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(14)

    try:
        while True:
            # Poll returns {TopicPartition: [ConsumerRecord, ...]}
            records = consumer.poll(timeout_ms=1000)

            if not records:
                # Small nap to keep loop gentle
                time.sleep(0.1)
                continue

            for tp, msgs in records.items():
                for msg in msgs:
                    try:
                        processed = process_message(msg.value)
                        if processed:
                            insert_message(processed, sql_path)
                    except Exception as inner_e:
                        logger.error(f"Error handling message on {tp}: {inner_e}")

            # If manual commits are desired, uncomment:
            # consumer.commit()

            # Optional pacing between polls (uses your env setting)
            if interval_secs and interval_secs > 0:
                time.sleep(min(interval_secs, 1))  # don’t stall too long in consumer loop

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise
    finally:
        try:
            consumer.close()
            logger.info("Kafka consumer closed.")
        except Exception:
            pass


#####################################
# Main
#####################################

def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
        logger.info(f"ENV: topic={topic}, kafka_url={kafka_url}, group_id={group_id}")
        logger.info(f"ENV: sqlite_path={sqlite_path}, interval_secs={interval_secs}")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    # NOTE: Comment this block if you want to append instead of wiping each run.
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_kafka(
            topic=topic,
            kafka_url=kafka_url,
            group=group_id,
            sql_path=sqlite_path,
            interval_secs=interval_secs,
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")


#####################################
# Entrypoint
#####################################

if __name__ == "__main__":
    main()
