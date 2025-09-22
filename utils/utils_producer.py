"""
utils_producer.py - common functions used by producers.

Producers send messages to a Kafka topic.
This version makes topic creation **idempotent** (never delete/recreate on startup),
which avoids the "marked for deletion" error loop.
"""

#####################################
# Imports
#####################################

import os
import sys
import time
from typing import Callable, Optional, Any

from dotenv import load_dotenv
from kafka import KafkaProducer, errors
from kafka.admin import KafkaAdminClient, NewTopic

from utils.utils_logger import logger


#####################################
# Defaults
#####################################

DEFAULT_KAFKA_BROKER_ADDRESS = "localhost:9092"


#####################################
# Helpers
#####################################

def get_kafka_broker_address() -> str:
    """
    Resolve the Kafka broker address the same way as the consumer.
    """
    try:
        import utils.utils_config as config
        broker_address = config.get_kafka_broker_address()
    except Exception:
        broker_address = os.getenv("KAFKA_BROKER_ADDRESS", "127.0.0.1:9092")
    logger.info(f"Kafka broker address: {broker_address}")
    return broker_address


#####################################
# Kafka readiness
#####################################

def check_kafka_service_is_ready() -> bool:
    """
    Check if Kafka is ready by connecting to the broker and fetching metadata.
    """
    kafka_broker = get_kafka_broker_address()
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
        cluster_info: dict = admin_client.describe_cluster()
        logger.info(f"Kafka is ready. Brokers: {cluster_info}")
        admin_client.close()
        return True
    except errors.KafkaError as e:
        logger.error(f"Error checking Kafka: {e}")
        return False


def verify_services(strict: bool = False) -> bool:
    """
    Check Kafka readiness. If strict=False, do not exit when unavailable.
    """
    ready = check_kafka_service_is_ready()
    if ready:
        return True

    msg = "Kafka broker not available; continuing without Kafka."
    if strict:
        logger.error("Kafka broker is not ready. Please check your Kafka setup. Exiting...")
        sys.exit(2)
    else:
        logger.warning(msg)
        return False


#####################################
# Producer construction
#####################################

def create_kafka_producer(
    value_serializer: Optional[Callable[[Any], bytes]] = None,
) -> Optional[KafkaProducer]:
    """
    Create and return a Kafka producer instance.
    """
    kafka_broker = get_kafka_broker_address()

    if value_serializer is None:
        def default_value_serializer(x: str) -> bytes:
            return x.encode("utf-8")
        value_serializer = default_value_serializer

    try:
        logger.info(f"Connecting to Kafka broker at {kafka_broker}...")
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=value_serializer,
        )
        logger.info("Kafka producer successfully created.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


#####################################
# Topic management (idempotent)
#####################################

def _topic_exists(admin: KafkaAdminClient, topic_name: str) -> bool:
    try:
        return topic_name in set(admin.list_topics())
    except Exception:
        # If listing fails, assume it doesn't exist to avoid false positives
        return False


def create_kafka_topic(topic_name: str, group_id: Optional[str] = None) -> None:
    """
    Idempotent topic creation:
      - If topic exists, log and return.
      - If not, create it.
    Avoids delete/recreate which triggers 'marked for deletion' races.
    """
    kafka_broker = get_kafka_broker_address()
    admin_client = None

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

        if _topic_exists(admin_client, topic_name):
            logger.info(f"Kafka topic '{topic_name}' is ready (exists).")
            return

        new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        logger.info(f"Kafka topic '{topic_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error ensuring topic '{topic_name}': {e}")
        # Do NOT exit here; let callers decide. This keeps the app resilient.
    finally:
        if admin_client is not None:
            try:
                admin_client.close()
            except Exception:
                pass


def clear_kafka_topic(topic_name: str, group_id: Optional[str] = None) -> None:
    """
    (Optional utility) Delete and recreate a topic on purpose.
    Not used by default startup. Use only when you explicitly want to reset a topic.
    """
    kafka_broker = get_kafka_broker_address()
    admin_client = None

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
        logger.info(f"Clearing topic '{topic_name}' by deleting and recreating it.")

        if _topic_exists(admin_client, topic_name):
            admin_client.delete_topics([topic_name])
            logger.info(f"Requested delete for '{topic_name}'. Waiting for deletion...")
            # Small wait loop for deletion to propagate
            deadline = time.time() + 10
            while time.time() < deadline and _topic_exists(admin_client, topic_name):
                time.sleep(0.25)

        new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        logger.info(f"Recreated topic '{topic_name}' successfully.")
    except Exception as e:
        logger.error(f"Error clearing topic '{topic_name}': {e}")
    finally:
        if admin_client is not None:
            try:
                admin_client.close()
            except Exception:
                pass


def is_topic_available(topic: str, timeout_ms: int = 5000) -> bool:
    """
    Return True if `topic` exists on the Kafka cluster defined by KAFKA_BROKER_ADDRESS.
    """
    try:
        kafka_broker = get_kafka_broker_address()
        admin = KafkaAdminClient(
            bootstrap_servers=kafka_broker,
            client_id="topic_check",
            request_timeout_ms=timeout_ms,
        )
        try:
            topics = set(admin.list_topics())
        finally:
            try:
                admin.close()
            except Exception:
                pass

        exists = topic in topics
        if exists:
            logger.info(f"is_topic_available: topic '{topic}' found on {kafka_broker}.")
        else:
            logger.warning(f"is_topic_available: topic '{topic}' NOT found on {kafka_broker}.")
        return exists

    except Exception as e:
        logger.error(f"is_topic_available: failed to check topic '{topic}': {e}")
        return False


#####################################
# Main (manual test)
#####################################

def main():
    """
    Manual test entry point.
    """
    logger.info("Starting utils_producer.py script...")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()

    if not check_kafka_service_is_ready():
        logger.error("Kafka is not ready. Check .env file and ensure Kafka is running.")
        sys.exit(2)

    logger.info("All services are ready. Proceed with producer setup.")
    create_kafka_topic("test_topic", "default_group")


if __name__ == "__main__":
    main()
