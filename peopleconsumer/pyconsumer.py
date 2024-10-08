import json
import logging
import os

from dotenv import load_dotenv

from kafka.consumer import KafkaConsumer
from kafka import OffsetAndMetadata, TopicPartition


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path="./.env", verbose=True)


def people_key_deserializer(key):
    return key.decode("utf-8")


def people_value_deserializer(value):
    return json.loads(value.decode("utf-8"))


def main():
    logger.info(f"""
        Starting consumer
        for topic {os.getenv("TOPICS_PEOPLE_ADVANCED_NAME")}
    """)

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
        group_id=os.getenv("CONSUMER_GROUP"),
        key_deserializer=people_key_deserializer,
        value_deserializer=people_value_deserializer,
        enable_auto_commit=False,
    )

    consumer.subscribe(os.getenv("TOPICS_PEOPLE_ADVANCED_NAME"))

    for record in consumer:
        logger.info(
            f"""Consumed person {record.value}
            with key {record.key}
            from partition {record.topic}
            at offset {record.offset}
            """
        )

        topic_partition = TopicPartition(record.topic, record.partition)
        offset_metadata = OffsetAndMetadata(record.offset + 1, record.timestamp)

        consumer.commit(
            {
                topic_partition: offset_metadata
            }
        )


if __name__ == '__main__':
    main()
