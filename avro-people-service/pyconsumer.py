import logging
import os

from dotenv import load_dotenv

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

import schemas
from models import Person

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

load_dotenv(verbose=True)


def make_consumer() -> DeserializingConsumer:
    schema_reg_client = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL")})

    avro_deserializer = AvroDeserializer(
        schema_reg_client,
        schemas.person_value_v2,
        lambda data, ctx: Person(**data),
    )

    return DeserializingConsumer(
        {
            "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
            "group.id": os.getenv("CONSUMER_GROUP"),
            "enable.auto.commit": "false",
        }
    )


def main():
    logger.info(
        f"""
        Starting consumer...
        for topic: {os.getenv("TOPICS_PEOPLE_AVRO_NAME")}
        """
    )

    consumer = make_consumer()
    consumer.subscribe([os.getenv("TOPICS_PEOPLE_AVRO_NAME")])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None:
            person = msg.value()
            logger.info(f"Consumed record with key {msg.key()} and value {person}")
            consumer.commit(msg)


if __name__ == "__main__":
    main()
