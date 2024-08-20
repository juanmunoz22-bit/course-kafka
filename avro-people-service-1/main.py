import logging
import os
from typing import List

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv
from faker import Faker
from fastapi import FastAPI

import schemas
from commands import CreatePeopleCommand
from models import Person

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(verbose=True)


app = FastAPI()


@app.on_event("startup")
async def startup_event():
    client = AdminClient({"bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS")})
    topic = NewTopic(
        os.getenv('TOPICS_PEOPLE_AVRO_NAME'),
        num_partitions=int(os.getenv("TOPICS_PEOPLE_AVRO_PARTITIONS")),
        replication_factor=int(os.getenv("TOPICS_PEOPLE_REPLICAS")),
    )
    try:
        futures = client.create_topics([topic])
        for topic_name, future in futures.items():
            future.result()
            logger.info(f"Topic {topic_name} created")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def make_producer() -> SerializingProducer:
    # make a SchemaRegistryClient
    schema_reg_client = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL")})

    # Create AvroSerializer
    avro_serializer = AvroSerializer(
        schema_reg_client,
        schemas.person_value_v2,
        lambda person, ctx: person.dict(),
    )
    # Create and return SerializingProducer
    return SerializingProducer(
        {
            "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
            "linger.ms": 300,
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 1,
            "acks": "all",
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "partitioner": "murmur2_random",
        }
    )


class ProducerCallback:
    def __init__(self, person):
        self.person = person

    def __call__(self, err, msg):
        if err is not None:
            logger.error(f"Failed to produce {self.person}: {err}")
        else:
            logger.info(f"""
                        Succeeded in producing {self.person} 
                        to {msg.partition()}
                        at offset {msg.offset()}
                        """)

@app.post("/api/people", status_code=201, response_model=List[Person])
async def create_people(cmd: CreatePeopleCommand):
    people: List[Person] = []
    faker = Faker()
    producer = make_producer()

    for _ in range(cmd.count):
        person = Person(
            first_name=faker.first_name(),
            last_name=faker.last_name(),
            title=faker.job().title()
        )
        people.append(person)
        producer.produce(
            topic=os.getenv("TOPICS_PEOPLE_AVRO_NAME"),
            key=person.title.lower().replace(r"s+", "-"),
            value=person,
            on_delivery=ProducerCallback(person)
        )

    producer.flush()
    return people
