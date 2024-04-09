import logging
import os
import uuid
from typing import List

from dotenv import load_dotenv
from fastapi import FastAPI

from faker import Faker

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer

from commands import CreatePeopleCommand
from models import Person

logger = logging.getLogger(__name__)

load_dotenv(verbose=True)

app = FastAPI()


@app.on_event("startup")
async def on_startup():
    client = KafkaAdminClient(bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"))

    topics = [
        NewTopic(
            name=os.environ["TOPICS_PEOPLE_ADVANCED_NAME"],
            num_partitions=int(
                os.environ["TOPICS_PEOPLE_ADVANCED_PARTITIONS"]),
            replication_factor=int(
                os.environ["TOPICS_PEOPLE_ADVANCED_REPLICAS"]),
        ),
    ]
    for topic in topics:
        try:
            client.create_topics([topic])
        except TopicAlreadyExistsError:
            logger.warning(f"Topic already exists")

    client.close()


@app.get("/")
async def hello_world():
    return {"message": "Hello World!"}


def make_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
        linger_ms=int(os.getenv("TOPICS_PEOPLE_ADVANCED_LINGER_MS")),
        retries=int(os.getenv("TOPICS_PEOPLE_ADVANCED_RETRIES")),
        max_in_flight_requests_per_connection=int(
            os.getenv("TOPICS_PEOPLE_ADVANCED_INFLIGHTS_REQS")
        ),
        acks=os.getenv("TOPICS_PEOPLE_ADVANCED_ACKS"),
    )


class SuccessHandler:
    def __init__(self, person: Person):
        self.person = person

    def __call__(self, record_metadata):
        logger.info(
            f"Successfully produced record Person "
            f"to topic {record_metadata.topic} "
            f"in partition {record_metadata.partition} "
            f"at offset {record_metadata.offset}"
        )

class ErrorHandler:
    def __init__(self, person: Person):
        self.person = person

    def __call__(self, exception):
        logger.error(f"Failed to produce record Person: {self.person}", exc_info=exception)


@app.post("/api/people", status_code=201, response_model=List[Person])
async def create_people(cmd: CreatePeopleCommand):
    people: List[Person] = []

    faker = Faker()

    producer = make_producer()

    for _ in range(cmd.count):
        person = Person(
            id=str(uuid.uuid4()),
            name=faker.name(),
            title=faker.job().title()
        )
        people.append(person)

        producer.send(
            topic=os.getenv("TOPICS_PEOPLE_ADVANCED_NAME"),
            key=person.title.lower().replace(r"s+", "-").encode("utf-8"),
            value=person.json().encode("utf-8")
        ).add_callback(
            SuccessHandler(person)
        ).add_errback(
            ErrorHandler(person)
        )

    producer.flush()

    return people
