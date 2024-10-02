import asyncio
import sys

from aiokafka import AIOKafkaConsumer
import pandas as pd

import uvloop
from styx.common.serialization import msgpack_deserialization

from client import g

SAVE_DIR: str = sys.argv[1]


def all_egress_topics_created(topics: set[str], egress_topic_names: list[str]):
    for topic in egress_topic_names:
        if topic not in topics:
            return False
    return True


async def consume():

    egress_topic_names: list[str] = g.get_egress_topic_names()

    records = []
    consumer = AIOKafkaConsumer(
        'styx-egress',
        auto_offset_reset='earliest',
        value_deserializer=msgpack_deserialization,
        bootstrap_servers='localhost:9092')
    await consumer.start()
    topics = []
    # Ensure topic is created by the producer (and not auto-created by this
    # consumer). This is important because it is the producer who holds the
    # information regarding the required partitions.
    while not all_egress_topics_created(topics, egress_topic_names):
        topics = set(await consumer.topics())
        print(f"Awaiting topics {egress_topic_names} to be created by the Styx coordinator, current topics: {topics}")
        await asyncio.sleep(5)
    print(f"Topics {egress_topic_names} has been created.")
    consumer.subscribe(topics=egress_topic_names)
    print(f"Consumer subscribed to topics {egress_topic_names}.")
    try:
        # Consume messages
        while True:
            data = await consumer.getmany(timeout_ms=10_000)
            if not data:
                break
            for messages in data.values():
                for msg in messages:
                    # print("consumed: ", msg.key, msg.value, msg.timestamp)
                    records.append((msg.key, msg.value, msg.timestamp))
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()
        pd.DataFrame.from_records(records,
                                  columns=['request_id', 'response', 'timestamp']).to_csv(f'{SAVE_DIR}/output.csv',
                                                                                          index=False)

uvloop.run(consume())
