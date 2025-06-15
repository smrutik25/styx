import asyncio
import sys

from aiokafka import AIOKafkaConsumer
import pandas as pd

import uvloop
import ast
from styx.common.serialization import msgpack_deserialization

from pure_kafka_demo import g


def all_egress_topics_created(topics: set[str], egress_topic_names: list[str]):
    for topic in egress_topic_names:
        if topic not in topics:
            return False
    return True


async def consume(save_dir):
    egress_topic_names: list[str] = g.get_egress_topic_names()

    records = []
    query_records = []
    freshness_records = []
    consumer = AIOKafkaConsumer(
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
    try:
        consumer.subscribe(topics=egress_topic_names)
        print(f"Consumer subscribed to topics {egress_topic_names}.")
        while True:
            data = await consumer.getmany(timeout_ms=10_000)
            if not data:
                break
            for messages in data.values():
                for msg in messages:
                    records.append((msg.key, msg.value, msg.timestamp))

        consumer.subscribe(topics=['styx-query-engine--OUT'])
        freshness_queries = pd.read_csv(f"{save_dir}/freshness_queries.csv")["request_id"].tolist()
        freshness_queries = [ast.literal_eval(s) for s in freshness_queries]
        while True:
            data = await consumer.getmany(timeout_ms=10_000)
            if not data:
                break
            for messages in data.values():
                for msg in messages:
                    if msg.key in freshness_queries:
                        freshness_records.append((msg.key, msg.value, msg.timestamp))
                    else:
                        query_records.append((msg.key, len(msg.value), msg.timestamp))
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()
        pd.DataFrame.from_records(records,
                                  columns=['request_id', 'response', 'timestamp']).sort_values(by="timestamp").to_csv(
            f'{save_dir}/output.csv', index=False)
        pd.DataFrame.from_records(query_records,
                                  columns=['request_id', 'response_size', 'timestamp']).sort_values(by="timestamp").to_csv(
            f'{save_dir}/query_output.csv', index=False)

        pd.DataFrame.from_records(freshness_records,
                                  columns=['request_id', 'response', 'timestamp']).sort_values(by="timestamp").to_csv(
            f'{save_dir}/freshness_output.csv', index=False)


def main(save_dir=None):
    if save_dir is None:
        print("Save directory for the results not provided.")
        exit(1)

    uvloop.run(consume(save_dir))


if __name__ == "__main__":
    main(sys.argv[1])
