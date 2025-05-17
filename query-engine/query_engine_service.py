import asyncio
import os
import re
import socket
import struct
from asyncio import StreamReader, StreamWriter
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError

import uvloop
from minio import Minio

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.serialization import msgpack_deserialization, msgpack_serialization
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.util.aio_task_scheduler import AIOTaskScheduler


KAFKA_URL: str = os.getenv('KAFKA_URL', "localhost:9092")
MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
QUERY_ENGINE_PORT: int = int(os.getenv('QUERY_ENGINE_PORT', 7000))
QUERY_ENGINE_TOPIC: str = "styx-query-engine"
SNAPSHOT_BUCKET_NAME: str = os.getenv('SNAPSHOT_BUCKET_NAME', "styx-snapshots")


class QueryEngineService(object):
    def __init__(self):
        self.minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        self.aio_task_scheduler = AIOTaskScheduler()
        self.kafka_query_consumer: AIOKafkaConsumer | None = None
        self.kafka_query_result_producer: AIOKafkaProducer | None = None
        self.kafka_producer_init_task: asyncio.Task = ...
        self.kafka_consumer_init_task: asyncio.Task = ...
        self.kafka_consumer_task: asyncio.Task = ...
        self.producer_ready = asyncio.Event()
        self.kafka_ready = asyncio.Event()

        self.networking = NetworkingManager(QUERY_ENGINE_PORT, mode=MessagingMode.QE_COR)
        self.qe_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.qe_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                  struct.pack('ii', 1, 0))  # Enable LINGER, timeout 0
        self.qe_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.qe_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.qe_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.qe_socket.bind(('0.0.0.0', QUERY_ENGINE_PORT))
        self.qe_socket.setblocking(False)

    async def query_engine_controller(self, data: bytes):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            case MessageType.SendExecutionGraph:
                message = self.networking.decode_message(data)
                logging.warning(f"Query engine received execution graph: {message[0]}")
            case MessageType.SnapID:
                snapshot_id = self.networking.decode_message(data)[0]
                matching_keys = []
                prefix = "data/"
                pattern = re.compile(rf"^{re.escape(prefix)}.*/{snapshot_id}\.bin$")
                for obj in self.minio_client.list_objects(SNAPSHOT_BUCKET_NAME, prefix=prefix, recursive=True):
                    if pattern.match(obj.object_name):
                        matching_keys.append(obj.object_name)
                logging.warning(f"Query engine received snapshot: {snapshot_id}. Matching keys: {matching_keys}")

    async def start_tcp_service(self):
        async def request_handler(reader: StreamReader, writer: StreamWriter):
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack('>Q', data)
                    message = await reader.readexactly(size)
                    self.aio_task_scheduler.create_task(self.query_engine_controller(message))
            except asyncio.IncompleteReadError as e:
                logging.info(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                logging.info("Closing the connection")
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(request_handler, sock=self.qe_socket, limit=2**32)
        async with server:
            await server.serve_forever()

    async def start_kafka_producer(self):
        self.kafka_query_result_producer = AIOKafkaProducer(bootstrap_servers=[KAFKA_URL],
                                                            client_id="QueryEngineProducer",
                                                            value_serializer=msgpack_serialization,
                                                            enable_idempotence=True)
        while True:
            try:
                await self.kafka_query_result_producer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break
        egress_topic = f"{QUERY_ENGINE_TOPIC}--OUT"
        topic = None
        while topic is None:
            topic = set(await self.kafka_query_result_producer.partitions_for(egress_topic))
            logging.warning(
                f"Awaiting topic {egress_topic} to be created by the Styx coordinator")
            await asyncio.sleep(5)
        logging.warning(f"Topic {egress_topic} created")
        self.producer_ready.set()

    async def start_kafka_consumer(self):
        while True:
            try:
                await asyncio.wait_for(self.producer_ready.wait(), timeout=1.0)
                break
            except asyncio.TimeoutError:
                await asyncio.sleep(5)
        self.kafka_query_consumer = AIOKafkaConsumer(auto_offset_reset='earliest',
                                                     bootstrap_servers=[KAFKA_URL],
                                                     client_id="QueryEngineConsumer",
                                                     value_deserializer=msgpack_deserialization)
        while True:
            try:
                await self.kafka_query_consumer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break
        ingress_topic = QUERY_ENGINE_TOPIC
        topics = []
        while ingress_topic not in topics:
            topics = set(await self.kafka_query_consumer.topics())
            logging.warning(
                f"Awaiting topic {ingress_topic} to be created by the Styx coordinator, current topics: {topics}")
            await asyncio.sleep(5)
        logging.warning(f"Topic {ingress_topic} created")
        self.kafka_query_consumer.subscribe(topics=[ingress_topic])
        logging.warning(f"Query engine subscribed to {ingress_topic}")
        self.kafka_ready.set()

    async def handle_client_query(self, kafka_message):
        try:
            logging.warning(f"Received query {kafka_message.value} from client")
            await self.kafka_query_result_producer.send_and_wait(f"{QUERY_ENGINE_TOPIC}--OUT",
                                                                 key=kafka_message.key,
                                                                 value="Res rows")
        except Exception as e:
            logging.warning(f"Error decoding query: {e}")

    async def consume_queries(self):
        while True:
            try:
                await asyncio.wait_for(self.kafka_ready.wait(), timeout=1.0)
                break
            except asyncio.TimeoutError:
                await asyncio.sleep(5)
        logging.warning("Starting kafka consumer")
        while True:
            try:
                try:
                    msg = await asyncio.wait_for(self.kafka_query_consumer.getone(), timeout=0.5)
                    await self.handle_client_query(msg)
                except asyncio.TimeoutError:
                    await asyncio.sleep(1)
                except Exception as e:
                    logging.error(f"Unexpected error: {e}")
            except KafkaError as e:
                logging.error(f"Kafka error: {e}")
            finally:
                await self.kafka_query_consumer.stop()

    def start_networking_tasks(self):
        self.networking.start_networking_tasks()

    async def main(self):
        self.start_networking_tasks()
        logging.info("Started networking tasks")
        self.kafka_producer_init_task = asyncio.create_task(self.start_kafka_producer())
        self.kafka_consumer_init_task = asyncio.create_task(self.start_kafka_consumer())
        # self.kafka_consumer_task = asyncio.create_task(self.consume_queries())
        await self.start_tcp_service()


if __name__ == '__main__':
    query_engine_service = QueryEngineService()
    uvloop.run(query_engine_service.main())
