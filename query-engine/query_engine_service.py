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
                logging.warning(f"COROUTINE QUERY CONTROLLER: Query engine received execution graph: {message[0]}")
            case MessageType.SnapID:
                snapshot_id = self.networking.decode_message(data)[0]
                matching_keys = []
                prefix = "data/"
                pattern = re.compile(rf"^{re.escape(prefix)}.*/{snapshot_id}\.bin$")
                for obj in self.minio_client.list_objects(SNAPSHOT_BUCKET_NAME, prefix=prefix, recursive=True):
                    if pattern.match(obj.object_name):
                        matching_keys.append(obj.object_name)
                logging.warning(f"COROUTINE QUERY CONTROLLER: Query engine received snapshot: {snapshot_id}. Matching keys: {matching_keys}")

    async def start_tcp_service(self):
        logging.warning("COROUTINE TCP SERVER START: Inside TCP service method")

        async def request_handler(reader: StreamReader, writer: StreamWriter):
            try:
                while True:
                    logging.warning("COROUTINE TCP SERVER PROCESS: Inside request handler while true loop")
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack('>Q', data)
                    message = await reader.readexactly(size)
                    self.aio_task_scheduler.create_task(self.query_engine_controller(message))
            except asyncio.IncompleteReadError as e:
                logging.warning(f"COROUTINE TCP SERVER PROCESS: Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError as e:
                logging.warning(f"COROUTINE TCP SERVER PROCESS: Client cancelled: {e}")
                pass
            finally:
                logging.warning("COROUTINE TCP SERVER PROCESS: Closing the connection")
                writer.close()
                await writer.wait_closed()
                logging.warning("COROUTINE TCP SERVER PROCESS: Completed/Exiting")

        server = await asyncio.start_server(request_handler, sock=self.qe_socket, limit=2**32)
        logging.warning("COROUTINE TCP SERVER START: Server init and started")
        async with server:
            logging.warning("COROUTINE TCP SERVER START: Serving TCP server")
            await server.serve_forever()

    async def start_kafka_producer(self):
        logging.warning("COROUTINE PRODUCER INIT: Inside init kafka producer method")
        self.kafka_query_result_producer = AIOKafkaProducer(bootstrap_servers=[KAFKA_URL],
                                                            client_id="QueryEngineProducer",
                                                            value_serializer=msgpack_serialization,
                                                            enable_idempotence=True)
        while True:
            try:
                logging.warning("COROUTINE PRODUCER INIT: Starting kafka producer init")
                await self.kafka_query_result_producer.start()
            except KafkaConnectionError:
                logging.warning("COROUTINE PRODUCER INIT: Init kafka producer did not work, retrying after sleep")
                await asyncio.sleep(1)
                logging.warning("COROUTINE PRODUCER INIT: Kafka not up yet for producer init")
                continue
            break
        logging.warning("COROUTINE PRODUCER INIT: Kafka started and producer init successful")
        egress_topic = f"{QUERY_ENGINE_TOPIC}--OUT"
        topic = None
        while topic is None:
            topic = set(await self.kafka_query_result_producer.partitions_for(egress_topic))
            logging.warning(
                f"COROUTINE PRODUCER INIT: Awaiting topic {egress_topic} to be created by the Styx coordinator")
            await asyncio.sleep(5)
        logging.warning(f"COROUTINE PRODUCER INIT: Topic {egress_topic} created for kafka producer init")
        self.producer_ready.set()
        logging.warning(f"COROUTINE PRODUCER INIT: Completed/Exiting")

    async def start_kafka_consumer(self):
        logging.warning("COROUTINE CONSUMER INIT: Inside init kafka consumer method")
        while True:
            try:
                logging.warning("COROUTINE CONSUMER INIT: Starting kafka consumer")
                await asyncio.wait_for(self.producer_ready.wait(), timeout=1.0)
                break
            except asyncio.TimeoutError:
                logging.warning("COROUTINE CONSUMER INIT: Init kafka consumer did not work, retrying after sleep")
                await asyncio.sleep(5)
        self.kafka_query_consumer = AIOKafkaConsumer(auto_offset_reset='earliest',
                                                     bootstrap_servers=[KAFKA_URL],
                                                     client_id="QueryEngineConsumer",
                                                     value_deserializer=msgpack_deserialization)
        while True:
            try:
                logging.warning("COROUTINE CONSUMER INIT: Starting kafka consumer init")
                await self.kafka_query_consumer.start()
            except KafkaConnectionError:
                logging.warning("COROUTINE CONSUMER INIT: Init kafka consumer did not work, retrying after sleep")
                await asyncio.sleep(1)
                logging.info("COROUTINE CONSUMER INIT: Waiting for Kafka")
                continue
            break
        logging.warning("COROUTINE CONSUMER INIT: Consumer init successful")
        ingress_topic = QUERY_ENGINE_TOPIC
        topics = []
        while ingress_topic not in topics:
            logging.info("COROUTINE CONSUMER INIT: Kafka not up yet for producer init")
            topics = set(await self.kafka_query_consumer.topics())
            logging.warning(
                f"COROUTINE CONSUMER INIT: Awaiting topic {ingress_topic} to be created by the Styx coordinator, current topics: {topics}")
            await asyncio.sleep(5)
        logging.warning(f"COROUTINE CONSUMER INIT: Topic {ingress_topic} created for consumer init")
        self.kafka_query_consumer.subscribe(topics=[ingress_topic])
        logging.warning(f"COROUTINE CONSUMER INIT: Query engine subscribed to {ingress_topic} for consuming messages in consumer init")
        self.kafka_ready.set()
        logging.warning(f"COROUTINE CONSUMER INIT: Completed/Exiting")

    async def handle_client_query(self, kafka_message):
        try:
            logging.warning("COROUTINE HANDLE QUERY: Received a query from kafka, running handler")
            logging.warning(f"COROUTINE HANDLE QUERY: Received query {kafka_message.value} from client")
            await self.kafka_query_result_producer.send_and_wait(f"{QUERY_ENGINE_TOPIC}--OUT",
                                                                 key=kafka_message.key,
                                                                 value="Res rows")
            logging.warning("COROUTINE HANDLE QUERY: Sent query response to through kafka producer. Completed/Exiting.")
        except Exception as e:
            logging.warning(f"COROUTINE HANDLE QUERY: Error decoding query: {e}")

    async def consume_queries(self):
        logging.warning("COROUTINE KAFKA CONSUMER: Inside query consumer method")
        while True:
            logging.warning("COROUTINE KAFKA CONSUMER: Inside while true loop of query consumer check init complete")
            try:
                logging.warning("COROUTINE KAFKA CONSUMER: Checking if kafka init is done to start consuming")
                await asyncio.wait_for(self.kafka_ready.wait(), timeout=1.0)
                break
            except asyncio.TimeoutError:
                logging.warning("COROUTINE KAFKA CONSUMER: Kafka init not done, cannot consume so sleeping")
                await asyncio.sleep(5)
        logging.warning("COROUTINE KAFKA CONSUMER: Kafka consumer init done, start consuming messages")
        try:
            while True:
                try:
                    logging.warning("COROUTINE KAFKA CONSUMER: Trying to get one kafka message in consumer")
                    msg = await asyncio.wait_for(self.kafka_query_consumer.getone(), timeout=0.5)
                    await self.handle_client_query(msg)
                except asyncio.TimeoutError:
                    logging.warning("COROUTINE KAFKA CONSUMER: No kafka message, sleeping and switching to another task")
                    await asyncio.sleep(1)
                except Exception as e:
                    logging.error(f"COROUTINE KAFKA CONSUMER: Unexpected error in consuming single kafka message: {e}")
        finally:
            logging.warning("COROUTINE KAFKA CONSUMER: Inside finally of kafka consumer, stopping consumer")
            await self.kafka_query_consumer.stop()
            logging.warning("COROUTINE KAFKA CONSUMER: Completed/Exiting")

    def start_networking_tasks(self):
        logging.warning("COROUTINE NETWORKING TASKS: Inside networking tasks method")
        self.networking.start_networking_tasks()
        logging.warning("COROUTINE NETWORKING TASKS: Completed")

    async def main(self):
        self.start_networking_tasks()
        logging.warning("COROUTINE MAIN: Started networking tasks")
        self.kafka_producer_init_task = asyncio.create_task(self.start_kafka_producer())
        self.kafka_consumer_init_task = asyncio.create_task(self.start_kafka_consumer())
        self.kafka_consumer_task = asyncio.create_task(self.consume_queries())
        await self.start_tcp_service()


if __name__ == '__main__':
    query_engine_service = QueryEngineService()
    uvloop.run(query_engine_service.main())
