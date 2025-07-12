import asyncio
import os
import socket
import struct
import time
from asyncio import StreamReader, StreamWriter
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

import uvloop

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.serialization import msgpack_serialization, msgpack_deserialization
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from query_engine_handler import QueryEngineHandler


KAFKA_URL: str = os.getenv('KAFKA_URL', "localhost:9092")
QUERY_ENGINE_PORT: int = int(os.getenv('QUERY_ENGINE_PORT', 7000))
QUERY_ENGINE_TOPIC: str = "styx-query-engine"
MAX_CONCURRENCY: int = 10
STATEFLOW_FILE_PATH: str = f"{os.getenv('DATABASE_FILE_PATH', 'data')}/stateflow_graph.json"


class QueryEngineService(object):
    def __init__(self):
        self.aio_task_scheduler = AIOTaskScheduler()
        self.kafka_query_consumer: AIOKafkaConsumer | None = None
        self.kafka_query_result_producer: AIOKafkaProducer | None = None
        self.kafka_producer_init_task: asyncio.Task = ...
        self.kafka_consumer_init_task: asyncio.Task = ...
        self.kafka_consumer_task: asyncio.Task = ...
        self.producer_ready = asyncio.Event()
        self.kafka_ready = asyncio.Event()
        self.duckdb_ready = asyncio.Event()

        self.networking = NetworkingManager(QUERY_ENGINE_PORT, mode=MessagingMode.QE_COR)
        self.qe_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.qe_socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                  struct.pack('ii', 1, 0))  # Enable LINGER, timeout 0
        self.qe_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.qe_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.qe_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        self.qe_socket.bind(('0.0.0.0', QUERY_ENGINE_PORT))
        self.qe_socket.setblocking(False)
        self.qe_handler = QueryEngineHandler()

        self.query_tasks = set()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async def query_engine_controller(self, data: bytes):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            case MessageType.SendExecutionGraph:
                message = self.networking.decode_message(data)
                logging.warning(f"Query engine received execution graph")
                with open(STATEFLOW_FILE_PATH, "wb") as sf:
                    sf.write(data)
                await self.qe_handler.stateflow_graph_to_tables(message[0])
                await self.qe_handler.init_data("0")
                await self.kafka_producer_create_topics()
                await self.kafka_consumer_subscribe_topic()
                self.duckdb_ready.set()
            case MessageType.SnapID:
                start = time.perf_counter()
                snapshot_id = self.networking.decode_message(data)[0]
                logging.warning(f"Query engine received snapshot: {snapshot_id}")
                await self.qe_handler.load_snapshots(snapshot_id)
                end = time.perf_counter()
                logging.warning(f"Snapshot {snapshot_id} committed. Took {end - start}s")

    async def start_tcp_service(self):
        async def request_handler(reader: StreamReader, writer: StreamWriter):
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack('>Q', data)
                    message = await reader.readexactly(size)
                    self.aio_task_scheduler.create_task(self.query_engine_controller(message))
            except asyncio.IncompleteReadError as e:
                logging.warning(f"TCP Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError as e:
                logging.warning(f"TCP Client cancelled: {e}")
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(request_handler, sock=self.qe_socket, limit=2**32)
        async with server:
            logging.warning("Serving TCP server")
            await server.serve_forever()

    async def _kafka_producer_start(self):
        self.kafka_query_result_producer = AIOKafkaProducer(bootstrap_servers=[KAFKA_URL],
                                                            client_id="QueryEngineProducer",
                                                            value_serializer=msgpack_serialization,
                                                            enable_idempotence=True)
        while True:
            try:
                await self.kafka_query_result_producer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                continue
            break

    async def kafka_producer_create_topics(self):
        await self._kafka_producer_start()
        egress_topic = f"{QUERY_ENGINE_TOPIC}--OUT"
        topic = None
        while topic is None:
            topic = set(await self.kafka_query_result_producer.partitions_for(egress_topic))
            logging.warning(f"Awaiting topic {egress_topic} to be created by the Styx coordinator")
            await asyncio.sleep(5)
        logging.warning(f"Topic {egress_topic} created for kafka producer init")
        self.producer_ready.set()

    async def _kafka_producer_ready(self):
        while True:
            try:
                await asyncio.wait_for(self.producer_ready.wait(), timeout=1.0)
                break
            except asyncio.TimeoutError:
                await asyncio.sleep(5)
        self.kafka_query_consumer = AIOKafkaConsumer(auto_offset_reset='earliest',
                                                     bootstrap_servers=[KAFKA_URL],
                                                     client_id="QueryEngineConsumer",
                                                     group_id="QueryEngineGroup",
                                                     session_timeout_ms=60000,
                                                     heartbeat_interval_ms=20000,
                                                     max_poll_interval_ms=600000,
                                                     enable_auto_commit=True)

    async def _kafka_consumer_start(self):
        await self._kafka_producer_ready()
        while True:
            try:
                await self.kafka_query_consumer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                continue
            break

    async def kafka_consumer_subscribe_topic(self):
        await self._kafka_consumer_start()
        ingress_topic = QUERY_ENGINE_TOPIC
        topics = []
        while ingress_topic not in topics:
            topics = set(await self.kafka_query_consumer.topics())
            logging.warning(
                f"Awaiting topic {ingress_topic} to be created by the Styx coordinator, current topics: {topics}")
            await asyncio.sleep(5)
        logging.warning(f"Topic {ingress_topic} created for consumer init")
        self.kafka_query_consumer.subscribe(topics=[ingress_topic])
        logging.warning(f"Query engine subscribed to {ingress_topic} for consuming messages")
        self.kafka_ready.set()

    async def _wait_for_kafka_ready(self):
        while True:
            try:
                await asyncio.wait_for(self.kafka_ready.wait(), timeout=1.0)
                logging.warning("Kafka is ready to consume client queries.")
                break
            except asyncio.TimeoutError:
                await asyncio.sleep(5)

    async def _wait_for_duckdb_ready(self):
        while True:
            try:
                await asyncio.wait_for(self.duckdb_ready.wait(), timeout=1.0)
                logging.warning("Duckdb is ready to consume client queries.")
                break
            except asyncio.TimeoutError:
                await asyncio.sleep(5)

    async def recovery_init(self):
        with open(STATEFLOW_FILE_PATH, "rb") as sf:
            message = self.networking.decode_message(sf.read())
        await self.qe_handler.stateflow_graph_to_tables(message[0])
        logging.warning("Restarted query engine")
        await self.kafka_producer_create_topics()
        await self.kafka_consumer_subscribe_topic()
        self.duckdb_ready.set()

    async def handle_client_query(self, kafka_message):
        async with self.semaphore:
            try:
                msg = msgpack_deserialization(kafka_message.value[2:])
                res = await self.qe_handler.get_query_result(msg[0])
                await self.kafka_query_result_producer.send_and_wait(f"{QUERY_ENGINE_TOPIC}--OUT",
                                                                     key=kafka_message.key,
                                                                     value=res)
            except Exception as e:
                logging.warning(f"Error decoding query: {e}")

    async def consume_queries(self):
        await self._wait_for_kafka_ready()
        await self._wait_for_duckdb_ready()
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(self.kafka_query_consumer.getone(), timeout=0.5)
                    task = asyncio.create_task(self.handle_client_query(msg))
                    self.query_tasks.add(task)
                    task.add_done_callback(self.query_tasks.discard)
                except asyncio.TimeoutError:
                    await asyncio.sleep(1)
                except Exception as e:
                    logging.error(f"Unexpected error in consuming a kafka message from client: {e}")
        finally:
            await self.kafka_query_consumer.stop()

    async def main(self):
        try:
            if await self.qe_handler.recovery_mode():
                logging.warning("Recovery initiated")
                await self.recovery_init()
            logging.warning("Recovery check done, resuming tasks")
            self.kafka_consumer_task = asyncio.create_task(self.consume_queries())
            await self.start_tcp_service()
        finally:
            self.qe_handler.close_connection()


if __name__ == '__main__':
    query_engine_service = QueryEngineService()
    uvloop.run(query_engine_service.main())
