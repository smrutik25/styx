import asyncio
import os
import re
import socket
import struct
from asyncio import StreamReader, StreamWriter

import uvloop
from minio import Minio

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.util.aio_task_scheduler import AIOTaskScheduler


MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
SNAPSHOT_BUCKET_NAME: str = os.getenv('SNAPSHOT_BUCKET_NAME', "styx-snapshots")
QUERY_ENGINE_PORT: int = int(os.getenv('QUERY_ENGINE_PORT', 7000))


class QueryEngineService(object):
    def __init__(self):
        self.minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        self.aio_task_scheduler = AIOTaskScheduler()

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
                # attrs = vars(message[0])
                # logging.warning(', '.join("%s: %s" % item for item in attrs.items()))
            case MessageType.SnapID:
                snapshot_id = self.networking.decode_message(data)[0]
                logging.warning(f"Query engine received snapshot {snapshot_id}")
                matching_keys = []
                prefix = "data/"
                pattern = re.compile(rf"^{prefix}[^/]+/id\.bin$")
                for obj in self.minio_client.list_objects(SNAPSHOT_BUCKET_NAME, prefix=prefix, recursive=True):
                    if pattern.match(obj.object_name):
                        matching_keys.append(obj.object_name)
                logging.info(f"Matching keys = {matching_keys}")

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

    def start_networking_tasks(self):
        self.networking.start_networking_tasks()

    async def main(self):
        self.start_networking_tasks()
        logging.info("Started networking tasks")
        await self.start_tcp_service()


if __name__ == '__main__':
    query_engine_service = QueryEngineService()
    uvloop.run(query_engine_service.main())