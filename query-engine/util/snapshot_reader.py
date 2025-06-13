import os
import re
from collections import defaultdict

from styx.common.serialization import zstd_msgpack_deserialization

SNAPSHOT_BUCKET_NAME: str = os.getenv('SNAPSHOT_BUCKET_NAME', "styx-snapshots")


class MinioReader:
    def __init__(self, minio_client, snapshot_id):
        self.minio_client = minio_client
        self.snapshot_id: str = snapshot_id
        self.operator_snapshots: dict = defaultdict(list)

    async def identify_minio_delta(self, operator_list):
        prefix = "data/"
        pattern = re.compile(rf"^{re.escape(prefix)}.*/{self.snapshot_id}\.bin$")
        for obj in self.minio_client.list_objects(SNAPSHOT_BUCKET_NAME, prefix=prefix, recursive=True):
            if pattern.match(obj.object_name):
                operator_name = obj.object_name.split("/")[1]
                if operator_name in operator_list:
                    self.operator_snapshots[operator_name].append(obj.object_name)

    async def deserialize_snapshot_partition(self, operator):
        for object_path in self.operator_snapshots[operator]:
            partition_data = zstd_msgpack_deserialization(
                self.minio_client.get_object(SNAPSHOT_BUCKET_NAME, object_path).data
            )
            yield partition_data

    async def deserialize_snapshots(self, operator):
        deserialized_objects = {}
        for object_path in self.operator_snapshots[operator]:
            partition_data = zstd_msgpack_deserialization(
                self.minio_client.get_object(SNAPSHOT_BUCKET_NAME, object_path).data
            )
            deserialized_objects.update(partition_data)
        return deserialized_objects

