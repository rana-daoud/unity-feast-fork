from typing import Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class AerospikeOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        # TODO
        # self.container = DockerContainer("aerospike").with_exposed_ports("6379")

    def create_online_store(self) -> Dict[str, str]:
        pass
        # self.container.start()
        # log_string_to_wait_for = "Ready to accept connections"
        # wait_for_logs(
        #     container=self.container, predicate=log_string_to_wait_for, timeout=10
        # )
        # exposed_port = self.container.get_exposed_port("6379")
        # return {"type": "aerospike", "connection_string": f"localhost:{exposed_port},db=0"}

    def teardown(self):
        pass
        # self.container.stop()
