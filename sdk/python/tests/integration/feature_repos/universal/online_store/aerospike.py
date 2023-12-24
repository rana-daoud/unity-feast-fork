from typing import Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)

AEROSPIKE_NAMESPACE = "aerospike_namespace"
AEROSPIKE_SET_NAME = "aerospike_set_name"
SHORT_FEATURE_VIEW_NAME = "short_fv_name"


class AerospikeOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = DockerContainer("aerospike:ce-7.0.0.3_1").with_exposed_ports("3000")

    # TODO check tests with aerospike docker using make command: make test-python-universal-aerospike-online
    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = "Ready to accept connections"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=30
        )

        container_host = self.container.get_container_host_ip()
        exposed_port = self.container.get_exposed_port("3000")

        return {
            "type": "aerospike",
            "aerospike_config": {
                "host": container_host,
                "port": exposed_port
            },
            "feature_views_config": {
                "driver_stats": {
                    AEROSPIKE_NAMESPACE: "test",
                    AEROSPIKE_SET_NAME: "test",
                    SHORT_FEATURE_VIEW_NAME: "driveSt"
                }
            }
        }

    def teardown(self):
        self.container.stop()
