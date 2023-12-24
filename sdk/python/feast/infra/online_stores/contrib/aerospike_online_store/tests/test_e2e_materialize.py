# This is local test for online store
# TODO: remove from branch

import logging
import os
from datetime import datetime

import pytest

from feast import FeatureStore
from feast.infra.online_stores.contrib.aerospike_online_store.aerospike_client import (
    AerospikeClient,
)
from feast.infra.online_stores.contrib.aerospike_online_store.aerospike_online_store import (
    AerospikeOnlineStoreConfig,
)
from feast.infra.online_stores.contrib.aerospike_online_store.tests.test_entity.driver_repo import (
    driver,
    driver_hourly_stats_view,
)
from feast.infra.online_stores.helpers import compute_entity_id
from feast.repo_config import RegistryConfig, RepoConfig

AEROSPIKE_ONLINE_STORE_CLASS = "feast_custom_online_store.aerospike_online_store.AerospikeOnlineStore"
TEST_REGISTRY_DATA_PATH = "test_entity/data/registry.db"
DRIVER_KEY_1 = 1004
DRIVER_KEY_2 = 1005
TEST_AEROSPIKE_NAMESPACE = "aura_universal_user_profile"
TEST_AEROSPIKE_SET = 'profiles'
AEROSPIKE_NAMESPACE = 'aerospike_namespace'
AEROSPIKE_SET_NAME = 'aerospike_set_name'

logger = logging.getLogger(__name__)
test_aerospike_config = {
    'host': 'aerospike-ci.ecs.isappcloud.com',
    'region': 'us-west-2',
    'port': '3000',
    'timeout': '200000'
}

test_feature_views_config = {
    "driver_stats": {
        AEROSPIKE_NAMESPACE: TEST_AEROSPIKE_NAMESPACE,
        AEROSPIKE_SET_NAME: TEST_AEROSPIKE_SET,
        "short_fv_name": "driveSt"
    }
}

online_store_config = AerospikeOnlineStoreConfig(
    aerospike_config=test_aerospike_config,
    feature_views_config=test_feature_views_config)  # TODO check with Fabi how to recive the namespace/set?
client = AerospikeClient(test_aerospike_config, logger)


def test_user_entity_end_to_end():
    repo_config = RepoConfig(
        registry=RegistryConfig(path=TEST_REGISTRY_DATA_PATH),
        project="driver_project",
        provider="local",
        online_store=online_store_config,
        entity_key_serialization_version=2
    )
    fs = FeatureStore(config=repo_config)
    # apply entity
    fs.apply([driver, driver_hourly_stats_view])

    # load data into online store
    start_date = datetime.strptime('01/01/2020 00:00:00', '%d/%m/%Y %H:%M:%S')
    end_date = datetime.strptime('01/01/2023 00:00:00', '%d/%m/%Y %H:%M:%S')
    fs.materialize(start_date=start_date, end_date=end_date)

    # Read features from online store
    online_features = fs.get_online_features(
        features=["driver_stats:avg_daily_trips", "driver_stats:string_feature"],
        entity_rows=[{"driver_id": "1004"}])
    assert online_features is not None
    feature_vector = online_features.to_dict()
    avg_daily_trips = feature_vector["avg_daily_trips"][0]
    assert avg_daily_trips == 539
    string_feature = feature_vector["string_feature"][0]
    assert string_feature == "test"


@pytest.fixture(autouse=True)
def my_setup_and_tear_down():
    # SETUP
    setup()
    yield  # this statement will let the tests execute
    # TEARDOWN
    cleanup()


def setup():
    insert_test_record(DRIVER_KEY_1)
    insert_test_record(DRIVER_KEY_2)


def cleanup():
    logger.info("cleanup after test")
    remove_test_record(DRIVER_KEY_1)
    remove_test_record(DRIVER_KEY_2)
    client.close()
    if os.path.isfile(TEST_REGISTRY_DATA_PATH):
        os.remove(TEST_REGISTRY_DATA_PATH)
        logger.info("registry file was removed successfully")


def insert_test_record(aerospike_key):
    bins = {
        'driver_id': aerospike_key,
        'insert_datetime': str(datetime.now())
    }
    client.put_record(TEST_AEROSPIKE_NAMESPACE, TEST_AEROSPIKE_SET, aerospike_key, bins)


def remove_test_record(aerospike_key):
    client.remove_record(TEST_AEROSPIKE_NAMESPACE, TEST_AEROSPIKE_SET, aerospike_key)

