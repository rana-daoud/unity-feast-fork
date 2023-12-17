import base64
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pytz

import feast.type_map
from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.contrib.aerospike_online_store.aerospike_client import (
    AerospikeClient,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

logger = logging.getLogger(__name__)

AEROSPIKE_NAMESPACE = 'aerospike_namespace'
AEROSPIKE_SET_NAME = 'aerospike_set_name'


class AerospikeOnlineStoreConfig(FeastConfigBaseModel):
    """
    Configuration for the Aerospike online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """
    type = "aerospike"
    aerospike_config: dict = {}
    feature_views_config: dict = {}  # map feature_view to namespace/set in aerospike


class AerospikeOnlineStore(OnlineStore):
    """
    An online store implementation that uses Aerospike.
    NOTE: The class *must* end with the `OnlineStore` suffix.
    """
    def __init__(self):
        logger.info("Initializing aerospike online store")

    def update(
            self,
            config: RepoConfig,
            tables_to_delete: Sequence[FeatureView],
            tables_to_keep: Sequence[FeatureView],
            entities_to_delete: Sequence[Entity],
            entities_to_keep: Sequence[Entity],
            partial: bool,
    ):
        logger.info("AerospikeOnlineStore - UPDATE: feast apply was invoked")

    def online_write_batch(
            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[
                Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
            ],
            progress: Optional[Callable[[int], Any]],
    ) -> None:
        feature_view_name = table.name
        logger.info(f"AerospikeOnlineStore - Starting online write for feature view [{feature_view_name}]")
        start = datetime.now()

        client = AerospikeClient(config.online_store.aerospike_config, logger)

        self.update_records(client, data, feature_view_name, progress, config)
        client.close()
        total_time = datetime.now() - start
        logger.info(f"AerospikeOnlineStore - Finished online write successfully for feature view [{feature_view_name}]."
                    f"Total time in seconds: {total_time.seconds}")

    def update_records(self, client, data, feature_view_name, progress, config):
        feature_views_config = config.online_store.feature_views_config
        feature_view_details = feature_views_config[feature_view_name]

        failure_count = 0
        for entity_key, values, timestamp, created in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=2,
            )

            # timestamp = _to_naive_utc(timestamp)
            # TODO make it general from ValueProto
            entity_key_value = entity_key.entity_values.pop()
            # entity_key_value = entity_key.entity_values[0].int64_val
            aerospike_key = entity_key_value
            feature_values_dict = self.generate_feature_values_dict(values)
            bins_to_update = {
                feature_view_name: feature_values_dict
            }
            # insert/update the bin based on primary key
            success = client.update_record_if_existed(feature_view_details[AEROSPIKE_NAMESPACE],
                                                      feature_view_details[AEROSPIKE_SET_NAME],
                                                      aerospike_key,
                                                      bins_to_update)
            if not success:
                failure_count += 1
            if progress and success:
                progress(1)

    def online_read(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKeyProto],
            requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        logger.info(f"AerospikeOnlineStore - Starting online read from feature view [{table.name}]")
        feature_views_config = config.online_store.feature_views_config

        aerospike_keys = [item.entity_values[0].string_val for item in entity_keys]

        client = AerospikeClient(config.online_store.aerospike_config, logger)
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        try:
            feature_view_name = table.name
            feature_view_details = feature_views_config[feature_view_name]
            records = client.get_records(feature_view_details[AEROSPIKE_NAMESPACE],
                                         feature_view_details[AEROSPIKE_SET_NAME],
                                         aerospike_keys)

            result = self._prepare_read_result(feature_view_name, records, requested_features)
            logger.info(f"AerospikeOnlineStore - Finished online read successfully from feature view [{table.name}]")
        except Exception as ex:
            logger.error(f"AerospikeOnlineStore - Failed while updating records of feature view [{table.name}]" + str(ex))
        finally:
            client.close()

        return result

    @staticmethod
    def generate_feature_values_dict(values: Dict[str, ValueProto]):
        feature_values_dict = {}
        for feature_name, val in values.items():
            # result is tuple of field descriptor and value (<FileDescriptor>, <value>)
            field = val.ListFields()[0]
            # get value of field from tuple
            if field:
                feature_values_dict[feature_name] = field[1]

        return feature_values_dict

    @staticmethod
    def _prepare_read_result(feature_view_name, records, requested_features):
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for primary_key, bins in records.items():
            features = bins[feature_view_name]
            res = {}
            for feature_name, value in features.items():
                if feature_name in requested_features:
                    res[feature_name] = feast.type_map.python_values_to_proto_values([value])[0]
            if res:
                # timestamp = None
                result.append((None, res))
        return result

    def _to_naive_utc(ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts
        else:
            return ts.astimezone(pytz.utc).replace(tzinfo=None)

    def teardown(
            self,
            config: RepoConfig,
            tables: Sequence[FeatureView],
            entities: Sequence[Entity],
    ):
        logger.info("AerospikeOnlineStore - teardown was invoked")

