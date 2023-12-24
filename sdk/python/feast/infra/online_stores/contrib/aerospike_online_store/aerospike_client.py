import logging

import aerospike

logger = logging.getLogger(__name__)


class AerospikeClient:
    _client: aerospike.Client = None

    def __init__(self, config, logger):
        self.config = config
        self._logger = logger
        client_config = {
            'hosts': [(self.config['host'], int(self.config['port']))],
            'policies': {
                'timeout': int(self.config['timeout'])
            },
            'user': self.config.get('username'),
            'password': self.config.get('password')
        }
        self._client = aerospike.client(client_config)
        self._client.connect(client_config['user'], client_config['password'])
        self._logger.info("Aerospike client is connected successfully")

    # def update_record_if_existed(self, namespace, set_name, key, bins):
    #     if not namespace or not set_name or not key or not bins:
    #         self._logger.error("One of the required params [namespace, set_name, key, bins] is None")
    #         return False
    #     try:
    #         # update_policy = {'exists': aerospike.POLICY_EXISTS_UPDATE}  # update only if key exists
    #         self._client.put((namespace, set_name, key), bins)  #, policy=update_policy)
    #         return True
    #     except Exception as ex:
    #         self._logger.error(f"Failed to update record with primary key [{key}] : {str(ex)}")
    #         return False

    def put_record(self, namespace, set_name, key, bins):
        try:
            self._client.put((namespace, set_name, key), bins)
        except Exception as ex:
            self._logger.error("Failed to put record with primary key [{key}] : {error_msg}"
                               .format(key=key, error_msg=str(ex)))

    def remove_record(self, namespace, set_name, key):
        try:
            self._client.remove((namespace, set_name, key))
        except Exception as ex:
            self._logger.error("Failed to remove record with primary key [{key}] : {error_msg}"
                               .format(key=key, error_msg=str(ex)))

    def is_record_exists(self, namespace, set_name, key):
        try:
            (key, metadata) = self._client.exists((namespace, set_name, key))
            if metadata is None:
                return False
            return True
        except Exception as ex:
            self._logger.error("Failed to check if record with primary key [{key}] exists: {error_msg}"
                               .format(key=key, error_msg=str(ex)))

    def get_record(self, namespace, set_name, key):
        try:
            (key, metadata, bins) = self._client.get((namespace, set_name, key))
            return bins
        except Exception as ex:
            self._logger.error("Failed to get record for primary key [{key}]: {error_msg}"
                               .format(key=key, error_msg=str(ex)))

    def get_records(self, namespace, set_name, primary_keys):
        try:
            result = {}
            key_list = []
            for primary_key in primary_keys:
                key = (namespace, set_name, primary_key)
                key_list.append(key)

            records = self._client.get_many(key_list)
            if records is not None:
                for record in records:
                    primary_key = record[0][2]  # extract primary key from record
                    bins = record[2]  # extract bins from record
                    if bins is not None:
                        result[primary_key] = bins

                self._logger.info("Found {count} records for keys {keys}.".format(count=len(result), keys=primary_keys))
            return result
        except Exception as ex:
            self._logger.error("Failed to get records :" + str(ex))

    def select_bins_of_record(self, namespace, set_name, key, bins_to_select):
        try:
            (key, meta, bins) = self._client.select((namespace, set_name, key), bins_to_select)
            return bins
        except Exception as ex:
            self._logger.error("Failed to select bins of record :" + str(ex))

    def close(self):
        try:
            if self._client and self._client.is_connected():
                self._client.close()
                self._logger.info("Aerospike client was closed successfully")
        except Exception as ex:
            self._logger.error("Failed to close client :" + str(ex))
