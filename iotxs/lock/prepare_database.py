"""
Run this script to prepare the database for the space and all the things
"""
import iotxs.config
from . import record_types
from .. import connectivity


async def prepare_database_task():
    await connectivity.mongo_client[iotxs.config.DATABASE_NAME][
        iotxs.config.LOCK_REQ_RECORD_COLLECTION_NAME].drop()
    await connectivity.mongo_client[iotxs.config.DATABASE_NAME][
        iotxs.config.LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].drop()
    await connectivity.mongo_client[iotxs.config.DATABASE_NAME][
        iotxs.config.LOCK_STATE_RECORD_COLLECTION_NAME].drop()
    await connectivity.mongo_client[iotxs.config.DATABASE_NAME].create_collection(
        iotxs.config.LOCK_REQ_RECORD_COLLECTION_NAME,
        capped=True, size=10485760)
    await connectivity.mongo_client[iotxs.config.DATABASE_NAME][
        iotxs.config.LOCK_REQ_RECORD_COLLECTION_NAME].create_index("datetime")
    await connectivity.mongo_client[iotxs.config.DATABASE_NAME].create_collection(
        iotxs.config.LOCK_NOTIFICATION_RECORD_COLLECTION_NAME,
        capped=True, size=10485760)
    await connectivity.mongo_client[iotxs.config.DATABASE_NAME][
        iotxs.config.LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].create_index("datetime")
    await connectivity.mongo_client[iotxs.config.DATABASE_NAME].create_collection(
        iotxs.config.LOCK_STATE_RECORD_COLLECTION_NAME,
        capped=True, size=10485760)
    await connectivity.mongo_client[iotxs.config.DATABASE_NAME][
        iotxs.config.LOCK_STATE_RECORD_COLLECTION_NAME].create_index("datetime")


if __name__ == '__main__':
    connectivity.init()
    connectivity.coroutine_reqs.append(prepare_database_task())
    connectivity.deinit()
    connectivity.thread.join()
    print("Successfully finished the database preparation")
