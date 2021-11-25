"""
Run this script to prepare the database for the space and all the things
"""

from . import record_types
from .. import connectivity


async def prepare_database_task():
    await connectivity.mongo_client[record_types.DATABASE_NAME][
        record_types.LOCK_REQ_RECORD_COLLECTION_NAME].drop()
    await connectivity.mongo_client[record_types.DATABASE_NAME][
        record_types.LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].drop()
    await connectivity.mongo_client[record_types.DATABASE_NAME][
        record_types.LOCK_STATE_RECORD_COLLECTION_NAME].drop()
    await connectivity.mongo_client[record_types.DATABASE_NAME].create_collection(
        record_types.LOCK_REQ_RECORD_COLLECTION_NAME,
        capped=True, size=1000000)
    await connectivity.mongo_client[record_types.DATABASE_NAME][
        record_types.LOCK_REQ_RECORD_COLLECTION_NAME].create_index("datetime")
    await connectivity.mongo_client[record_types.DATABASE_NAME].create_collection(
        record_types.LOCK_NOTIFICATION_RECORD_COLLECTION_NAME,
        capped=True, size=1000000)
    await connectivity.mongo_client[record_types.DATABASE_NAME][
        record_types.LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].create_index("datetime")
    await connectivity.mongo_client[record_types.DATABASE_NAME].create_collection(
        record_types.LOCK_STATE_RECORD_COLLECTION_NAME,
        capped=True, size=1000000)
    await connectivity.mongo_client[record_types.DATABASE_NAME][
        record_types.LOCK_STATE_RECORD_COLLECTION_NAME].create_index("datetime")


if __name__ == '__main__':
    connectivity.init()
    connectivity.coroutine_reqs.append(prepare_database_task())
    connectivity.deinit()
    connectivity.thread.join()
    print("Successfully finished the database preparation")
