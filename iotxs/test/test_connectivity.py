import asyncio
import json
from datetime import datetime

from .. import connectivity
from ..datatypes import DatetimeContent
from pydantic import BaseModel


class OnlyStr(BaseModel):
    msg: str


def test_mqtt_publish():
    connectivity.init()
    received_msg: list[OnlyStr] = []

    async def mqtt_publish():
        for i in range(5):
            connectivity.mqtt_client.publish("test", OnlyStr(msg='hello world!').json())
            await asyncio.sleep(0.1)
        connectivity.deinit()

    async def mqtt_subscribe():
        def subscribe_callback(client, userdata, message):
            received_msg.append(OnlyStr.parse_raw(message.payload))

        connectivity.mqtt_client.subscribe("test")
        connectivity.mqtt_client.message_callback_add("test", subscribe_callback)

    connectivity.coroutine_reqs.append(mqtt_subscribe())
    connectivity.coroutine_reqs.append(mqtt_publish())
    connectivity.thread.join()
    assert len(received_msg) == 5
    for msg in received_msg:
        assert msg.msg == 'hello world!'


def test_mongo_manipulate():
    connectivity.init()
    document_list_to_insert: list[DatetimeContent] = [
        DatetimeContent(content="hey{}".format(i), datetime=datetime.fromisoformat(datetime.now().isoformat()[:-3])) for i in
        range(10)]
    document_list_queried: list[DatetimeContent] = []

    async def mongo_manipulate():
        try:
            collection = connectivity.mongo_client["iotxs"]["test_connectivity"]
            await collection.drop()
            await collection.insert_many((document.dict() for document in document_list_to_insert))
            assert (await collection.estimated_document_count()) == 10
            cursor = collection.find()
            document_list_queried.extend([DatetimeContent.parse_obj(doc) for doc in await cursor.to_list(20)])
        except BaseException as e:
            print(e)
        finally:
            connectivity.deinit()

    connectivity.coroutine_reqs.append(mongo_manipulate())
    connectivity.thread.join()
    assert document_list_to_insert == document_list_queried
