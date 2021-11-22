from iotxs import connectivity

life_state = False


async def new_req_callback():
    ...


async def setup_new_req_callback():
    ...


def init():
    life_state = True
    connectivity.coroutine_reqs.append()


def deinit():
    ...
