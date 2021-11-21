from ..msg_types import LockNotification


def test_omit_field():
    assert LockNotification(state="HOLD").json(exclude_unset=True) == '{"state": "HOLD"}'
