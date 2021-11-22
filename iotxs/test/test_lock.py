from ..lock import mqtt_listener
import re


def test_re_pattern():
    assert re.search(database_pusher.CLIENT_NAME_PATTERN, "jiowe/wjiofew/jfoiwejf/wioefj").group(1) == "wjiofew"
