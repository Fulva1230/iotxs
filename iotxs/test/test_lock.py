from .. import lock
import re


def test_re_pattern():
    assert re.search(lock.CLIENT_NAME_PATTERN, "jiowe/wjiofew/jfoiwejf/wioefj").group(1) == "wjiofew"
