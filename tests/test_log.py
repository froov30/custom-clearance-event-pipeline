import json
import logging

from ccp.common.log import setup_logging


def test_json_line_with_fields(capsys):
    setup_logging("validator", "INFO")
    logging.getLogger("t").info("routed", extra={"fields": {"reason": "SCHEMA_INVALID"}})
    line = json.loads(capsys.readouterr().out.strip())
    assert line["service"] == "validator"
    assert line["msg"] == "routed"
    assert line["reason"] == "SCHEMA_INVALID"
    assert line["ts"].endswith("+00:00")
