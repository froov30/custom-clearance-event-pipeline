from pathlib import Path

import pytest

from ccp.common.refdata import RefData, load

REF_DIR = Path(__file__).resolve().parents[1] / "data" / "ref"


def test_bundled_reference_data_loads():
    ref = load(REF_DIR)
    assert len(ref.ports) >= 30 and len(ref.hs_codes) >= 20
    assert "NLRTM" in ref.ports and "847130" in ref.hs_codes


def test_unknown_in_reports_only_unknown_codes():
    ref = RefData(ports=frozenset({"NLRTM"}), hs_codes=frozenset({"847130"}))
    assert ref.unknown_in({"port_code": "NLRTM", "hs_code": "847130"}) == []
    assert ref.unknown_in({"port_code": "XXZZZ", "hs_code": "999999"}) == [
        "port_code=XXZZZ",
        "hs_code=999999",
    ]
    assert ref.unknown_in({"reason": "x-ray"}) == []


def test_malformed_row_fails_loudly(tmp_path):
    (tmp_path / "ports.csv").write_text("code,name\nNLRTM,Rotterdam\nrotterdam,bad\n")
    (tmp_path / "hs_codes.csv").write_text("code,description\n847130,Laptops\n")
    with pytest.raises(ValueError, match="rotterdam"):
        load(tmp_path)
