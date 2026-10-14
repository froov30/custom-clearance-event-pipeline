import os
import time

from ccp.common import probe


def test_fresh_heartbeat_passes(tmp_path, monkeypatch):
    monkeypatch.setenv("HEARTBEAT_DIR", str(tmp_path))
    (tmp_path / "processor.heartbeat").touch()
    assert probe.main(["processor", "60"]) == 0


def test_stale_heartbeat_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("HEARTBEAT_DIR", str(tmp_path))
    beat = tmp_path / "processor.heartbeat"
    beat.touch()
    old = time.time() - 120
    os.utime(beat, (old, old))
    assert probe.main(["processor", "60"]) == 1


def test_missing_heartbeat_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("HEARTBEAT_DIR", str(tmp_path))
    assert probe.main(["validator"]) == 1


def test_default_max_age_is_60_seconds(tmp_path, monkeypatch):
    monkeypatch.setenv("HEARTBEAT_DIR", str(tmp_path))
    beat = tmp_path / "monitor.heartbeat"
    beat.touch()
    recent = time.time() - 50  # over the 45 s minimum from D-053, under the default
    os.utime(beat, (recent, recent))
    assert probe.main(["monitor"]) == 0
