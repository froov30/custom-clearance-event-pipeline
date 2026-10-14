"""Manifest guard (no cluster needed).

Kubernetes injects `<SERVICE>_PORT=tcp://ip:port` into every pod for each Service in the
namespace. The Service named `api` therefore sets API_PORT, which collides with our own
API_PORT setting and crash-loops every service (D-061).
"""

from pathlib import Path

import pytest

MANIFESTS = sorted(Path(__file__).parent.parent.joinpath("deploy", "k8s").glob("*.yaml"))
POD_MANIFESTS = [m for m in MANIFESTS if "containers:" in m.read_text(encoding="utf-8")]


def test_pod_manifests_are_found():
    assert len(POD_MANIFESTS) == 8


@pytest.mark.parametrize("manifest", POD_MANIFESTS, ids=lambda m: m.name)
def test_service_links_are_disabled(manifest):
    assert "enableServiceLinks: false" in manifest.read_text(encoding="utf-8")


def test_kafka_keeps_its_log_on_a_volume():
    # Without this a broker pod restart loses every topic and offset (D-062).
    text = (MANIFESTS[0].parent / "kafka.yaml").read_text(encoding="utf-8")
    assert "{ name: KAFKA_LOG_DIRS, value: /var/lib/kafka/data }" in text
    assert "mountPath: /var/lib/kafka/data" in text
    assert "volumeClaimTemplates:" in text
