import pytest
from confluent_kafka.admin import AdminClient

from ccp.common.config import Config

# Groups our tests create: infra round trip, validator collect(), and processor runs with a suffix.
TEST_GROUP_PREFIXES = (
    "ccp-test-",
    "ccp-it-",
    "ccp-processor-it-",
    "ccp-validator-it-",
    "ccp-monitor-dlq-it-",
)


@pytest.fixture(scope="session", autouse=True)
def delete_test_consumer_groups():
    """Test consumers must not leave groups behind in the shared dev Kafka (limit 6, D-054).

    Runs once after the integration session. The real groups, `ccp-validator` and
    `ccp-processor`, are never touched.
    """
    yield
    admin = AdminClient({"bootstrap.servers": Config.from_env().kafka_bootstrap_servers})
    listed = admin.list_consumer_groups(request_timeout=15).result().valid
    groups = [g.group_id for g in listed if g.group_id.startswith(TEST_GROUP_PREFIXES)]
    if not groups:
        return
    failed = []
    for group, future in admin.delete_consumer_groups(groups, request_timeout=15).items():
        try:
            future.result()
        except Exception as e:  # report them all together below
            failed.append(f"{group}: {e}")
    if failed:
        raise RuntimeError("could not delete test consumer groups: " + "; ".join(failed))
