import pytest

from collector.config import TOPICS, Config


def test_defaults_need_no_environment():
    config = Config.from_env({})
    assert config.bootstrap == "127.0.0.1:9092"
    assert config.system_interval == 10.0
    assert config.http_targets == ()
    assert config.host


def test_values_come_from_the_environment():
    config = Config.from_env(
        {
            "SENTINEL_BOOTSTRAP": "100.125.224.124:9094",
            "SENTINEL_HOST": "g7-server",
            "SENTINEL_ROLE": "server",
            "SENTINEL_SYSTEM_INTERVAL": "5",
            "SENTINEL_HTTP_TARGETS": "https://pesanth.com, https://cubestore.pesanth.com ",
        }
    )
    assert config.bootstrap == "100.125.224.124:9094"
    assert config.host == "g7-server"
    assert config.role == "server"
    assert config.system_interval == 5.0
    assert config.http_targets == ("https://pesanth.com", "https://cubestore.pesanth.com")


def test_blank_values_fall_back_to_defaults():
    config = Config.from_env({"SENTINEL_BOOTSTRAP": "   ", "SENTINEL_SYSTEM_INTERVAL": ""})
    assert config.bootstrap == "127.0.0.1:9092"
    assert config.system_interval == 10.0


@pytest.mark.parametrize("value", ["0", "-5", "abc"])
def test_bad_intervals_fail_loudly(value):
    with pytest.raises(ValueError):
        Config.from_env({"SENTINEL_SYSTEM_INTERVAL": value})


def test_every_kind_has_a_topic():
    assert set(TOPICS) == {"system", "http", "container"}
    assert all(name.startswith("sentinel.") for name in TOPICS.values())
