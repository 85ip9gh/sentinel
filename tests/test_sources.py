import json

from collector.sources import containers, http_check, system


def test_system_collect_returns_the_expected_shape():
    data = system.collect()

    assert set(data) >= {"cpu", "memory", "disks", "network", "uptime_seconds", "process_count"}
    assert 0 <= data["memory"]["percent"] <= 100
    assert data["uptime_seconds"] > 0
    assert isinstance(data["disks"], list)
    # Everything must survive the trip through Kafka as JSON.
    json.dumps(data)


def test_one_filesystem_is_reported_once_however_many_times_it_is_mounted(monkeypatch):
    """A hardened systemd unit sees the root filesystem through several bind mounts."""

    class Part:
        def __init__(self, device, mountpoint, fstype):
            self.device = device
            self.mountpoint = mountpoint
            self.fstype = fstype

    parts = [
        Part("/dev/mapper/vg-root", "/", "ext4"),
        Part("/dev/mapper/vg-root", "/tmp", "ext4"),
        Part("/dev/mapper/vg-root", "/var/lib/sentinel", "ext4"),
        Part("/dev/sda2", "/boot", "ext4"),
        Part("tmpfs", "/run", "tmpfs"),
    ]
    class Usage:
        total = 250_000_000_000
        used = 20_000_000_000
        percent = 8.1

    monkeypatch.setattr(system.psutil, "disk_partitions", lambda all=False: parts)
    monkeypatch.setattr(system.psutil, "disk_usage", lambda mount: Usage())

    disks = system._disks()

    assert [d["mount"] for d in disks] == ["/", "/boot"]


def test_http_check_reports_a_healthy_response():
    class Response:
        status_code = 200

    result = http_check.check("https://pesanth.com", fetch=lambda url, timeout: Response())

    assert result["ok"] is True
    assert result["status"] == 200
    assert result["error"] is None
    assert result["latency_ms"] >= 0


def test_http_check_treats_a_server_error_as_not_ok():
    class Response:
        status_code = 503

    result = http_check.check("https://pesanth.com", fetch=lambda url, timeout: Response())

    assert result["ok"] is False
    assert result["status"] == 503


def test_http_check_turns_an_exception_into_an_observation():
    def fetch(url, timeout):
        raise ConnectionError("no route to host")

    result = http_check.check("https://pesanth.com", fetch=fetch)

    assert result["ok"] is False
    assert result["status"] is None
    assert "ConnectionError" in result["error"]


def test_check_all_covers_every_target():
    class Response:
        status_code = 200

    results = http_check.check_all(
        ("https://a.test", "https://b.test"), fetch=lambda url, timeout: Response()
    )

    assert [r["url"] for r in results] == ["https://a.test", "https://b.test"]


def test_containers_parses_the_docker_json_lines():
    raw = (
        '{"ID":"abc123","Names":"sentinel-kafka","Image":"apache/kafka:3.9.0",'
        '"State":"running","Status":"Up 2 minutes","RunningFor":"2 minutes ago"}\n'
    )
    result = containers.collect(runner=lambda cmd: raw, check_available=False)

    assert result == [
        {
            "id": "abc123",
            "names": "sentinel-kafka",
            "image": "apache/kafka:3.9.0",
            "state": "running",
            "status": "Up 2 minutes",
            "runningfor": "2 minutes ago",
        }
    ]


def test_containers_reports_nothing_when_docker_is_absent(monkeypatch):
    monkeypatch.setattr(containers.shutil, "which", lambda _: None)
    assert containers.collect() == []


def test_a_docker_failure_is_an_empty_list_not_a_crash():
    def boom(cmd):
        raise OSError("docker daemon not running")

    assert containers.collect(runner=boom, check_available=False) == []
