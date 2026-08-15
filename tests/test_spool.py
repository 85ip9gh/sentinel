from collector.spool import Spool


def test_append_then_drain_returns_payloads_in_order(tmp_path):
    spool = Spool(tmp_path, max_bytes=10_000)
    for i in range(3):
        spool.append("sentinel.system", f'{{"n":{i}}}')

    seen = []
    sent = spool.drain("sentinel.system", lambda payload: seen.append(payload) or True)

    assert sent == 3
    assert seen == ['{"n":0}', '{"n":1}', '{"n":2}']
    assert spool.pending("sentinel.system") == 0
    assert not spool.path_for("sentinel.system").exists()


def test_a_refusal_stops_the_drain_and_keeps_the_remainder(tmp_path):
    spool = Spool(tmp_path, max_bytes=10_000)
    for i in range(4):
        spool.append("sentinel.system", f'{{"n":{i}}}')

    def send(payload):
        return '"n":2' not in payload

    sent = spool.drain("sentinel.system", send)

    assert sent == 2
    assert spool.pending("sentinel.system") == 2

    rest = []
    spool.drain("sentinel.system", lambda payload: rest.append(payload) or True)
    assert rest == ['{"n":2}', '{"n":3}']


def test_draining_an_empty_spool_is_a_no_op(tmp_path):
    spool = Spool(tmp_path, max_bytes=10_000)
    assert spool.drain("sentinel.http", lambda _: True) == 0
    assert spool.pending("sentinel.http") == 0


def test_the_cap_drops_the_oldest_readings(tmp_path):
    # Nine bytes per line including the newline, so the cap holds three.
    spool = Spool(tmp_path, max_bytes=27)
    for i in range(6):
        spool.append("sentinel.system", f'{{"n":{i}}}')

    seen = []
    spool.drain("sentinel.system", lambda payload: seen.append(payload) or True)

    assert seen == ['{"n":3}', '{"n":4}', '{"n":5}']


def test_the_directory_is_created_on_demand(tmp_path):
    target = tmp_path / "nested" / "spool"
    spool = Spool(target, max_bytes=1000)
    spool.append("sentinel.container", "{}")
    assert target.is_dir()
