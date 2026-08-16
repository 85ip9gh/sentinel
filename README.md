# Sentinel

Streaming telemetry and root-cause platform for self-hosted infrastructure.

Machines publish readings every few seconds into Kafka, Hadoop keeps the full
history partitioned by day, Spark learns what a normal hour looks like on each
host, and an AI agent investigates the readings that do not fit: it queries the
history, pulls the logs from that minute, runs read-only checks against the
host, and writes an incident report with its evidence attached.

**Status: step 2 of 5, running.** Two hosts publish into Kafka every ten
seconds, a sink lands every reading in HDFS partitioned by day, and a dashboard
serves the archive back. Nothing intelligent is wired up yet: there is no
baseline, no anomaly detection and no agent.

## Why

A monitoring gap cost a real hour. A server was believed to have been down for
fourteen hours when it had never gone down at all: the tailnet coordination
server reported it offline, that report was taken as the truth, and one `uptime`
call later refuted it. The lesson was about the instrument rather than the
machine, and this is the instrument.

## What step 1 does

Each host runs a collector that publishes to three topics:

| Topic | Contents | Default interval |
|---|---|---|
| `sentinel.system` | CPU, load, memory, swap, disks, network counters, temperatures, uptime, process count | 10s |
| `sentinel.container` | `docker ps` inventory with state and status | 30s |
| `sentinel.http` | Reachability, HTTP status and latency for each configured URL | 60s |

Every message is JSON keyed by host name, so a machine's readings stay ordered
relative to its own history.

## What step 2 does

A sink consumes all three topics and writes them into HDFS as newline-delimited
JSON, one immutable file per batch:

```
/sentinel/raw/<kind>/dt=<YYYY-MM-DD>/part-<timestamp>-<token>.ndjson
```

**Partitioning is by the reading's own timestamp, not by arrival.** A collector
that spent six hours spooled writes into the days it observed, so replaying a
backlog repairs history instead of stacking it onto the day the broker came
back. A reading whose timestamp cannot be parsed goes to `dt=unknown` rather
than being dropped or quietly filed under today.

**Offsets are committed after the files land, never before.** A crash in
between replays those readings and writes them again under a new name.
Duplicates are removable later by host, kind and timestamp. A gap in the
archive is not.

Files are written once and never appended to. Appending over WebHDFS would mean
a network round trip per reading and a partially written file after any crash.

### The dashboard

`http://127.0.0.1:8088` serves the archive: a card per host with its freshness,
the newest site check per URL, container counts, and what the archive actually
holds. It reads HDFS and deliberately not Kafka. A page served from the stream
would look identical whether or not anything had been stored, so it would prove
nothing. The cost is that readings appear one sink batch late, which is why
every row carries its own age.

### What the public view withholds

The dashboard is published under a personal domain, and telemetry from a
handful of personally owned machines is not neutral data. A hostname, a drive
letter, a RAM figure and a process count are a description of one named
person's equipment, and the same fields sampled every ten seconds are a
description of when that person is at their desk.

So the outside gets a projection. Hosts appear under stable aliases, mount
points, capacities, filesystem types, free memory and process counts are
dropped, and uptime is bucketed rather than exact, because an exact uptime
dates a reboot and a reboot dates a person. Percentages, temperatures,
latencies and freshness survive, which is everything the page was for.

Three properties matter more than the field list:

* The projection is applied once, to the whole status document, so the HTML
  page and `/api/status` cannot disagree about what is public.
* It is on unless `SENTINEL_PUBLIC=0`, so a forgotten variable fails towards
  saying less. A dashboard built without a view argument redacts.
* Aliases are stable, because a name that changed on every restart would
  destroy the one thing this page is for.

What redaction does not fix is timing. A card that goes quiet still says the
machine behind it is off, whatever the machine is called, and that is a
presence signal about a home rather than a fact about a computer.
`SENTINEL_PUBLIC_LAG_SECONDS=86400` answers it by serving yesterday's view, and
costs the page its liveness, so it is off by default and is a deliberate call.

Raw values are still one variable away, and the honest way to have both is a
second dashboard bound to loopback with `SENTINEL_PUBLIC=0`, rather than
turning redaction off on the instance the tunnel points at.

### The spool is the point

The broker runs on a workstation that is not on all the time. A collector that
simply dropped readings whenever the broker was unreachable would leave holes
exactly where an incident is most likely. So an undeliverable reading is
appended to a bounded newline-delimited file on the collector's own disk and
replayed in order once the broker answers again. The spool file is claimed by
rename before a replay starts, which is what keeps an asynchronous producer from
deleting readings whose failure has not been reported yet.

## Running it

### The stack

```
cp docker/.env.example docker/.env      # set SENTINEL_TAILNET_IP
docker compose -f docker/compose.yml up -d --build
```

Brings up Kafka, a single-node HDFS (NameNode plus DataNode), the sink and the
dashboard. The NameNode UI is on `127.0.0.1:9870` and the dashboard on
`127.0.0.1:8088`, both loopback only.

The broker publishes three listeners, because a Kafka client is told to
reconnect to the *advertised* address and the three kinds of client sit in
different networks: `127.0.0.1:9092` for processes on this machine,
`kafka:9095` for containers on the compose network, and `<tailnet-ip>:9094`
for collectors on other machines. The tailnet binding is explicit and Compose
refuses to start without it, because an empty value would bind `0.0.0.0` and
put an unauthenticated broker on the LAN.

The sink and the dashboard run inside the compose network on purpose. A WebHDFS
write is redirected from the NameNode to a DataNode by hostname, so a client
outside the network gets an address it cannot resolve.

### Collector

```
python -m venv .venv
.venv/bin/pip install -r requirements.txt
SENTINEL_ROLE=workstation python -m collector
```

`--dry-run` prints one round of readings as JSON and never touches Kafka, which
is the fastest way to see what a host actually reports. `--once` runs a single
cycle against the broker and exits non-zero if anything failed to deliver.

### Watching it work

```
python scripts/tail_topic.py sentinel.system --from-start --limit 5
```

## Configuration

Every setting is an environment variable with a working default.

| Variable | Default | Meaning |
|---|---|---|
| `SENTINEL_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `SENTINEL_HOST` | machine hostname | Identity stamped on every reading, and the Kafka key |
| `SENTINEL_ROLE` | `unknown` | Free-form label, for example `server` or `workstation` |
| `SENTINEL_SYSTEM_INTERVAL` | `10` | Seconds between host metric readings |
| `SENTINEL_CONTAINER_INTERVAL` | `30` | Seconds between container inventories |
| `SENTINEL_HTTP_INTERVAL` | `60` | Seconds between HTTP checks |
| `SENTINEL_HTTP_TARGETS` | empty | Comma-separated URLs. No targets means no HTTP checks |
| `SENTINEL_HTTP_TIMEOUT` | `10` | Per-request timeout in seconds |
| `SENTINEL_SPOOL_DIR` | `var/spool` | Where undeliverable readings are buffered |
| `SENTINEL_SPOOL_MAX_BYTES` | `67108864` | Spool cap per topic. Oldest readings are dropped first |
| `SENTINEL_FLUSH_TIMEOUT` | `5` | Seconds to wait for delivery confirmation each cycle |

Sink and dashboard:

| Variable | Default | Meaning |
|---|---|---|
| `SENTINEL_HDFS_URL` | `http://namenode:9870` | WebHDFS endpoint |
| `SENTINEL_HDFS_ROOT` | `/sentinel/raw` | Root of the raw archive |
| `SENTINEL_SINK_GROUP` | `sentinel-hdfs-sink` | Consumer group. Reset it to replay the whole retained log |
| `SENTINEL_SINK_BATCH_RECORDS` | `500` | Close a batch at this many readings |
| `SENTINEL_SINK_BATCH_SECONDS` | `60` | Close a batch at this age, so a quiet topic still lands |
| `SENTINEL_DASHBOARD_PORT` | `8088` | Dashboard listen port |
| `SENTINEL_PUBLIC` | `1` | Redact the dashboard. `0` serves real hostnames and full detail |
| `SENTINEL_HOST_ALIASES` | empty | `real=public,other=public2`. Unlisted hosts become `host-<digest>` |
| `SENTINEL_ALIAS_SALT` | empty | Makes the digest unguessable. Changing it renames every host |
| `SENTINEL_PUBLIC_LAG_SECONDS` | `0` | Hold readings back this long. `86400` removes the live presence signal |

## Deploying a collector

**Linux, under systemd:**

```
sudo deploy/g7/install.sh --bootstrap 100.x.x.x:9094 --role server
```

Installs into `/opt/sentinel`, runs as a dedicated unprivileged `sentinel` user
under `ProtectSystem=strict`, and spools to `/var/lib/sentinel/spool`. Needs
`python3-venv`, which is not installed by default on Ubuntu Server.

**Windows, as a scheduled task:**

```
powershell -ExecutionPolicy Bypass -File deploy\windows\install-task.ps1
```

Registered for the current user and started at logon, so no elevation is
needed. Docker Desktop also starts at logon and takes longer, which the spool
covers.

## Tests

```
pip install -r requirements-dev.txt
pytest
```

No Kafka, no HDFS, no network and no docker daemon are required. The producer,
the consumer, the writer and the clocks are all injected, so the suite covers
the failure paths that matter: a dead broker, a full local queue, a replay that
fails again, a source that raises, a spool over its cap, a batch spanning two
days, a failed HDFS write leaving offsets uncommitted, and a reading with no
usable timestamp.

## Roadmap

1. **Collectors and Kafka.** Done.
2. **HDFS sink writing day partitions, plus a dashboard over the archive.** Done.
3. Spark on YARN for daily rollups and per-host, per-hour baselines.
4. The agent: trigger rules, investigation tools, incident reports.
5. Public dashboard.

## Design notes

- **Kafka is transport, not archive.** Topic retention is seven days. History
  belongs in HDFS from step 2 onward.
- **One thread.** At these intervals concurrency would buy nothing and cost the
  ability to reason about ordering.
- **A failing source is an observation, not a crash.** A missing docker daemon,
  an unreadable mount and a refused HTTP connection are all recorded and the
  loop continues.
- **Single node, gigabytes not terabytes.** This is a home lab, and the
  documentation says so rather than implying a scale it does not have.

## License

MIT.
