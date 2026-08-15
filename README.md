# Sentinel

Streaming telemetry and root-cause platform for self-hosted infrastructure.

Machines publish readings every few seconds into Kafka, Hadoop keeps the full
history partitioned by day, Spark learns what a normal hour looks like on each
host, and an AI agent investigates the readings that do not fit: it queries the
history, pulls the logs from that minute, runs read-only checks against the
host, and writes an incident report with its evidence attached.

**Status: step 1 of 5.** Collectors and Kafka only. Nothing intelligent is
wired up yet, and the storage and agent layers do not exist.

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

### The spool is the point

The broker runs on a workstation that is not on all the time. A collector that
simply dropped readings whenever the broker was unreachable would leave holes
exactly where an incident is most likely. So an undeliverable reading is
appended to a bounded newline-delimited file on the collector's own disk and
replayed in order once the broker answers again. The spool file is claimed by
rename before a replay starts, which is what keeps an asynchronous producer from
deleting readings whose failure has not been reported yet.

## Running it

### Broker

```
cp docker/.env.example docker/.env      # set SENTINEL_TAILNET_IP
docker compose -f docker/compose.yml up -d
```

The broker publishes two listeners: `127.0.0.1:9092` for local clients and
`<tailnet-ip>:9094` for remote collectors. The tailnet binding is explicit and
Compose refuses to start without it, because an empty value would bind `0.0.0.0`
and put an unauthenticated broker on the LAN.

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

## Deploying a collector to a Linux host

```
deploy/g7/install.sh --bootstrap 100.x.x.x:9094 --role server
```

Installs into `/opt/sentinel`, runs as a dedicated unprivileged `sentinel` user
under systemd, and spools to `/var/lib/sentinel/spool`.

## Tests

```
pip install -r requirements-dev.txt
pytest
```

No Kafka, no network and no docker daemon are required. The producer, the
sources and the loop are all injected, so the suite covers the failure paths
that matter: a dead broker, a full local queue, a replay that fails again, a
source that raises, and a spool over its cap.

## Roadmap

1. **Collectors and Kafka.** Done.
2. HDFS sink writing day partitions, plus a plain dashboard over recent data.
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
