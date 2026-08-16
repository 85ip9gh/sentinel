"""What the public view is allowed to say.

The dashboard is reachable from the open internet under a personal domain, and
telemetry from a handful of personally owned machines is not neutral data. A
real hostname, a drive letter, a total RAM figure and a process count together
describe one named person's equipment, and the same fields sampled every ten
seconds describe when that person is at their desk.

So the outside gets a projection rather than the record. Three rules hold it
together:

* It is applied once, to the whole status document, so the HTML page and the
  JSON API cannot drift apart and leak through the one that was forgotten.
* It is on unless the operator turns it off, so a missing environment variable
  fails towards saying less.
* Host aliases are stable, because a name that changed on every restart would
  destroy the one thing the page is for: watching a host over time.

What it does not solve is timing. A card that goes quiet still says the machine
is off, whatever the machine is called. `SENTINEL_PUBLIC_LAG_SECONDS` is the
answer to that one, and it costs the page its liveness, so it is off by default
and is the operator's call.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from typing import Any

_FALSE = {"0", "false", "no", "off"}

# Enough of the digest to keep two hosts apart, short enough to read as a label.
_ALIAS_DIGITS = 6

# Exact uptime dates a reboot, and a reboot dates a person. Buckets keep the
# "has this host been up long enough to trust" signal and drop the calendar.
_UPTIME_BUCKETS = (
    (1.0, "< 1 day"),
    (7.0, "1 to 7 days"),
    (30.0, "7 to 30 days"),
)
_UPTIME_LONGEST = "30+ days"


def uptime_bucket(days: float | None) -> str | None:
    if days is None:
        return None
    for limit, label in _UPTIME_BUCKETS:
        if days < limit:
            return label
    return _UPTIME_LONGEST


@dataclass(frozen=True)
class PublicView:
    """The projection applied before anything leaves the process."""

    enabled: bool = True
    aliases: dict[str, str] = field(default_factory=dict)
    salt: str = ""
    lag_seconds: float = 0.0

    @classmethod
    def from_env(cls, env: dict | None = None) -> "PublicView":
        env = dict(os.environ if env is None else env)
        raw_lag = env.get("SENTINEL_PUBLIC_LAG_SECONDS", "").strip()
        try:
            lag = max(0.0, float(raw_lag)) if raw_lag else 0.0
        except ValueError:
            # A typo in a lag is not worth refusing to serve over, and zero is
            # the behaviour the page had before the setting existed.
            lag = 0.0
        return cls(
            enabled=env.get("SENTINEL_PUBLIC", "").strip().lower() not in _FALSE,
            aliases=_parse_aliases(env.get("SENTINEL_HOST_ALIASES", "")),
            salt=env.get("SENTINEL_ALIAS_SALT", "").strip(),
            lag_seconds=lag,
        )

    def alias(self, host: str | None) -> str | None:
        """A stable public name for a host.

        A configured alias reads better on the page. Without one the digest is
        still safe on its own, which is what makes an unconfigured deployment
        safe rather than merely inconvenient.
        """
        if not host:
            return host
        mapped = self.aliases.get(host.lower())
        if mapped:
            return mapped
        digest = hashlib.sha256(f"{self.salt}{host.lower()}".encode()).hexdigest()
        return f"host-{digest[:_ALIAS_DIGITS]}"

    def apply(self, status: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return status

        hosts = [self._host(host) for host in status.get("hosts", [])]
        checks = [
            {**check, "host": self.alias(check.get("host"))}
            for check in status.get("checks", [])
        ]
        containers = [
            {**row, "host": self.alias(row.get("host"))}
            for row in status.get("containers", [])
        ]
        return {
            **status,
            "public": True,
            "hosts": hosts,
            "checks": checks,
            "containers": containers,
        }

    def _host(self, host: dict[str, Any]) -> dict[str, Any]:
        disk = host.get("disk_worst")
        if disk is not None:
            # The percentage is the interesting number. The mount point, the
            # filesystem type and the capacity are a description of hardware:
            # "G:\, FAT32, 16 GB" is a specific USB stick in a specific machine.
            disk = {"mount": None, "percent": disk.get("percent")}

        return {
            **host,
            "host": self.alias(host.get("host")),
            "disk_worst": disk,
            # Total RAM is a machine fingerprint; the percentage in use is not.
            "memory_available_gb": None,
            # Low value on the page, and it tracks what the person is running.
            "process_count": None,
            "uptime_days": None,
            "uptime_label": uptime_bucket(host.get("uptime_days")),
        }


def _parse_aliases(raw: str) -> dict[str, str]:
    """`real=public,other=public2`, ignoring anything malformed."""
    out: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        real, _, public = item.partition("=")
        real, public = real.strip().lower(), public.strip()
        if real and public:
            out[real] = public
    return out
