#!/usr/bin/env bash
# Install the Sentinel dashboard mirror on the always-on host.
#
#   sudo deploy/g7/install-mirror.sh --upstream http://100.100.100.100:8088/api/status
#
# The mirror serves the public page from a host that does not get switched off,
# so the site no longer goes dark with the workstation that holds the archive.
# It listens on loopback only: the Cloudflare Tunnel runs on this same host and
# reaches it locally, and nothing else should.
#
# Idempotent: re-running updates the code and the environment file, then
# restarts the service. Shares the virtualenv the collector installer creates.
set -euo pipefail

UPSTREAM=""
BIND="127.0.0.1"
PORT="8088"
INTERVAL="20"
PREFIX="/opt/sentinel"
STATE_DIR="/var/lib/sentinel"
ENV_DIR="/etc/sentinel"
SERVICE_USER="sentinel"

usage() {
  sed -n '2,12p' "$0"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --upstream) UPSTREAM="$2"; shift 2 ;;
    --bind) BIND="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

[[ -n "$UPSTREAM" ]] || { echo "--upstream is required" >&2; exit 1; }
[[ $EUID -eq 0 ]] || { echo "run with sudo" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> user and directories"
id -u "$SERVICE_USER" >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" "$PREFIX" "$STATE_DIR"
install -d -m 0750 "$ENV_DIR"

echo "==> code"
# The dashboard package imports the sink's partition and writer helpers, so
# both go over even though nothing here writes to HDFS.
for package in dashboard sink; do
  rm -rf "${PREFIX:?}/$package"
  cp -r "$REPO_ROOT/$package" "$PREFIX/$package"
done
cp "$REPO_ROOT/requirements.txt" "$PREFIX/requirements.txt"
chown -R "$SERVICE_USER:$SERVICE_USER" "$PREFIX/dashboard" "$PREFIX/sink" "$PREFIX/requirements.txt"

echo "==> virtualenv"
# Same pip-not-python test as the collector installer: a venv created while
# python3-venv was missing has an interpreter and no pip.
if [[ ! -x "$PREFIX/.venv/bin/pip" ]]; then
  if ! python3 -c "import ensurepip" >/dev/null 2>&1; then
    echo "python3-venv is missing. Install it with: sudo apt-get install -y python3-venv" >&2
    exit 1
  fi
  rm -rf "$PREFIX/.venv"
  python3 -m venv "$PREFIX/.venv"
fi
"$PREFIX/.venv/bin/pip" install --quiet --upgrade pip
"$PREFIX/.venv/bin/pip" install --quiet -r "$PREFIX/requirements.txt"
chown -R "$SERVICE_USER:$SERVICE_USER" "$PREFIX/.venv"

echo "==> configuration"
cat > "$ENV_DIR/mirror.env" <<EOF
SENTINEL_MIRROR_UPSTREAM=$UPSTREAM
SENTINEL_MIRROR_INTERVAL=$INTERVAL
SENTINEL_MIRROR_CACHE=$STATE_DIR/mirror-status.json
SENTINEL_MIRROR_HOST=$BIND
SENTINEL_MIRROR_PORT=$PORT
EOF
chmod 0640 "$ENV_DIR/mirror.env"
chown root:"$SERVICE_USER" "$ENV_DIR/mirror.env"

echo "==> service"
install -m 0644 "$REPO_ROOT/deploy/g7/sentinel-mirror.service" /etc/systemd/system/sentinel-mirror.service
systemctl daemon-reload
systemctl enable --now sentinel-mirror.service
systemctl restart sentinel-mirror.service
systemctl --no-pager --lines=10 status sentinel-mirror.service || true

echo "==> done"
