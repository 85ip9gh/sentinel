#!/usr/bin/env bash
# Install the Sentinel collector on a Linux host as a systemd service.
#
#   sudo deploy/g7/install.sh --bootstrap 100.125.224.124:9094 --role server
#
# Idempotent: re-running updates the code and the environment file, then
# restarts the service.
set -euo pipefail

BOOTSTRAP=""
ROLE="server"
HOST_NAME="$(hostname)"
HTTP_TARGETS=""
PREFIX="/opt/sentinel"
STATE_DIR="/var/lib/sentinel"
ENV_DIR="/etc/sentinel"
SERVICE_USER="sentinel"

usage() {
  sed -n '2,10p' "$0"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap) BOOTSTRAP="$2"; shift 2 ;;
    --role) ROLE="$2"; shift 2 ;;
    --host) HOST_NAME="$2"; shift 2 ;;
    --http-targets) HTTP_TARGETS="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

[[ -n "$BOOTSTRAP" ]] || { echo "--bootstrap is required" >&2; exit 1; }
[[ $EUID -eq 0 ]] || { echo "run with sudo" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> user and directories"
id -u "$SERVICE_USER" >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" "$PREFIX" "$STATE_DIR" "$STATE_DIR/spool"
install -d -m 0750 "$ENV_DIR"

echo "==> code"
rm -rf "$PREFIX/collector"
cp -r "$REPO_ROOT/collector" "$PREFIX/collector"
cp "$REPO_ROOT/requirements.txt" "$PREFIX/requirements.txt"
chown -R "$SERVICE_USER:$SERVICE_USER" "$PREFIX/collector" "$PREFIX/requirements.txt"

echo "==> virtualenv"
if [[ ! -x "$PREFIX/.venv/bin/python" ]]; then
  python3 -m venv "$PREFIX/.venv"
fi
"$PREFIX/.venv/bin/pip" install --quiet --upgrade pip
"$PREFIX/.venv/bin/pip" install --quiet -r "$PREFIX/requirements.txt"
chown -R "$SERVICE_USER:$SERVICE_USER" "$PREFIX/.venv"

echo "==> configuration"
cat > "$ENV_DIR/collector.env" <<EOF
SENTINEL_BOOTSTRAP=$BOOTSTRAP
SENTINEL_HOST=$HOST_NAME
SENTINEL_ROLE=$ROLE
SENTINEL_SPOOL_DIR=$STATE_DIR/spool
SENTINEL_HTTP_TARGETS=$HTTP_TARGETS
EOF
chmod 0640 "$ENV_DIR/collector.env"
chown root:"$SERVICE_USER" "$ENV_DIR/collector.env"

echo "==> service"
install -m 0644 "$REPO_ROOT/deploy/g7/sentinel-collector.service" /etc/systemd/system/sentinel-collector.service
systemctl daemon-reload
systemctl enable --now sentinel-collector.service
systemctl --no-pager --lines=10 status sentinel-collector.service || true

echo "==> done"
