#!/usr/bin/env bash
# Installs pgsync (Rebecca -> PasarGuard sync) as a systemd service.
#   bash install.sh            install or update, run setup on first install
#   bash install.sh setup      re-enter credentials
#   bash install.sh uninstall  stop and remove the service (keeps settings + database)
set -euo pipefail

REPO="${PGSYNC_REPO:-ali-ar1/Rebecca}"
TAG="${PGSYNC_TAG:-pgsync}"
DIR="/opt/pgsync"
BIN="$DIR/pgsync"
ENV_FILE="$DIR/.env"
SERVICE="/etc/systemd/system/pgsync.service"

[ "$(id -u)" = "0" ] || { echo "Run as root."; exit 1; }

case "$(uname -m)" in
    x86_64|amd64) ARCH=amd64 ;;
    aarch64|arm64) ARCH=arm64 ;;
    *) echo "Unsupported CPU: $(uname -m)"; exit 1 ;;
esac

if [ "${1:-}" = "uninstall" ]; then
    systemctl disable --now pgsync >/dev/null 2>&1 || true
    rm -f "$SERVICE" && systemctl daemon-reload
    echo "pgsync service removed. Settings kept in $ENV_FILE (database untouched)."
    exit 0
fi

if [ "${1:-}" = "setup" ]; then
    systemctl stop pgsync >/dev/null 2>&1 || true
    "$BIN" -env "$ENV_FILE" setup
    systemctl start pgsync
    echo "pgsync restarted. Logs: journalctl -u pgsync -f"
    exit 0
fi

mkdir -p "$DIR"
chmod 700 "$DIR"
URL="https://github.com/$REPO/releases/download/$TAG/pgsync-linux-$ARCH"
echo "Downloading $URL"
curl -fL --retry 3 -o "$BIN.new" "$URL"
chmod 755 "$BIN.new"
systemctl stop pgsync >/dev/null 2>&1 || true
mv -f "$BIN.new" "$BIN"

if [ ! -s "$ENV_FILE" ]; then
    "$BIN" -env "$ENV_FILE" setup
fi

cat > "$SERVICE" <<EOF
[Unit]
Description=pgsync - Rebecca to PasarGuard user and usage sync
After=network-online.target mysql.service mariadb.service rebecca.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=$BIN -env $ENV_FILE run
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now pgsync
echo
echo "pgsync is running."
echo "  Logs:            journalctl -u pgsync -f"
echo "  Test (no change): $BIN -env $ENV_FILE dry-run"
echo "  Change settings: bash install.sh setup"
