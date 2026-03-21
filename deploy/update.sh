#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: deploy/update.sh [service-name ...]

Pull the latest code from GitHub, sync Python dependencies, and optionally
restart one or more systemd services.

Arguments:
  service-name   systemd service(s) to restart after update (space-separated)
                 default: crypto-cross-spread
                 use "none" to skip restart
                 use "all" to restart all crypto-* services found in deploy/systemd

Environment:
  REMOTE         git remote name, default: origin
  BRANCH         git branch to deploy, default: main
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

REMOTE="${REMOTE:-origin}"
BRANCH="${BRANCH:-main}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="$ROOT_DIR/.venv/bin/python"
VENV_PIP="$ROOT_DIR/.venv/bin/pip"

# Collect service names
SERVICES=()
if [[ $# -eq 0 ]]; then
  SERVICES=("crypto-cross-spread")
elif [[ "$1" == "all" ]]; then
  for f in "$ROOT_DIR"/deploy/systemd/crypto-*.service; do
    SERVICES+=("$(basename "${f%.service}")")
  done
elif [[ "$1" != "none" ]]; then
  SERVICES=("$@")
fi

echo "[deploy] repo=$ROOT_DIR remote=$REMOTE branch=$BRANCH services=${SERVICES[*]:-none}"

cd "$ROOT_DIR"

git fetch "$REMOTE" "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only "$REMOTE" "$BRANCH"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "[deploy] creating virtualenv"
  python3 -m venv "$ROOT_DIR/.venv"
fi

REQ_HASH=$(md5sum "$ROOT_DIR/requirements.txt" | awk '{print $1}')
HASH_FILE="$ROOT_DIR/.req_install_hash"
if [[ -f "$HASH_FILE" ]] && [[ "$(cat "$HASH_FILE")" == "$REQ_HASH" ]]; then
  echo "[deploy] requirements.txt unchanged, skipping pip install"
else
  echo "[deploy] installing dependencies"
  "$VENV_PIP" install --quiet --prefer-binary -r "$ROOT_DIR/requirements.txt"
  echo "$REQ_HASH" > "$HASH_FILE"
fi

if [[ ${#SERVICES[@]} -eq 0 ]]; then
  echo "[deploy] skipping service restart"
  exit 0
fi

# Sync service files and reload systemd
echo "[deploy] syncing systemd service files"
for svc in "${SERVICES[@]}"; do
  src_file="$ROOT_DIR/deploy/systemd/${svc}.service"
  if [[ -f "$src_file" ]]; then
    sudo -n cp "$src_file" /etc/systemd/system/
  fi
done
sudo -n systemctl daemon-reload

for svc in "${SERVICES[@]}"; do
  echo "[deploy] restarting $svc"
  sudo -n systemctl restart "$svc"
  sudo -n systemctl --no-pager --full status "$svc"
done
