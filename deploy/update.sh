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

echo "[deploy] installing dependencies"
"$VENV_PIP" install -r "$ROOT_DIR/requirements.txt"

if [[ ${#SERVICES[@]} -eq 0 ]]; then
  echo "[deploy] skipping service restart"
  exit 0
fi

for svc in "${SERVICES[@]}"; do
  echo "[deploy] restarting $svc"
  sudo -n systemctl restart "$svc"
  sudo -n systemctl --no-pager --full status "$svc"
done
