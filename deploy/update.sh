#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: deploy/update.sh [service-name]

Pull the latest code from GitHub, sync Python dependencies, and optionally
restart a systemd service.

Arguments:
  service-name   systemd service to restart after update
                 default: crypto-okx-rest
                 use "none" to skip restart

Environment:
  REMOTE         git remote name, default: origin
  BRANCH         git branch to deploy, default: main
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

SERVICE_NAME="${1:-crypto-okx-rest}"
REMOTE="${REMOTE:-origin}"
BRANCH="${BRANCH:-main}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="$ROOT_DIR/.venv/bin/python"
VENV_PIP="$ROOT_DIR/.venv/bin/pip"

echo "[deploy] repo=$ROOT_DIR remote=$REMOTE branch=$BRANCH service=$SERVICE_NAME"

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

if [[ "$SERVICE_NAME" == "none" ]]; then
  echo "[deploy] skipping service restart"
  exit 0
fi

echo "[deploy] restarting $SERVICE_NAME"
sudo -n systemctl restart "$SERVICE_NAME"
sudo -n systemctl --no-pager --full status "$SERVICE_NAME"
