#!/bin/bash
set -e

DB_URL="${PATTERN_SCANNER_DB_URL:-sqlite+aiosqlite:////app/data/pattern_scanner.db}"

echo "[entrypoint] Initialising database: ${DB_URL%%\?*}"
python - <<'PYEOF'
import asyncio, os, sys
from pattern_scanner.database.session import init_db, create_tables

db_url = os.environ.get(
    'PATTERN_SCANNER_DB_URL',
    'sqlite+aiosqlite:////app/data/pattern_scanner.db',
)
try:
    init_db(db_url)
    asyncio.run(create_tables())
    print('[entrypoint] Database ready')
except Exception as e:
    print(f'[entrypoint] DB init failed: {e}', file=sys.stderr)
    sys.exit(1)
PYEOF

echo "[entrypoint] Starting Pattern Scanner..."
exec python -m pattern_scanner.scheduler "$@"
