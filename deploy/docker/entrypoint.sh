#!/bin/bash
set -e

DB_URL="${PATTERN_SCANNER_DB_URL:-sqlite+aiosqlite:////app/data/pattern_scanner.db}"
echo "[entrypoint] DB: ${DB_URL%%\?*}"

# 等待数据库可用（最多 60 秒，每 3 秒重试）
python - <<'PYEOF'
import asyncio, os, sys, time

db_url = os.environ.get('PATTERN_SCANNER_DB_URL', '')

# MySQL：用 socket 探测端口，确保 TCP 可用
if 'mysql' in db_url:
    import socket, urllib.parse
    parsed = urllib.parse.urlparse(db_url.replace('+aiomysql', ''))
    host = parsed.hostname or 'db'
    port = parsed.port or 3306
    for attempt in range(20):
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f'[entrypoint] MySQL {host}:{port} ready')
                break
        except OSError:
            print(f'[entrypoint] Waiting for MySQL ({attempt+1}/20)...')
            time.sleep(3)
    else:
        print('[entrypoint] MySQL not reachable, giving up', file=sys.stderr)
        sys.exit(1)
PYEOF

# 初始化表结构
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
    print('[entrypoint] Database tables OK')
except Exception as e:
    print(f'[entrypoint] DB init failed: {e}', file=sys.stderr)
    sys.exit(1)
PYEOF

echo "[entrypoint] Starting Pattern Scanner..."
exec python -m pattern_scanner.scheduler "$@"
