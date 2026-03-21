.PHONY: install test lint build up down logs migrate run-once clean

# ── 本地开发 ──────────────────────────────────────────────────────────────────

install:
	pip install -r requirements-dev.txt

test:
	python -m pytest ../pattern_scanner/tests/ -q --tb=short

test-cov:
	python -m pytest ../pattern_scanner/tests/ -q --tb=short --cov=pattern_scanner --cov-report=term-missing

lint:
	python -m py_compile $$(find . -name "*.py" ! -path "*/migrations/*")
	@echo "Syntax OK"

# ── 数据库（本地 SQLite）──────────────────────────────────────────────────────

db-init:
	python -c "\
import asyncio, os; os.chdir('..'); \
from pattern_scanner.database.session import init_db, create_tables; \
init_db('sqlite+aiosqlite:///./dev.db'); \
asyncio.run(create_tables()); \
print('DB initialized: dev.db')"

# ── Docker ───────────────────────────────────────────────────────────────────

build:
	docker compose build

up:
	docker compose up -d db
	docker compose run --rm migrate
	docker compose up -d scanner

down:
	docker compose down

logs:
	docker compose logs -f scanner

# 手动触发一次扫描（不启动调度器）
run-once:
	docker compose run --rm scanner python -m pattern_scanner.scheduler --run-once

# ── 清理 ─────────────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -f dev.db
