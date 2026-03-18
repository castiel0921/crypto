from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any


class OIDailyDB:
    """SQLite persistence for daily open interest data."""

    def __init__(self, db_path: str | Path) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS oi_daily (
                symbol     TEXT NOT NULL,
                date       TEXT NOT NULL,
                binance_oi REAL NOT NULL DEFAULT 0,
                okx_oi     REAL NOT NULL DEFAULT 0,
                total_oi   REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, date)
            );
            CREATE INDEX IF NOT EXISTS idx_oi_daily_date ON oi_daily(date);
        """)

    def close(self) -> None:
        self._conn.close()

    def upsert_binance_history(self, symbol: str, points: list[dict[str, Any]]) -> None:
        """Upsert Binance daily history. Preserves existing okx_oi values."""
        now = _utc_now()
        with self._conn:
            for p in points:
                date = _to_date(p["t"])
                val = float(p["v"])
                self._conn.execute(
                    """
                    INSERT INTO oi_daily (symbol, date, binance_oi, okx_oi, total_oi, updated_at)
                    VALUES (?, ?, ?, 0, ?, ?)
                    ON CONFLICT(symbol, date) DO UPDATE SET
                        binance_oi = excluded.binance_oi,
                        total_oi   = excluded.binance_oi + oi_daily.okx_oi,
                        updated_at = excluded.updated_at
                    """,
                    (symbol, date, val, val, now),
                )

    def upsert_realtime_snapshot(self, rows: list[dict[str, Any]]) -> None:
        """Save today's combined OI from real-time poll (both exchanges)."""
        today = time.strftime("%Y-%m-%d", time.gmtime())
        now = _utc_now()
        with self._conn:
            for row in rows:
                bn = float(row.get("binanceOI", 0))
                okx = float(row.get("okxOI", 0))
                total = bn + okx
                self._conn.execute(
                    """
                    INSERT INTO oi_daily (symbol, date, binance_oi, okx_oi, total_oi, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, date) DO UPDATE SET
                        binance_oi = excluded.binance_oi,
                        okx_oi     = excluded.okx_oi,
                        total_oi   = excluded.total_oi,
                        updated_at = excluded.updated_at
                    """,
                    (row["symbol"], today, bn, okx, total, now),
                )

    def upsert_okx_history(self, symbol: str, points: list[dict[str, Any]]) -> None:
        """Upsert OKX daily history. Preserves existing binance_oi values."""
        now = _utc_now()
        with self._conn:
            for p in points:
                date = _to_date(p["t"])
                val = float(p["v"])
                self._conn.execute(
                    """
                    INSERT INTO oi_daily (symbol, date, binance_oi, okx_oi, total_oi, updated_at)
                    VALUES (?, ?, 0, ?, ?, ?)
                    ON CONFLICT(symbol, date) DO UPDATE SET
                        okx_oi     = excluded.okx_oi,
                        total_oi   = oi_daily.binance_oi + excluded.okx_oi,
                        updated_at = excluded.updated_at
                    """,
                    (symbol, date, val, val, now),
                )

    def get_history(
        self, symbols: list[str] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Return daily OI grouped by symbol: {symbol: [{t, v, bn, okx}, ...]}."""
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            sql = f"SELECT symbol, date, total_oi, binance_oi, okx_oi FROM oi_daily WHERE symbol IN ({placeholders}) ORDER BY date"
            rows = self._conn.execute(sql, symbols).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT symbol, date, total_oi, binance_oi, okx_oi FROM oi_daily ORDER BY date"
            ).fetchall()

        result: dict[str, list[dict[str, Any]]] = {}
        for symbol, date, total_oi, binance_oi, okx_oi in rows:
            if symbol not in result:
                result[symbol] = []
            result[symbol].append({"t": date, "v": total_oi, "bn": binance_oi, "okx": okx_oi})
        return result

    def get_latest_date(self, symbol: str) -> str | None:
        """Return the most recent date for a symbol, or None."""
        row = self._conn.execute(
            "SELECT MAX(date) FROM oi_daily WHERE symbol = ?", (symbol,)
        ).fetchone()
        return row[0] if row and row[0] else None


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _to_date(t: str) -> str:
    """Normalize timestamp to YYYY-MM-DD."""
    return t[:10]
