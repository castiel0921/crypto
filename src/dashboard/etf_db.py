from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any


class ETFDailyDB:
    """SQLite persistence for daily ETF inflow/outflow data from SoSoValue."""

    def __init__(self, db_path: str | Path) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS etf_daily (
                etf_type        TEXT NOT NULL,
                date            TEXT NOT NULL,
                total_net_inflow  REAL NOT NULL DEFAULT 0,
                total_value_traded REAL NOT NULL DEFAULT 0,
                total_net_assets  REAL NOT NULL DEFAULT 0,
                cum_net_inflow    REAL NOT NULL DEFAULT 0,
                updated_at      TEXT NOT NULL,
                PRIMARY KEY (etf_type, date)
            );
            CREATE INDEX IF NOT EXISTS idx_etf_daily_date ON etf_daily(date);
        """)

    def close(self) -> None:
        self._conn.close()

    def upsert_history(self, etf_type: str, records: list[dict[str, Any]]) -> int:
        """Bulk upsert ETF daily records. Returns number of rows upserted."""
        now = _utc_now()
        count = 0
        with self._conn:
            for r in records:
                self._conn.execute(
                    """
                    INSERT INTO etf_daily (etf_type, date, total_net_inflow,
                        total_value_traded, total_net_assets, cum_net_inflow, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(etf_type, date) DO UPDATE SET
                        total_net_inflow   = excluded.total_net_inflow,
                        total_value_traded = excluded.total_value_traded,
                        total_net_assets   = excluded.total_net_assets,
                        cum_net_inflow     = excluded.cum_net_inflow,
                        updated_at         = excluded.updated_at
                    """,
                    (
                        etf_type,
                        r["date"],
                        float(r.get("totalNetInflow", 0) or 0),
                        float(r.get("totalValueTraded", 0) or 0),
                        float(r.get("totalNetAssets", 0) or 0),
                        float(r.get("cumNetInflow", 0) or 0),
                        now,
                    ),
                )
                count += 1
        return count

    def get_history(self, etf_type: str) -> list[dict[str, Any]]:
        """Return all daily records for an ETF type, ordered by date asc."""
        rows = self._conn.execute(
            """SELECT date, total_net_inflow, total_value_traded,
                      total_net_assets, cum_net_inflow
               FROM etf_daily WHERE etf_type = ? ORDER BY date""",
            (etf_type,),
        ).fetchall()
        return [
            {
                "date": r[0],
                "totalNetInflow": r[1],
                "totalValueTraded": r[2],
                "totalNetAssets": r[3],
                "cumNetInflow": r[4],
            }
            for r in rows
        ]

    def get_latest_date(self, etf_type: str) -> str | None:
        """Return the most recent date for an ETF type, or None."""
        row = self._conn.execute(
            "SELECT MAX(date) FROM etf_daily WHERE etf_type = ?", (etf_type,)
        ).fetchone()
        return row[0] if row and row[0] else None

    def get_record_count(self, etf_type: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM etf_daily WHERE etf_type = ?", (etf_type,)
        ).fetchone()
        return row[0] if row else 0


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
