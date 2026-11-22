"""SQLite database setup and lightweight row helpers."""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS service_accounts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL UNIQUE,
    namespace     TEXT NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    current_gen   INTEGER NOT NULL DEFAULT 0,
    created_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS token_generations (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    sa_id              INTEGER NOT NULL
        REFERENCES service_accounts(id),
    generation         INTEGER NOT NULL,
    token              TEXT NOT NULL UNIQUE,
    status             TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'superseded', 'revoked')),
    created_at         REAL NOT NULL,
    expires_at         REAL,
    superseded_at      REAL,
    revoked_at         REAL,
    revoke_reason      TEXT NOT NULL DEFAULT '',
    UNIQUE (sa_id, generation)
);

CREATE TABLE IF NOT EXISTS pods (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    sa_id               INTEGER NOT NULL
        REFERENCES service_accounts(id),
    name                TEXT NOT NULL,
    namespace           TEXT NOT NULL,
    alive               INTEGER NOT NULL DEFAULT 1,
    last_committed_gen  INTEGER NOT NULL DEFAULT 0,
    created_at          REAL NOT NULL,
    updated_at          REAL NOT NULL,
    UNIQUE (sa_id, namespace, name)
);

CREATE TABLE IF NOT EXISTS rotations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sa_id           INTEGER NOT NULL REFERENCES service_accounts(id),
    old_generation  INTEGER NOT NULL,
    new_generation  INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'completed', 'superseded', 'expired')),
    grace_seconds   INTEGER NOT NULL,
    created_at      REAL NOT NULL,
    completed_at    REAL,
    finished_reason TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS confirmations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    rotation_id   INTEGER NOT NULL REFERENCES rotations(id),
    pod_id        INTEGER NOT NULL REFERENCES pods(id),
    generation    INTEGER NOT NULL,
    confirmed_at  REAL NOT NULL,
    UNIQUE (rotation_id, pod_id)
);

CREATE INDEX IF NOT EXISTS idx_gens_sa ON token_generations(sa_id);
CREATE INDEX IF NOT EXISTS idx_pods_sa ON pods(sa_id);
CREATE INDEX IF NOT EXISTS idx_rot_sa ON rotations(sa_id);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path),
        timeout=30.0,
        check_same_thread=False,
        isolation_level=None,  # autocommit; transactions are explicit
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
