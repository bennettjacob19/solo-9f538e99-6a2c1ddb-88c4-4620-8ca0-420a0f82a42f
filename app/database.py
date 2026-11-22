"""SQLite database setup, schema and in-place migrations."""
from __future__ import annotations

import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# Schema
#
# ``pods`` carries roster metadata since roster imports were introduced:
#
# * ``uid``      – Kubernetes pod UID (NULL for manually registered pods)
# * ``source``   – ``manual`` or ``roster``
# * ``retired``  – soft-deleted members of an older roster
#
# An old database (created before roster imports) is migrated in place by
# ``_migrate()`` below; ids are preserved so confirmations keep pointing at
# the same pods.
# ---------------------------------------------------------------------------

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS service_accounts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT NOT NULL UNIQUE,
    namespace         TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    current_gen       INTEGER NOT NULL DEFAULT 0,
    active_roster_id  INTEGER,
    created_at        REAL NOT NULL
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
    uid                 TEXT,
    source              TEXT NOT NULL DEFAULT 'manual'
        CHECK (source IN ('manual', 'roster')),
    retired             INTEGER NOT NULL DEFAULT 0,
    retired_reason      TEXT NOT NULL DEFAULT '',
    alive               INTEGER NOT NULL DEFAULT 1,
    last_committed_gen  INTEGER NOT NULL DEFAULT 0,
    created_at          REAL NOT NULL,
    updated_at          REAL NOT NULL
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

-- One import round (a paged PodList snapshot) for a service account.
CREATE TABLE IF NOT EXISTS roster_imports (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    sa_id             INTEGER NOT NULL REFERENCES service_accounts(id),
    namespace         TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'collecting'
        CHECK (status IN ('collecting', 'active', 'superseded')),
    resource_version  TEXT,
    next_continue     TEXT NOT NULL DEFAULT '',
    page_count        INTEGER NOT NULL DEFAULT 0,
    pod_count         INTEGER NOT NULL DEFAULT 0,
    created_at        REAL NOT NULL,
    activated_at      REAL,
    finished_reason   TEXT NOT NULL DEFAULT ''
);

-- Raw pages already accepted for an import round.
CREATE TABLE IF NOT EXISTS roster_pages (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    import_id         INTEGER NOT NULL REFERENCES roster_imports(id),
    page_index        INTEGER NOT NULL,
    continue_token    TEXT NOT NULL,
    next_continue     TEXT NOT NULL,
    resource_version  TEXT NOT NULL,
    item_count        INTEGER NOT NULL,
    items_json        TEXT NOT NULL,
    received_at       REAL NOT NULL,
    UNIQUE (import_id, page_index)
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_gens_sa ON token_generations(sa_id);
CREATE INDEX IF NOT EXISTS idx_pods_sa ON pods(sa_id);
CREATE INDEX IF NOT EXISTS idx_rot_sa ON rotations(sa_id);
CREATE INDEX IF NOT EXISTS idx_roster_imports_sa ON roster_imports(sa_id);
CREATE INDEX IF NOT EXISTS idx_roster_pages_import ON roster_pages(import_id);

-- A service account has at most one non-retired pod per (namespace, name),
-- and every non-retired Kubernetes uid appears exactly once.
CREATE UNIQUE INDEX IF NOT EXISTS uq_pods_active_name
    ON pods(sa_id, namespace, name) WHERE retired = 0;
CREATE UNIQUE INDEX IF NOT EXISTS uq_pods_active_uid
    ON pods(sa_id, uid) WHERE uid IS NOT NULL AND retired = 0;
CREATE INDEX IF NOT EXISTS idx_pods_uid ON pods(uid);
"""

# Used both for a fresh database and when rebuilding a pre-roster ``pods``
# table. Kept separate so the migration can create ``pods_new`` and rename it.
PODS_DDL = """
CREATE TABLE {table} (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    sa_id               INTEGER NOT NULL
        REFERENCES service_accounts(id),
    name                TEXT NOT NULL,
    namespace           TEXT NOT NULL,
    uid                 TEXT,
    source              TEXT NOT NULL DEFAULT 'manual'
        CHECK (source IN ('manual', 'roster')),
    retired             INTEGER NOT NULL DEFAULT 0,
    retired_reason      TEXT NOT NULL DEFAULT '',
    alive               INTEGER NOT NULL DEFAULT 1,
    last_committed_gen  INTEGER NOT NULL DEFAULT 0,
    created_at          REAL NOT NULL,
    updated_at          REAL NOT NULL
)
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


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _rebuild_pods(conn: sqlite3.Connection, old_columns: set[str]) -> None:
    """Rebuild a pre-roster ``pods`` table without losing ids or rows.

    The old table had ``UNIQUE (sa_id, namespace, name)``. Roster imports need
    historical (retired) rows and a replaced pod with the same name but a new
    uid, so the constraint is replaced by partial unique indexes in INDEXES.
    """
    uid_expr = "uid" if "uid" in old_columns else "NULL"
    source_expr = "source" if "source" in old_columns else "'manual'"
    retired_expr = "retired" if "retired" in old_columns else "0"
    retired_reason_expr = (
        "retired_reason" if "retired_reason" in old_columns else "''"
    )

    # PRAGMA foreign_keys cannot be changed inside a transaction. With it off,
    # dropping/renaming pods does not cascade into confirmations; their FK is
    # by table name and still points at the new ``pods`` table afterwards.
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DROP TABLE IF EXISTS pods_new")
        conn.execute(PODS_DDL.format(table="pods_new"))
        conn.execute(
            "INSERT INTO pods_new(id, sa_id, name, namespace, uid, source, "
            "retired, retired_reason, alive, last_committed_gen, created_at, "
            f"updated_at) SELECT id, sa_id, name, namespace, {uid_expr}, "
            f"{source_expr}, {retired_expr}, {retired_reason_expr}, alive, "
            "last_committed_gen, created_at, updated_at FROM pods"
        )
        conn.execute("DROP TABLE pods")
        conn.execute("ALTER TABLE pods_new RENAME TO pods")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _migrate(conn: sqlite3.Connection) -> None:
    """Bring an existing database created by an older release up to date."""
    pod_columns = _columns(conn, "pods")
    if pod_columns and not {"uid", "source", "retired"} <= pod_columns:
        _rebuild_pods(conn, pod_columns)
    elif pod_columns and "retired_reason" not in pod_columns:
        conn.execute(
            "ALTER TABLE pods ADD COLUMN retired_reason TEXT NOT NULL DEFAULT ''"
        )

    sa_columns = _columns(conn, "service_accounts")
    if sa_columns and "active_roster_id" not in sa_columns:
        conn.execute(
            "ALTER TABLE service_accounts ADD COLUMN active_roster_id INTEGER"
        )


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    _migrate(conn)
    conn.executescript(INDEXES)
