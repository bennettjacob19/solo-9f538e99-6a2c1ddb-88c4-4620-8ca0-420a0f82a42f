"""Domain services: registration, rotation, confirmation, verification, sweep.

Concurrency model
-----------------
Every mutating operation acquires one process-wide ``threading.RLock`` and
opens a ``BEGIN IMMEDIATE`` transaction, so rotations, restarts and
confirmations are serialized. Combined with SQLite WAL this is safe for the
FastAPI threadpool (sync ``sqlite3`` calls run in worker threads).

Safety invariants
-----------------
1. The old generation is only revoked after *every alive pod* has confirmed
   mounting the new generation.
2. During the grace period the superseded generation still validates; once
   ``expires_at`` has passed it is rejected immediately.
3. A stale confirmation (for a generation older than the pod's last committed
   one, or for an already finished rotation) is rejected and never revokes any
   token — in particular it can never revoke a newer generation.
4. After failure / restart a pod keeps using its last committed generation.
"""
from __future__ import annotations

import secrets
import sqlite3
import threading
import time
from typing import Optional

from .errors import ConflictError, NotFoundError

TOKEN_PREFIX = "k8s-sat-"


def _now() -> float:
    # Indirection so tests can monkeypatch time deterministically.
    return time.time()


def _new_token() -> str:
    return TOKEN_PREFIX + secrets.token_urlsafe(32)


class RotationService:
    def __init__(self, conn: sqlite3.Connection, now=_now):
        self._conn = conn
        self._now = now
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ utils

    def _get_sa(self, sa_id: int) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT * FROM service_accounts WHERE id = ?", (sa_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"service account id={sa_id} not found")
        return row

    def _get_sa_by_name(self, name: str, namespace: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM service_accounts WHERE name = ? AND namespace = ?",
            (name, namespace),
        ).fetchone()

    def _get_pod(self, pod_id: int) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT * FROM pods WHERE id = ?", (pod_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"pod id={pod_id} not found")
        return row

    def _get_generation(self, sa_id: int, generation: int) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM token_generations WHERE sa_id = ? AND generation = ?",
            (sa_id, generation),
        ).fetchone()

    @staticmethod
    def _pod_out(row: sqlite3.Row) -> dict:
        return {
            "id": row["id"],
            "name": row["name"],
            "namespace": row["namespace"],
            "alive": bool(row["alive"]),
            "last_committed_gen": row["last_committed_gen"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _gen_out(row: sqlite3.Row) -> dict:
        return {
            "generation": row["generation"],
            "token": row["token"],
            "status": row["status"],
            "created_at": row["created_at"],
            "expires_at": row["expires_at"],
            "superseded_at": row["superseded_at"],
            "revoked_at": row["revoked_at"],
            "revoke_reason": row["revoke_reason"],
        }

    # ------------------------------------------------------------- registration

    def register_sa(self, name: str, namespace: str, description: str) -> dict:
        with self._lock:
            if self._get_sa_by_name(name, namespace) is not None:
                raise ConflictError(
                    f"service account {namespace}/{name} already registered"
                )
            now = self._now()
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                cur = self._conn.execute(
                    "INSERT INTO service_accounts(name, namespace, description, "
                    "current_gen, created_at) VALUES (?, ?, ?, 0, ?)",
                    (name, namespace, description, now),
                )
                sa_id = cur.lastrowid
                # Generation 1 is issued immediately so the SA has a usable token.
                self._conn.execute(
                    "INSERT INTO token_generations(sa_id, generation, token, "
                    "status, created_at) VALUES (?, 1, ?, 'active', ?)",
                    (sa_id, _new_token(), now),
                )
                self._conn.execute(
                    "UPDATE service_accounts SET current_gen = 1 WHERE id = ?",
                    (sa_id,),
                )
                self._conn.execute("COMMIT")
            except sqlite3.IntegrityError:
                self._conn.execute("ROLLBACK")
                raise ConflictError(
                    f"service account {namespace}/{name} already registered"
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return self.get_sa(sa_id)

    def register_pod(self, sa_id: int, name: str, namespace: str) -> dict:
        with self._lock:
            sa = self._get_sa(sa_id)
            existing = self._conn.execute(
                "SELECT * FROM pods WHERE sa_id = ? AND namespace = ? AND name = ?",
                (sa_id, namespace, name),
            ).fetchone()
            if existing is not None:
                raise ConflictError(
                    f"pod {namespace}/{name} already registered on service "
                    f"account {sa['namespace']}/{sa['name']}"
                )
            now = self._now()
            # A freshly registered pod mounts the SA's current generation.
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                cur = self._conn.execute(
                    "INSERT INTO pods(sa_id, name, namespace, alive, "
                    "last_committed_gen, created_at, updated_at) "
                    "VALUES (?, ?, ?, 1, ?, ?, ?)",
                    (sa_id, name, namespace, sa["current_gen"], now, now),
                )
                self._conn.execute("COMMIT")
            except sqlite3.IntegrityError:
                self._conn.execute("ROLLBACK")
                raise ConflictError(
                    f"pod {namespace}/{name} already registered on service "
                    f"account {sa['namespace']}/{sa['name']}"
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return self._pod_out(self._get_pod(cur.lastrowid))

    def set_pod_alive(self, pod_id: int, alive: bool) -> dict:
        with self._lock:
            self._get_pod(pod_id)
            now = self._now()
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute(
                    "UPDATE pods SET alive = ?, updated_at = ? WHERE id = ?",
                    (1 if alive else 0, now, pod_id),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return self._pod_out(self._get_pod(pod_id))

    def restart_pod(self, pod_id: int) -> dict:
        """Simulate a restart / crash-recovery.

        The pod re-mounts the last generation it had successfully committed
        (never an uncommitted newer one). Its confirmation of a pending
        rotation is cleared implicitly: status of that rotation recomputes
        from alive pods + confirmations on every read, so the pod shows up as
        pending again and must re-confirm.
        """
        with self._lock:
            pod = self._get_pod(pod_id)
            now = self._now()
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute(
                    "UPDATE pods SET alive = 1, updated_at = ? WHERE id = ?",
                    (now, pod_id),
                )
                self._conn.execute(
                    "DELETE FROM confirmations WHERE pod_id = ?", (pod_id,)
                )
                gen = self._get_generation(
                    pod["sa_id"], pod["last_committed_gen"]
                )
                if gen is None:  # defensive: cannot happen with intact history
                    raise ConflictError(
                        "last committed generation no longer exists; cannot restart"
                    )
                pending = self._conn.execute(
                    "SELECT id FROM rotations WHERE sa_id = ? AND status = 'pending' "
                    "ORDER BY id DESC LIMIT 1",
                    (pod["sa_id"],),
                ).fetchone()
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return {
                "pod": self._pod_out(self._get_pod(pod_id)),
                "reused_generation": pod["last_committed_gen"],
                "token": gen["token"],
                "needs_confirmation_for_rotation": pending["id"]
                if pending
                else None,
            }

    # ------------------------------------------------------------------ rotate

    def start_rotation(self, sa_id: int, grace_seconds: int) -> dict:
        with self._lock:
            now = self._now()
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                # Read inside the transaction so a concurrent rotation cannot
                # interleave between the read and the write.
                sa = self._get_sa(sa_id)
                old_gen_no = sa["current_gen"]
                old = self._get_generation(sa_id, old_gen_no)
                if old is None or old["status"] != "active":
                    raise ConflictError(
                        "current generation is not active; rotation already in flight"
                    )
                new_gen_no = old_gen_no + 1

                # Any earlier pending rotation is overtaken by this one and can
                # never complete or revoke anything on its own.
                self._conn.execute(
                    "UPDATE rotations SET status = 'superseded', "
                    "finished_reason = 'superseded by a newer rotation' "
                    "WHERE sa_id = ? AND status = 'pending'",
                    (sa_id,),
                )

                cur = self._conn.execute(
                    "INSERT INTO rotations(sa_id, old_generation, new_generation, "
                    "status, grace_seconds, created_at) VALUES (?, ?, ?, 'pending', ?, ?)",
                    (sa_id, old_gen_no, new_gen_no, grace_seconds, now),
                )
                rotation_id = cur.lastrowid

                self._conn.execute(
                    "INSERT INTO token_generations(sa_id, generation, token, "
                    "status, created_at) VALUES (?, ?, ?, 'active', ?)",
                    (sa_id, new_gen_no, _new_token(), now),
                )
                # Old generation stays valid throughout the grace window.
                self._conn.execute(
                    "UPDATE token_generations SET status = 'superseded', "
                    "superseded_at = ?, expires_at = ? WHERE id = ?",
                    (now, now + grace_seconds, old["id"]),
                )
                self._conn.execute(
                    "UPDATE service_accounts SET current_gen = ? WHERE id = ?",
                    (new_gen_no, sa_id),
                )
                # No alive pods: nothing needs to load the new generation, so
                # the rotation completes (and old gen is revoked) immediately.
                self._maybe_complete_rotation(rotation_id, now)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return self.get_rotation(rotation_id)

    def confirm(self, rotation_id: int, pod_id: int, generation: int) -> dict:
        with self._lock:
            now = self._now()
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                rotation = self._conn.execute(
                    "SELECT * FROM rotations WHERE id = ?", (rotation_id,)
                ).fetchone()
                if rotation is None:
                    raise NotFoundError(f"rotation id={rotation_id} not found")
                pod = self._get_pod(pod_id)

                # ---- stale / invalid confirmations are rejected, no state change
                if pod["sa_id"] != rotation["sa_id"]:
                    raise ConflictError(
                        "pod does not belong to this service account"
                    )
                if not pod["alive"]:
                    raise ConflictError(
                        "pod is not alive; revive it before confirming"
                    )
                if rotation["status"] != "pending":
                    raise ConflictError(
                        f"rotation already {rotation['status']}; "
                        "late confirmation rejected"
                    )
                if generation != rotation["new_generation"]:
                    raise ConflictError(
                        f"stale generation: rotation expects generation "
                        f"{rotation['new_generation']}, pod reported {generation}"
                    )
                if generation < pod["last_committed_gen"]:
                    # Hard guarantee: a late confirmation can never move a pod
                    # backwards and can never revoke a newer token.
                    raise ConflictError(
                        "reported generation is older than the pod's last "
                        "committed generation; stale confirmation rejected"
                    )
                # Note: jumping forward (e.g. skipping an expired generation) is
                # allowed; equality with the rotation's new generation was
                # already enforced above.

                self._conn.execute(
                    "INSERT INTO confirmations(rotation_id, pod_id, generation, "
                    "confirmed_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(rotation_id, pod_id) DO UPDATE SET "
                    "generation = excluded.generation, "
                    "confirmed_at = excluded.confirmed_at",
                    (rotation_id, pod_id, generation, now),
                )
                # A pod only ever moves forward.
                if generation > pod["last_committed_gen"]:
                    self._conn.execute(
                        "UPDATE pods SET last_committed_gen = ?, updated_at = ? "
                        "WHERE id = ?",
                        (generation, now, pod_id),
                    )

                completed = self._maybe_complete_rotation(rotation_id, now)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            result = self.get_rotation(rotation_id)
            if completed:
                result["finished_reason"] = (
                    "all alive pods confirmed; old generation revoked"
                )
            return result

    def _maybe_complete_rotation(self, rotation_id: int, now: float) -> bool:
        """Revoke the old generation only when every alive pod confirmed."""
        rotation = self._conn.execute(
            "SELECT * FROM rotations WHERE id = ?", (rotation_id,)
        ).fetchone()
        if rotation["status"] != "pending":
            return False

        alive_pods = self._conn.execute(
            "SELECT id FROM pods WHERE sa_id = ? AND alive = 1",
            (rotation["sa_id"],),
        ).fetchall()
        confirmed = self._conn.execute(
            "SELECT pod_id FROM confirmations WHERE rotation_id = ?", (rotation_id,)
        ).fetchall()
        confirmed_ids = {r["pod_id"] for r in confirmed}

        if not all(p["id"] in confirmed_ids for p in alive_pods):
            return False

        # All alive pods have the new generation mounted. Retire the old one.
        self._conn.execute(
            "UPDATE token_generations SET status = 'revoked', revoked_at = ?, "
            "revoke_reason = 'rotation completed: all alive pods confirmed' "
            "WHERE sa_id = ? AND generation = ? AND status != 'revoked' "
            "AND status != 'active'",
            (now, rotation["sa_id"], rotation["old_generation"]),
        )
        self._conn.execute(
            "UPDATE rotations SET status = 'completed', completed_at = ?, "
            "finished_reason = 'all alive pods confirmed; old generation revoked' "
            "WHERE id = ?",
            (now, rotation_id),
        )
        return True

    # ------------------------------------------------------------------ verify

    def verify_token(self, token: str) -> dict:
        with self._lock:
            now = self._now()
            row = self._conn.execute(
                "SELECT g.*, s.name AS sa_name, s.namespace AS sa_namespace "
                "FROM token_generations g JOIN service_accounts s "
                "ON s.id = g.sa_id WHERE g.token = ?",
                (token,),
            ).fetchone()
            if row is None:
                return {"valid": False, "reason": "unknown token"}

            if row["status"] == "revoked":
                return {
                    "valid": False,
                    "reason": "token generation has been revoked",
                    "service_account": row["sa_name"],
                    "namespace": row["sa_namespace"],
                    "generation": row["generation"],
                }
            if row["status"] == "superseded":
                if row["expires_at"] is not None and now >= row["expires_at"]:
                    # Expired immediately rejected; lazily mark it revoked.
                    self._expire_generation(row, now)
                    return {
                        "valid": False,
                        "reason": "grace period elapsed; old generation expired",
                        "service_account": row["sa_name"],
                        "namespace": row["sa_namespace"],
                        "generation": row["generation"],
                    }
                return {
                    "valid": True,
                    "reason": "valid (superseded generation within grace period)",
                    "service_account": row["sa_name"],
                    "namespace": row["sa_namespace"],
                    "generation": row["generation"],
                }
            return {
                "valid": True,
                "reason": "valid",
                "service_account": row["sa_name"],
                "namespace": row["sa_namespace"],
                "generation": row["generation"],
            }

    def _expire_generation(self, row: sqlite3.Row, now: float) -> None:
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                "UPDATE token_generations SET status = 'revoked', revoked_at = ?, "
                "revoke_reason = 'grace period elapsed' WHERE id = ? AND status = 'superseded'",
                (now, row["id"]),
            )
            # The rotation that produced this generation cannot complete now.
            self._conn.execute(
                "UPDATE rotations SET status = 'expired', finished_reason = "
                "'grace period elapsed before all pods confirmed' "
                "WHERE sa_id = ? AND old_generation = ? AND status = 'pending'",
                (row["sa_id"], row["generation"]),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------- sweep

    def sweep_expired(self) -> int:
        """Background maintenance: expire superseded generations past grace."""
        with self._lock:
            now = self._now()
            rows = self._conn.execute(
                "SELECT * FROM token_generations WHERE status = 'superseded' "
                "AND expires_at IS NOT NULL AND expires_at <= ?",
                (now,),
            ).fetchall()
            for row in rows:
                self._expire_generation(row, now)
            return len(rows)

    # -------------------------------------------------------------------- reads

    def list_sas(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM service_accounts ORDER BY id"
        ).fetchall()
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "namespace": r["namespace"],
                "description": r["description"],
                "current_gen": r["current_gen"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def get_sa(self, sa_id: int) -> dict:
        sa = self._get_sa(sa_id)
        gens = self._conn.execute(
            "SELECT * FROM token_generations WHERE sa_id = ? ORDER BY generation",
            (sa_id,),
        ).fetchall()
        pods = self._conn.execute(
            "SELECT * FROM pods WHERE sa_id = ? ORDER BY id", (sa_id,)
        ).fetchall()
        return {
            "id": sa["id"],
            "name": sa["name"],
            "namespace": sa["namespace"],
            "description": sa["description"],
            "current_gen": sa["current_gen"],
            "created_at": sa["created_at"],
            "generations": [self._gen_out(g) for g in gens],
            "pods": [self._pod_out(p) for p in pods],
        }

    def get_rotation(self, rotation_id: int) -> dict:
        rotation = self._conn.execute(
            "SELECT * FROM rotations WHERE id = ?", (rotation_id,)
        ).fetchone()
        if rotation is None:
            raise NotFoundError(f"rotation id={rotation_id} not found")

        confirmed_rows = self._conn.execute(
            "SELECT p.* FROM confirmations c JOIN pods p ON p.id = c.pod_id "
            "WHERE c.rotation_id = ? ORDER BY p.id",
            (rotation_id,),
        ).fetchall()
        confirmed_ids = {r["id"] for r in confirmed_rows}
        pending_rows = self._conn.execute(
            "SELECT * FROM pods WHERE sa_id = ? AND alive = 1 ORDER BY id",
            (rotation["sa_id"],),
        ).fetchall()
        pending_rows = [p for p in pending_rows if p["id"] not in confirmed_ids]

        old = self._get_generation(rotation["sa_id"], rotation["old_generation"])
        return {
            "id": rotation["id"],
            "sa_id": rotation["sa_id"],
            "old_generation": rotation["old_generation"],
            "new_generation": rotation["new_generation"],
            "status": rotation["status"],
            "grace_seconds": rotation["grace_seconds"],
            "created_at": rotation["created_at"],
            "completed_at": rotation["completed_at"],
            "finished_reason": rotation["finished_reason"],
            "confirmed_pods": [self._pod_out(p) for p in confirmed_rows],
            "pending_pods": [self._pod_out(p) for p in pending_rows],
            "old_generation_status": old["status"] if old else "revoked",
            "old_generation_expires_at": old["expires_at"] if old else None,
        }

    def list_rotations(self, sa_id: Optional[int] = None) -> list[dict]:
        if sa_id is not None:
            self._get_sa(sa_id)
            rows = self._conn.execute(
                "SELECT id FROM rotations WHERE sa_id = ? ORDER BY id", (sa_id,)
            ).fetchall()
        else:
            rows = self._conn.execute("SELECT id FROM rotations ORDER BY id").fetchall()
        return [self.get_rotation(r["id"]) for r in rows]
