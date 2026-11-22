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
5. A roster import only becomes effective once every page of one
   ``resourceVersion`` has been received, in order. Until then the previous
   roster (or the manual pod list) stays in force. Activation is an atomic
   transaction: an import opened earlier can never activate after one opened
   later; an import whose pages are interleaved with a different
   ``resourceVersion`` is rejected; any persistence failure rolls back and the
   old roster remains in force.
6. A retired pod cannot confirm or restart. A pod that is still in the roster
   under the same uid keeps its committed generation; a different uid with the
   same name is a brand-new pod.
"""
from __future__ import annotations

import json
import secrets
import sqlite3
import threading
import time
from typing import Optional

from .errors import ConflictError, NotFoundError, StorageError, ValidationError

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
            "uid": row["uid"],
            "source": row["source"],
            "retired": bool(row["retired"]),
            "retired_reason": row["retired_reason"],
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
                    "INSERT INTO pods(sa_id, name, namespace, source, retired, "
                    "alive, last_committed_gen, created_at, updated_at) "
                    "VALUES (?, ?, ?, 'manual', 0, 1, ?, ?, ?)",
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
            pod = self._get_pod(pod_id)
            self._ensure_not_retired(pod)
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

    @staticmethod
    def _ensure_not_retired(pod: sqlite3.Row) -> None:
        if pod["retired"]:
            raise ConflictError(
                f"pod {pod['namespace']}/{pod['name']} is no longer in the "
                "service account roster; request rejected"
            )

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
            self._ensure_not_retired(pod)
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
                if pod["retired"]:
                    raise ConflictError(
                        f"pod {pod['namespace']}/{pod['name']} is no longer in "
                        "the service account roster; confirmation rejected"
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
            "SELECT id FROM pods WHERE sa_id = ? AND alive = 1 AND retired = 0",
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
            self._expire_generation_locked(row, now)
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _expire_generation_locked(self, row: sqlite3.Row, now: float) -> None:
        """Apply expiry; the caller owns the surrounding transaction."""
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

    # ------------------------------------------------------------------ roster

    # Page handling is deliberately split in two steps:
    #   submit_roster_page()  – validate, persist and acknowledge exactly the
    #                           page the caller sent (no activation);
    #   _try_activate()       – one atomic transaction that either swaps the
    #                           roster or leaves the previous one untouched.
    # Because activation lives in a single transaction, a persistence failure
    # rolls everything back and the old roster keeps serving.

    def _get_import(self, import_id: int) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT * FROM roster_imports WHERE id = ?", (import_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"roster import id={import_id} not found")
        return row

    @staticmethod
    def _import_out(row: sqlite3.Row) -> dict:
        return {
            "id": row["id"],
            "sa_id": row["sa_id"],
            "status": row["status"],
            "resource_version": row["resource_version"],
            "page_count": row["page_count"],
            "pod_count": row["pod_count"],
            "next_continue": row["next_continue"],
            "collected_complete": (
                row["next_continue"] == "" and row["page_count"] > 0
            ),
            "created_at": row["created_at"],
            "activated_at": row["activated_at"],
            "finished_reason": row["finished_reason"],
        }

    def start_roster_import(
        self, sa_id: int, namespace: Optional[str] = None
    ) -> dict:
        """Open a new import round and return its id.

        Any still-collecting round for this service account is closed as
        superseded; only the newest round can ever activate.
        """
        with self._lock:
            sa = self._get_sa(sa_id)
            ns = namespace or sa["namespace"]
            now = self._now()
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute(
                    "UPDATE roster_imports SET status = 'superseded', "
                    "finished_reason = 'superseded by a newer import round' "
                    "WHERE sa_id = ? AND status = 'collecting'",
                    (sa_id,),
                )
                cur = self._conn.execute(
                    "INSERT INTO roster_imports(sa_id, namespace, status, "
                    "next_continue, page_count, pod_count, created_at) "
                    "VALUES (?, ?, 'collecting', '', 0, 0, ?)",
                    (sa_id, ns, now),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return self._import_out(self._get_import(cur.lastrowid))

    # ------------------------------------------------------- page validation

    @staticmethod
    def _parse_pod_list(pod_list: dict, namespace: str) -> tuple[str, str, list]:
        """Validate a PodList and return (resourceVersion, continue, items).

        Raises ValidationError for anything that is not a single consistent
        page, so no partial roster is ever written.
        """
        if not isinstance(pod_list, dict):
            raise ValidationError("pod_list must be a Kubernetes PodList object")
        metadata = pod_list.get("metadata") or {}
        if not isinstance(metadata, dict):
            raise ValidationError("pod_list.metadata must be an object")
        rv = metadata.get("resourceVersion")
        if rv is None or str(rv) == "":
            raise ValidationError(
                "pod_list.metadata.resourceVersion is required to pin the "
                "snapshot"
            )
        rv = str(rv)
        nxt = metadata.get("continue", "") or ""
        nxt = str(nxt)

        items = pod_list.get("items")
        if items is None:
            items = []
        if not isinstance(items, list):
            raise ValidationError("pod_list.items must be a list")
        if len(items) > 100:
            raise ValidationError(
                f"page has {len(items)} pods; a page may contain at most 100"
            )

        scope = metadata.get("namespace")
        if scope is not None and str(scope) != namespace:
            raise ValidationError(
                f"pod_list is scoped to namespace {scope!r}, expected "
                f"{namespace!r}"
            )

        for index, item in enumerate(items):
            kind = item.get("kind") if isinstance(item, dict) else None
            if kind not in (None, "Pod"):
                raise ValidationError(
                    f"item {index} has kind {kind!r}; only Pods are accepted"
                )
            pod_meta = item.get("metadata") if isinstance(item, dict) else None
            if not isinstance(pod_meta, dict):
                raise ValidationError(f"item {index} has no metadata object")
            if not pod_meta.get("name"):
                raise ValidationError(f"item {index} has no metadata.name")
            if not pod_meta.get("uid"):
                raise ValidationError(f"item {index} has no metadata.uid")
            item_ns = pod_meta.get("namespace")
            if item_ns is None:
                raise ValidationError(
                    f"item {index} has no metadata.namespace"
                )
            # Items from other namespaces are tolerated (a cluster-wide list)
            # but are filtered out when the roster is built.
        return rv, nxt, items

    def submit_roster_page(
        self, import_id: int, continue_token: str, pod_list: dict
    ) -> dict:
        with self._lock:
            now = self._now()
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                imp = self._get_import(import_id)
                if imp["status"] == "superseded":
                    raise ConflictError(
                        "roster import was superseded by a newer import round"
                    )
                if imp["status"] == "active":
                    raise ConflictError("roster import is already active")

                rv, nxt, items = self._parse_pod_list(pod_list, imp["namespace"])

                # The continuation marker on the wire must be the one the
                # previous page reported; this rejects re-ordering, gaps and
                # a page submitted twice.
                if imp["next_continue"] == "" and imp["page_count"] > 0:
                    raise ConflictError(
                        "all pages have already been collected; use activate "
                        "to retry promotion"
                    )
                if continue_token != imp["next_continue"]:
                    raise ConflictError(
                        "out-of-order page: expected continue token "
                        f"{imp['next_continue']!r}, got {continue_token!r}"
                    )
                # All pages must come from the same snapshot.
                if imp["resource_version"] is not None and rv != imp["resource_version"]:
                    raise ConflictError(
                        "resourceVersion mismatch: page belongs to snapshot "
                        f"{rv}, import is pinned to {imp['resource_version']}"
                    )

                page_index = imp["page_count"]
                self._conn.execute(
                    "INSERT INTO roster_pages(import_id, page_index, "
                    "continue_token, next_continue, resource_version, "
                    "item_count, items_json, received_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        import_id,
                        page_index,
                        continue_token,
                        nxt,
                        rv,
                        len(items),
                        json.dumps(items),
                        now,
                    ),
                )
                self._conn.execute(
                    "UPDATE roster_imports SET resource_version = ?, "
                    "next_continue = ?, page_count = ?, pod_count = ? "
                    "WHERE id = ?",
                    (
                        rv,
                        nxt,
                        page_index + 1,
                        imp["pod_count"] + len(items),
                        import_id,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

            message = "page stored; submit the next page"
            if nxt == "":
                # All pages are durable. Try to put the snapshot into force;
                # if that fails the previous roster stays in force and the
                # round remains complete, so activation can be retried.
                try:
                    self._try_activate(import_id)
                except Exception as exc:  # storage failure: nothing changed
                    message = (
                        "page stored; all pages collected; activation failed "
                        f"and the previous roster remains in force ({exc})"
                    )
                else:
                    state = self._get_import(import_id)
                    if state["status"] == "active":
                        message = (
                            "page stored; all pages collected; roster activated"
                        )
                    elif state["status"] == "superseded":
                        message = (
                            "page stored; all pages collected; a newer import "
                            "round was opened, so this round cannot take effect"
                        )
                    else:
                        message = (
                            "page stored; all pages collected; activation was "
                            f"not possible ({state['finished_reason']})"
                        )
            return {
                "import": self._import_out(self._get_import(import_id)),
                "accepted": True,
                "message": message,
            }

    # ----------------------------------------------------------- activation

    @staticmethod
    def _is_running(item: dict) -> bool:
        status = item.get("status") or {}
        if status.get("phase") != "Running":
            return False
        # A pod being deleted is about to disappear; do not take it in.
        if (item.get("metadata") or {}).get("deletionTimestamp"):
            return False
        return True

    @staticmethod
    def _uses_service_account(item: dict, sa_name: str) -> bool:
        spec = item.get("spec") or {}
        return spec.get("serviceAccountName") == sa_name

    def _try_activate(self, import_id: int) -> bool:
        """Atomically promote a fully collected import; old roster on failure.

        Returns True when this call activated the import, False when the
        import is not (or no longer) eligible. Eligibility is re-checked here,
        in the same transaction as the writes, which is what makes an older
        round unable to activate after a newer one has been opened.
        """
        with self._lock:
            now = self._now()
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                imp = self._get_import(import_id)
                if imp["status"] != "collecting":
                    self._conn.execute("COMMIT")
                    return False
                if imp["next_continue"] != "" or imp["page_count"] == 0:
                    self._conn.execute("COMMIT")
                    return False
                if imp["resource_version"] is None:
                    self._conn.execute("COMMIT")
                    return False
                newer = self._conn.execute(
                    "SELECT 1 FROM roster_imports WHERE sa_id = ? AND "
                    "id > ? AND status IN ('collecting', 'active') LIMIT 1",
                    (imp["sa_id"], import_id),
                ).fetchone()
                if newer is not None:
                    # A later round was opened: this one can never take effect.
                    self._conn.execute(
                        "UPDATE roster_imports SET status = 'superseded', "
                        "finished_reason = 'a newer import round was opened' "
                        "WHERE id = ?",
                        (import_id,),
                    )
                    self._conn.execute("COMMIT")
                    return False

                self._apply_roster(imp, now)
                # Only one round is ever active for a service account.
                self._conn.execute(
                    "UPDATE roster_imports SET status = 'superseded', "
                    "finished_reason = 'replaced by a newer roster' "
                    "WHERE sa_id = ? AND id != ? AND status = 'active'",
                    (imp["sa_id"], import_id),
                )
                self._conn.execute(
                    "UPDATE roster_imports SET status = 'active', "
                    "activated_at = ?, finished_reason = 'roster is in force' "
                    "WHERE id = ?",
                    (now, import_id),
                )
                self._conn.execute(
                    "UPDATE service_accounts SET active_roster_id = ? "
                    "WHERE id = ?",
                    (import_id, imp["sa_id"]),
                )
                self._conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                # The page itself was fine but the snapshot cannot be stored
                # (storage failure / constraint conflict). Roll the whole
                # switch back so the previous roster stays in force.
                self._conn.execute("ROLLBACK")
                raise StorageError(
                    "roster activation failed and was rolled back; the "
                    f"previous roster remains in force ({exc})"
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return True

    def _apply_roster(self, imp: sqlite3.Row, now: float) -> None:
        """Promote one complete snapshot. Caller holds the transaction."""
        sa = self._get_sa(imp["sa_id"])
        current_gen = sa["current_gen"]

        pages = self._conn.execute(
            "SELECT * FROM roster_pages WHERE import_id = ? ORDER BY page_index",
            (imp["id"],),
        ).fetchall()
        # One uid can appear twice if Kubernetes moved a pod between pages of
        # the same snapshot; the later page wins.
        chosen: dict[str, dict] = {}
        for page in pages:
            for item in json.loads(page["items_json"]):
                chosen[str(item["metadata"]["uid"])] = item

        desired: dict[str, dict] = {}
        for uid, item in chosen.items():
            meta = item["metadata"]
            if str(meta["namespace"]) != imp["namespace"]:
                continue
            if self._is_running(item) and self._uses_service_account(
                item, sa["name"]
            ):
                desired[uid] = item

        active = self._conn.execute(
            "SELECT * FROM pods WHERE sa_id = ? AND retired = 0", (imp["sa_id"],)
        ).fetchall()
        by_uid = {row["uid"]: row for row in active if row["uid"] is not None}
        by_name = {(row["namespace"], row["name"]): row for row in active}

        keep: set[str] = set()
        for uid, item in desired.items():
            meta = item["metadata"]
            key = (str(meta["namespace"]), str(meta["name"]))
            other = by_name.get(key)
            if other is not None and other["uid"] != uid:
                # Same name, different uid: the pod was replaced. Retire the
                # old row first so the partial unique index on
                # (sa_id, namespace, name) allows the new one; the new uid is
                # registered as a brand-new pod.
                self._retire_pod(other["id"], now, "replaced by a new pod uid")
                del by_name[key]
                if other["uid"] is not None:
                    by_uid.pop(other["uid"], None)
            existing = by_uid.get(uid)
            if existing is None:
                self._retire_same_uid_elsewhere(uid, imp["sa_id"], now)
                self._conn.execute(
                    "INSERT INTO pods(sa_id, name, namespace, uid, source, "
                    "retired, alive, last_committed_gen, created_at, "
                    "updated_at) VALUES (?, ?, ?, ?, 'roster', 0, 1, ?, ?, ?)",
                    (
                        imp["sa_id"],
                        key[1],
                        key[0],
                        uid,
                        current_gen,
                        now,
                        now,
                    ),
                )
            else:
                # Still in the roster under the same uid: keep its committed
                # generation and its confirmations for the in-flight rotation.
                self._conn.execute(
                    "UPDATE pods SET name = ?, namespace = ?, source = 'roster', "
                    "alive = 1, updated_at = ? WHERE id = ?",
                    (
                        key[1],
                        key[0],
                        now,
                        existing["id"],
                    ),
                )
            keep.add(uid)

        for row in active:
            if row["uid"] is not None and row["uid"] in keep:
                continue
            self._retire_pod(row["id"], now, "not in the imported roster")

        # An in-flight rotation is judged against the new membership and its
        # original grace window: if every remaining alive pod has already
        # confirmed, it may complete now; if its grace has run out, it expires.
        pending_rotations = self._conn.execute(
            "SELECT * FROM rotations WHERE sa_id = ? AND status = 'pending'",
            (imp["sa_id"],),
        ).fetchall()
        for rotation in pending_rotations:
            old = self._get_generation(
                imp["sa_id"], rotation["old_generation"]
            )
            if (
                old is not None
                and old["status"] == "superseded"
                and old["expires_at"] is not None
                and now >= old["expires_at"]
            ):
                self._expire_generation_locked(old, now)
            else:
                self._maybe_complete_rotation(rotation["id"], now)

    def _retire_same_uid_elsewhere(
        self, uid: str, sa_id: int, now: float
    ) -> None:
        """A Kubernetes uid can only be a member of one service account.

        If the same uid is still on another service account's roster (for
        example, a pod moved from one account to another), that member is
        retired so the uid is not double-counted.
        """
        rows = self._conn.execute(
            "SELECT id FROM pods WHERE uid = ? AND sa_id != ? AND retired = 0",
            (uid, sa_id),
        ).fetchall()
        for row in rows:
            self._retire_pod(row["id"], now, "uid moved to another service account")

    def _retire_pod(self, pod_id: int, now: float, reason: str) -> None:
        self._conn.execute(
            "UPDATE pods SET retired = 1, retired_reason = ?, alive = 0, "
            "updated_at = ? WHERE id = ? AND retired = 0",
            (reason, now, pod_id),
        )
        self._conn.execute(
            "DELETE FROM confirmations WHERE pod_id = ?", (pod_id,)
        )

    def activate_roster_import(self, import_id: int) -> dict:
        """Explicitly activate a complete import (idempotent for active)."""
        with self._lock:
            imp = self._get_import(import_id)
            if imp["status"] == "active":
                return self._import_out(imp)
            if imp["status"] == "superseded":
                raise ConflictError(
                    "roster import was superseded and can never take effect"
                )
            if imp["next_continue"] != "" or imp["page_count"] == 0:
                raise ConflictError(
                    "roster import is incomplete; submit the remaining pages"
                )
            self._try_activate(import_id)
            return self._import_out(self._get_import(import_id))

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
                "active_roster_id": r["active_roster_id"],
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
            "SELECT * FROM pods WHERE sa_id = ? AND retired = 0 ORDER BY id",
            (sa_id,),
        ).fetchall()
        return {
            "id": sa["id"],
            "name": sa["name"],
            "namespace": sa["namespace"],
            "description": sa["description"],
            "current_gen": sa["current_gen"],
            "active_roster_id": sa["active_roster_id"],
            "created_at": sa["created_at"],
            "generations": [self._gen_out(g) for g in gens],
            "pods": [self._pod_out(p) for p in pods],
        }

    def get_roster(self, sa_id: int) -> dict:
        sa = self._get_sa(sa_id)
        members = self._conn.execute(
            "SELECT * FROM pods WHERE sa_id = ? AND retired = 0 ORDER BY id",
            (sa_id,),
        ).fetchall()
        retired = self._conn.execute(
            "SELECT * FROM pods WHERE sa_id = ? AND retired = 1 ORDER BY id",
            (sa_id,),
        ).fetchall()
        active_import = None
        active_since = None
        if sa["active_roster_id"] is not None:
            row = self._conn.execute(
                "SELECT * FROM roster_imports WHERE id = ?",
                (sa["active_roster_id"],),
            ).fetchone()
            if row is not None:
                active_since = row["activated_at"]
        pending = self._conn.execute(
            "SELECT * FROM roster_imports WHERE sa_id = ? AND "
            "status = 'collecting' ORDER BY id DESC LIMIT 1",
            (sa_id,),
        ).fetchone()
        roster_members = [p for p in members if p["source"] == "roster"]
        return {
            "service_account_id": sa_id,
            "active_roster_id": sa["active_roster_id"],
            "active_since": active_since,
            "source": "roster" if roster_members else "manual",
            "members": [self._pod_out(p) for p in members],
            "retired": [self._pod_out(p) for p in retired],
            "pending_import": self._import_out(pending) if pending else None,
        }

    def list_roster_imports(self, sa_id: Optional[int] = None) -> list[dict]:
        if sa_id is not None:
            self._get_sa(sa_id)
            rows = self._conn.execute(
                "SELECT * FROM roster_imports WHERE sa_id = ? ORDER BY id",
                (sa_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM roster_imports ORDER BY id"
            ).fetchall()
        return [self._import_out(r) for r in rows]

    def get_roster_import(self, import_id: int) -> dict:
        return self._import_out(self._get_import(import_id))

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
            "SELECT * FROM pods WHERE sa_id = ? AND alive = 1 AND retired = 0 "
            "ORDER BY id",
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
