"""Roster import: paged PodList snapshots become the effective pod roster."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path


# --------------------------------------------------------------------- helpers


def make_pod(name, uid, namespace="monitoring", sa="metrics-reader",
             phase="Running", deletion_timestamp=None):
    meta = {"name": name, "uid": uid, "namespace": namespace}
    if deletion_timestamp:
        meta["deletionTimestamp"] = deletion_timestamp
    return {
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": meta,
        "spec": {"serviceAccountName": sa},
        "status": {"phase": phase},
    }


def pod_list(items, rv="1000", cont=None, namespace=None):
    meta = {"resourceVersion": rv}
    if cont:
        meta["continue"] = cont
    if namespace:
        meta["namespace"] = namespace
    return {"kind": "PodList", "apiVersion": "v1", "metadata": meta,
            "items": items}


def create_sa(client, name="metrics-reader", namespace="monitoring"):
    r = client.post(
        "/service-accounts",
        json={"name": name, "namespace": namespace, "description": ""},
    )
    assert r.status_code == 201, r.text
    return r.json()


def open_import(client, sa_id, namespace=None):
    body = {"namespace": namespace} if namespace else {}
    r = client.post(f"/service-accounts/{sa_id}/roster-imports", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def submit(client, import_id, continue_token, items, rv="1000",
           next_continue=None):
    r = client.post(
        f"/roster-imports/{import_id}/pages",
        json={
            "continue_token": continue_token,
            "pod_list": pod_list(items, rv=rv, cont=next_continue),
        },
    )
    return r


def roster(client, sa_id):
    return client.get(f"/service-accounts/{sa_id}/roster").json()


def pods_by_name(client, sa_id):
    body = client.get(f"/service-accounts/{sa_id}").json()
    return {p["name"]: p for p in body["pods"]}


# --------------------------------------------------------------- happy paths


def test_single_page_roster_becomes_effective(client):
    sa = create_sa(client)
    imp = open_import(client, sa["id"])
    r = submit(
        client,
        imp["id"],
        "",
        [
            make_pod("pod-a", "uid-a"),
            make_pod("pod-b", "uid-b", phase="Pending"),      # not Running
            make_pod("pod-c", "uid-c", namespace="other"),    # other namespace
            make_pod("pod-d", "uid-d", sa="someone-else"),    # other account
            make_pod(
                "pod-e", "uid-e", deletion_timestamp="2026-01-01T00:00:00Z"
            ),                                               # terminating
        ],
    )
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["accepted"] is True
    assert body["import"]["status"] == "active"
    assert body["import"]["pod_count"] == 5
    assert body["import"]["collected_complete"] is True

    pods = pods_by_name(client, sa["id"])
    assert set(pods) == {"pod-a"}
    assert pods["pod-a"]["uid"] == "uid-a"
    assert pods["pod-a"]["source"] == "roster"
    assert pods["pod-a"]["last_committed_gen"] == 1

    body = roster(client, sa["id"])
    assert body["source"] == "roster"
    assert body["active_roster_id"] == imp["id"]
    assert [m["name"] for m in body["members"]] == ["pod-a"]


def test_multi_page_roster_collects_all_pages(client):
    sa = create_sa(client, name="bulk", namespace="ns")
    imp = open_import(client, sa["id"])
    assert imp["next_continue"] == ""

    # 250 pods over three pages, 100 + 100 + 50.
    pages = [
        [make_pod(f"pod-{i}", f"uid-{i}", namespace="ns", sa="bulk")
         for i in range(0, 100)],
        [make_pod(f"pod-{i}", f"uid-{i}", namespace="ns", sa="bulk")
         for i in range(100, 200)],
        [make_pod(f"pod-{i}", f"uid-{i}", namespace="ns", sa="bulk")
         for i in range(200, 250)],
    ]
    r = submit(client, imp["id"], "", pages[0], next_continue="c1")
    assert r.status_code == 202
    assert r.json()["import"]["status"] == "collecting"
    assert r.json()["import"]["next_continue"] == "c1"
    assert r.json()["import"]["collected_complete"] is False

    r = submit(client, imp["id"], "c1", pages[1], next_continue="c2")
    assert r.json()["import"]["status"] == "collecting"
    assert r.json()["import"]["page_count"] == 2

    # While collecting the old (manual) roster is still the effective one.
    assert roster(client, sa["id"])["active_roster_id"] is None

    r = submit(client, imp["id"], "c2", pages[2])
    assert r.status_code == 202
    body = r.json()
    assert body["import"]["status"] == "active"
    assert body["import"]["pod_count"] == 250
    assert body["import"]["page_count"] == 3
    assert body["import"]["next_continue"] == ""

    detail = client.get(f"/service-accounts/{sa['id']}").json()
    assert len(detail["pods"]) == 250
    assert detail["active_roster_id"] == imp["id"]


def test_duplicate_uid_across_pages_is_one_pod(client):
    sa = create_sa(client, name="dups", namespace="ns")
    imp = open_import(client, sa["id"])
    pod = make_pod("p", "uid-p", namespace="ns", sa="dups")
    submit(client, imp["id"], "", [pod], next_continue="c1")
    r = submit(client, imp["id"], "c1", [pod])
    assert r.status_code == 202
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    assert [p["name"] for p in detail["pods"]] == ["p"]


# ------------------------------------------------------------- rejects pages


def test_page_with_more_than_100_pods_rejected(client):
    sa = create_sa(client, name="big", namespace="ns")
    imp = open_import(client, sa["id"])
    items = [make_pod(f"p-{i}", f"u-{i}", namespace="ns", sa="big")
             for i in range(101)]
    r = submit(client, imp["id"], "", items)
    assert r.status_code == 400
    assert "at most 100" in r.json()["detail"]
    # Nothing was stored; the round can still take a valid first page.
    state = client.get(f"/roster-imports/{imp['id']}").json()
    assert state["page_count"] == 0
    assert state["status"] == "collecting"


def test_missing_page_keeps_old_roster(client):
    sa = create_sa(client, name="gappy", namespace="ns")
    manual = client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "manual-pod", "namespace": "ns"},
    ).json()
    imp = open_import(client, sa["id"])
    submit(client, imp["id"], "", [make_pod("a", "uid-a", namespace="ns",
                                            sa="gappy")],
           next_continue="c1")

    # The final page is never submitted, so the round cannot be activated.
    r = client.post(f"/roster-imports/{imp['id']}/activate")
    assert r.status_code == 409
    assert "incomplete" in r.json()["detail"]

    state = client.get(f"/roster-imports/{imp['id']}").json()
    assert state["status"] == "collecting"
    assert state["next_continue"] == "c1"
    assert roster(client, sa["id"])["active_roster_id"] is None
    assert set(pods_by_name(client, sa["id"])) == {"manual-pod"}
    # Manual pod still works.
    assert client.post(f"/pods/{manual['id']}/restart").status_code == 200


def test_wrong_continue_token_rejected(client):
    sa = create_sa(client, name="order", namespace="ns")
    imp = open_import(client, sa["id"])
    submit(client, imp["id"], "", [make_pod("a", "uid-a", namespace="ns",
                                            sa="order")],
           next_continue="c1")

    r = submit(client, imp["id"], "wrong", [make_pod("b", "uid-b",
                                                     namespace="ns",
                                                     sa="order")])
    assert r.status_code == 409
    assert "out-of-order" in r.json()["detail"]

    # The correct marker still works and completes the roster.
    r = submit(client, imp["id"], "c1", [make_pod("b", "uid-b", namespace="ns",
                                                  sa="order")])
    assert r.json()["import"]["status"] == "active"


def test_mixed_resource_version_rejected(client):
    sa = create_sa(client, name="mixed", namespace="ns")
    imp = open_import(client, sa["id"])
    submit(client, imp["id"], "", [make_pod("a", "uid-a", namespace="ns",
                                            sa="mixed")],
           rv="1000", next_continue="c1")

    r = submit(client, imp["id"], "c1",
               [make_pod("b", "uid-b", namespace="ns", sa="mixed")],
               rv="2000")
    assert r.status_code == 409
    assert "resourceVersion mismatch" in r.json()["detail"]
    state = client.get(f"/roster-imports/{imp['id']}").json()
    assert state["page_count"] == 1
    assert state["status"] == "collecting"
    assert roster(client, sa["id"])["active_roster_id"] is None

    # Re-submitting the same page with the pinned version completes it.
    r = submit(client, imp["id"], "c1",
               [make_pod("b", "uid-b", namespace="ns", sa="mixed")],
               rv="1000")
    assert r.json()["import"]["status"] == "active"


def test_item_namespace_mismatch_is_ignored(client):
    sa = create_sa(client, name="nscheck", namespace="ns")
    imp = open_import(client, sa["id"])
    r = submit(client, imp["id"], "",
               [make_pod("a", "uid-a", namespace="other", sa="nscheck"),
                make_pod("b", "uid-b", namespace="ns", sa="nscheck")])
    assert r.status_code == 202
    assert set(pods_by_name(client, sa["id"])) == {"b"}


def test_malformed_pages_rejected(client):
    sa = create_sa(client, name="malformed", namespace="ns")
    imp = open_import(client, sa["id"])

    r = submit(client, imp["id"], "", [{"kind": "Pod", "metadata": {
        "name": "x", "namespace": "ns"}}])
    assert r.status_code == 400  # missing uid

    r = client.post(
        f"/roster-imports/{imp['id']}/pages",
        json={"continue_token": "", "pod_list": {"metadata": {}, "items": []}},
    )
    assert r.status_code == 400  # missing resourceVersion


# ------------------------------------------------------ persistence failures


def test_storage_failure_keeps_previous_roster(client, service):
    sa = create_sa(client, name="fragile", namespace="ns")
    manual = client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "keeper", "namespace": "ns"},
    ).json()

    imp = open_import(client, sa["id"])
    # Inject a storage failure that only triggers while activation inserts a
    # new pod row; the page itself is already persisted.
    conn = service._conn
    conn.execute(
        "CREATE TRIGGER fail_roster_insert BEFORE INSERT ON pods "
        "WHEN NEW.source = 'roster' "
        "BEGIN SELECT RAISE(ABORT, 'disk on fire'); END"
    )
    try:
        r = submit(client, imp["id"], "",
                   [make_pod("new-a", "uid-new-a", namespace="ns",
                             sa="fragile")])
        assert r.status_code == 202
        body = r.json()
        # The page is acknowledged, but activation failed and was rolled back.
        assert body["import"]["status"] == "collecting"
        assert body["import"]["collected_complete"] is True
        assert "previous roster remains in force" in body["message"]

        # An explicit activation surfaces the storage failure and still
        # leaves the previous roster untouched.
        r = client.post(f"/roster-imports/{imp['id']}/activate")
        assert r.status_code == 500
        assert "previous roster remains in force" in r.json()["detail"]
    finally:
        conn.execute("DROP TRIGGER fail_roster_insert")

    # Old roster is untouched and the old pod still works.
    assert roster(client, sa["id"])["active_roster_id"] is None
    pods = pods_by_name(client, sa["id"])
    assert set(pods) == {"keeper"}
    assert pods["keeper"]["last_committed_gen"] == 1
    assert client.post(f"/pods/{manual['id']}/restart").status_code == 200

    # A fresh round can still be imported afterwards.
    imp2 = open_import(client, sa["id"])
    r = submit(client, imp2["id"], "",
               [make_pod("new-a", "uid-new-a", namespace="ns", sa="fragile")])
    assert r.json()["import"]["status"] == "active"
    assert set(pods_by_name(client, sa["id"])) == {"new-a"}


def test_uid_moving_to_another_service_account_is_single_counted(client):
    a = create_sa(client, name="sa-a", namespace="ns")
    b = create_sa(client, name="sa-b", namespace="ns")
    imp_a = open_import(client, a["id"])
    submit(client, imp_a["id"], "",
           [make_pod("shared", "uid-shared", namespace="ns", sa="sa-a")])

    imp_b = open_import(client, b["id"])
    submit(client, imp_b["id"], "",
           [make_pod("shared", "uid-shared", namespace="ns", sa="sa-b")])

    assert pods_by_name(client, a["id"]) == {}
    assert set(pods_by_name(client, b["id"])) == {"shared"}
    assert roster(client, a["id"])["retired"][0]["uid"] == "uid-shared"


def test_explicit_activation_after_transient_failure(client, service):
    sa = create_sa(client, name="retry", namespace="ns")
    imp = open_import(client, sa["id"])
    conn = service._conn
    conn.execute(
        "CREATE TRIGGER fail_roster_insert BEFORE INSERT ON pods "
        "WHEN NEW.source = 'roster' "
        "BEGIN SELECT RAISE(ABORT, 'disk on fire'); END"
    )
    try:
        r = submit(client, imp["id"], "",
                   [make_pod("a", "uid-a", namespace="ns", sa="retry")])
        assert r.json()["import"]["status"] == "collecting"
    finally:
        conn.execute("DROP TRIGGER fail_roster_insert")

    # The collected pages are still there; activation can be retried.
    r = client.post(f"/roster-imports/{imp['id']}/activate")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "active"
    assert set(pods_by_name(client, sa["id"])) == {"a"}


# --------------------------------------------------------- ordering of rounds


def test_later_import_opened_prevents_earlier_activation(client):
    sa = create_sa(client, name="races", namespace="ns")
    early = open_import(client, sa["id"])
    submit(client, early["id"], "",
           [make_pod("early-pod", "uid-early", namespace="ns", sa="races")],
           next_continue="c1")

    # A newer round is opened before the first one is complete.
    late = open_import(client, sa["id"])

    r = submit(client, early["id"], "c1",
               [make_pod("early-pod-2", "uid-early-2", namespace="ns",
                         sa="races")])
    assert r.status_code == 409
    assert "superseded" in r.json()["detail"]
    assert client.post(
        f"/roster-imports/{early['id']}/activate"
    ).status_code == 409

    # The later round wins.
    r = submit(client, late["id"], "",
               [make_pod("late-pod", "uid-late", namespace="ns", sa="races")])
    assert r.json()["import"]["status"] == "active"
    assert set(pods_by_name(client, sa["id"])) == {"late-pod"}

    body = roster(client, sa["id"])
    assert body["active_roster_id"] == late["id"]
    assert body["pending_import"] is None
    imports = client.get(f"/roster-imports?sa_id={sa['id']}").json()
    statuses = {i["id"]: i["status"] for i in imports}
    assert statuses[early["id"]] == "superseded"
    assert statuses[late["id"]] == "active"


def test_newer_active_import_replaces_earlier_active_roster(client):
    sa = create_sa(client, name="ab", namespace="ns")
    a = open_import(client, sa["id"])
    submit(client, a["id"], "",
           [make_pod("a1", "uid-a1", namespace="ns", sa="ab"),
            make_pod("a2", "uid-a2", namespace="ns", sa="ab")])
    assert set(pods_by_name(client, sa["id"])) == {"a1", "a2"}

    b = open_import(client, sa["id"])
    submit(client, b["id"], "",
           [make_pod("a1", "uid-a1", namespace="ns", sa="ab")])
    body = roster(client, sa["id"])
    assert body["active_roster_id"] == b["id"]
    assert set(pods_by_name(client, sa["id"])) == {"a1"}
    retired = {p["name"]: p for p in body["retired"]}
    assert retired["a2"]["retired"] is True
    assert retired["a2"]["alive"] is False


# --------------------------------------- takeover, identity, retained state


def test_first_roster_retires_manual_pods_and_keeps_matching_state(client):
    sa = create_sa(client, name="takeover", namespace="ns")
    manual = client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "old-manual", "namespace": "ns"},
    ).json()

    # Give the manual pod some history: rotate and let it commit gen 2.
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    assert client.post(
        f"/rotations/{rotation['id']}/confirm?pod_id={manual['id']}",
        json={"generation": 2},
    ).json()["status"] == "completed"

    # A second rotation stays in flight while the roster is imported.
    rotation2 = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()

    imp = open_import(client, sa["id"])
    # The imported pod keeps the same name but a new uid, plus one newcomer.
    submit(client, imp["id"], "",
           [make_pod("old-manual", "uid-new", namespace="ns", sa="takeover"),
            make_pod("new-pod", "uid-new-pod", namespace="ns",
                     sa="takeover")])

    pods = pods_by_name(client, sa["id"])
    assert set(pods) == {"old-manual", "new-pod"}
    # Same name, different uid -> brand-new pod at the current generation.
    assert pods["old-manual"]["uid"] == "uid-new"
    assert pods["old-manual"]["id"] != manual["id"]
    assert pods["old-manual"]["last_committed_gen"] == 3

    body = roster(client, sa["id"])
    retired = {p["id"]: p for p in body["retired"]}
    assert manual["id"] in retired
    assert retired[manual["id"]]["retired"] is True
    assert "replaced by a new pod uid" in retired[manual["id"]]["retired_reason"]

    # The retired member can no longer confirm or restart.
    r = client.post(
        f"/rotations/{rotation2['id']}/confirm?pod_id={manual['id']}",
        json={"generation": 3},
    )
    assert r.status_code == 409
    assert "no longer in" in r.json()["detail"]
    r = client.post(f"/pods/{manual['id']}/restart")
    assert r.status_code == 409
    assert client.post(
        f"/pods/{manual['id']}/alive?alive=true"
    ).status_code == 409

    # The rotation is now judged on the new roster: both new pods confirm.
    for name in ("old-manual", "new-pod"):
        r = client.post(
            f"/rotations/{rotation2['id']}/confirm?pod_id="
            f"{pods[name]['id']}",
            json={"generation": 3},
        )
        assert r.status_code == 200, r.text
    assert client.get(
        f"/rotations/{rotation2['id']}"
    ).json()["status"] == "completed"


def test_same_uid_keeps_committed_generation_and_confirmation(client):
    sa = create_sa(client, name="stable", namespace="ns")
    imp1 = open_import(client, sa["id"])
    submit(client, imp1["id"], "",
           [make_pod("steady", "uid-steady", namespace="ns", sa="stable")])
    pod = pods_by_name(client, sa["id"])["steady"]

    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    assert client.post(
        f"/rotations/{rotation['id']}/confirm?pod_id={pod['id']}",
        json={"generation": 2},
    ).json()["status"] == "completed"

    # A later snapshot still contains the same pod (same uid).
    rotation2 = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    imp2 = open_import(client, sa["id"])
    submit(client, imp2["id"], "",
           [make_pod("steady", "uid-steady", namespace="ns", sa="stable")])

    kept = pods_by_name(client, sa["id"])["steady"]
    assert kept["id"] == pod["id"]
    assert kept["last_committed_gen"] == 2

    # Its confirmation is retained, but the new round's pod is still pending
    # because the pod itself did not confirm gen 3.
    body = client.get(f"/rotations/{rotation2['id']}").json()
    assert body["status"] == "pending"

    # Confirm gen 3 and the retained identity can complete the rotation.
    assert client.post(
        f"/rotations/{rotation2['id']}/confirm?pod_id={kept['id']}",
        json={"generation": 3},
    ).json()["status"] == "completed"


def test_roster_drop_makes_rotation_complete_or_expire(client, clock):
    # --- case 1: dropping the only pending pod lets the rotation complete.
    sa = create_sa(client, name="shrink", namespace="ns")
    imp1 = open_import(client, sa["id"])
    submit(client, imp1["id"], "",
           [make_pod("a", "uid-a", namespace="ns", sa="shrink"),
            make_pod("b", "uid-b", namespace="ns", sa="shrink")])
    pods = pods_by_name(client, sa["id"])
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    client.post(
        f"/rotations/{rotation['id']}/confirm?pod_id={pods['a']['id']}",
        json={"generation": 2},
    )
    assert client.get(f"/rotations/{rotation['id']}").json()["status"] == "pending"

    imp2 = open_import(client, sa["id"])
    submit(client, imp2["id"], "",
           [make_pod("a", "uid-a", namespace="ns", sa="shrink")])
    # "b" left the roster; only "a" must confirm, and it already had.
    assert client.get(f"/rotations/{rotation['id']}").json()["status"] == "completed"

    # --- case 2: grace already elapsed -> the rotation expires on switch.
    sa2 = create_sa(client, name="expiring", namespace="ns")
    imp3 = open_import(client, sa2["id"])
    submit(client, imp3["id"], "",
           [make_pod("c", "uid-c", namespace="ns", sa="expiring")])
    rot2 = client.post(
        f"/service-accounts/{sa2['id']}/rotations", json={"grace_seconds": 10}
    ).json()
    clock.advance(11)
    imp4 = open_import(client, sa2["id"])
    submit(client, imp4["id"], "",
           [make_pod("c", "uid-c", namespace="ns", sa="expiring")])
    assert client.get(f"/rotations/{rot2['id']}").json()["status"] == "expired"


def test_rotation_and_confirmation_work_while_collecting(client):
    sa = create_sa(client, name="live", namespace="ns")
    imp = open_import(client, sa["id"])
    submit(client, imp["id"], "",
           [make_pod("a", "uid-a", namespace="ns", sa="live"),
            make_pod("b", "uid-b", namespace="ns", sa="live")],
           next_continue="c1")

    # The previous (manual) pod is still effective; rotation happens normally.
    manual = client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "manual", "namespace": "ns"},
    ).json()
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    )
    assert rotation.status_code == 201
    assert client.get(
        f"/service-accounts/{sa['id']}/roster"
    ).json()["pending_import"]["page_count"] == 1


def test_empty_roster_retires_all_existing_members(client):
    sa = create_sa(client, name="empty", namespace="ns")
    manual = client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "manual", "namespace": "ns"},
    ).json()
    imp = open_import(client, sa["id"])
    # A complete snapshot with zero pods is valid and means "nobody".
    r = submit(client, imp["id"], "", [])
    assert r.status_code == 202, r.text
    assert r.json()["import"]["status"] == "active"
    assert pods_by_name(client, sa["id"]) == {}
    body = roster(client, sa["id"])
    assert body["members"] == []
    assert [p["id"] for p in body["retired"]] == [manual["id"]]
    assert client.post(f"/pods/{manual['id']}/restart").status_code == 409


def test_completed_page_cannot_be_resubmitted(client, service):
    sa = create_sa(client, name="twice", namespace="ns")
    imp = open_import(client, sa["id"])
    conn = service._conn
    # Force activation to fail so the round stays complete-but-not-active.
    conn.execute(
        "CREATE TRIGGER fail_roster_insert BEFORE INSERT ON pods "
        "WHEN NEW.source = 'roster' "
        "BEGIN SELECT RAISE(ABORT, 'disk on fire'); END"
    )
    try:
        submit(client, imp["id"], "",
               [make_pod("a", "uid-a", namespace="ns", sa="twice")])
    finally:
        conn.execute("DROP TRIGGER fail_roster_insert")
    r = submit(client, imp["id"], "",
               [make_pod("b", "uid-b", namespace="ns", sa="twice")])
    assert r.status_code == 409
    assert "already been collected" in r.json()["detail"]


def test_superseded_active_roster_cannot_be_reactivated(client):
    sa = create_sa(client, name="oldactive", namespace="ns")
    a = open_import(client, sa["id"])
    submit(client, a["id"], "",
           [make_pod("a", "uid-a", namespace="ns", sa="oldactive")])
    b = open_import(client, sa["id"])
    submit(client, b["id"], "",
           [make_pod("b", "uid-b", namespace="ns", sa="oldactive")])

    r = client.post(f"/roster-imports/{a['id']}/activate")
    assert r.status_code == 409
    assert "superseded" in r.json()["detail"]
    assert set(pods_by_name(client, sa["id"])) == {"b"}


def test_roster_endpoint_in_openapi_docs(client):
    schema = client.get("/openapi.json").json()
    assert "/service-accounts/{sa_id}/roster-imports" in schema["paths"]
    assert "/roster-imports/{import_id}/pages" in schema["paths"]
    assert "/service-accounts/{sa_id}/roster" in schema["paths"]
    assert client.get("/docs").status_code == 200


# -------------------------------------------------------------- db migration


def test_old_database_is_taken_over_in_place(tmp_path):
    """A database created by the previous release keeps its data."""
    from app import database

    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE service_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
            namespace TEXT NOT NULL, description TEXT NOT NULL DEFAULT '',
            current_gen INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL);
        CREATE TABLE token_generations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, sa_id INTEGER NOT NULL,
            generation INTEGER NOT NULL, token TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'active', created_at REAL NOT NULL,
            expires_at REAL, superseded_at REAL, revoked_at REAL,
            revoke_reason TEXT NOT NULL DEFAULT '',
            UNIQUE (sa_id, generation));
        CREATE TABLE pods (
            id INTEGER PRIMARY KEY AUTOINCREMENT, sa_id INTEGER NOT NULL,
            name TEXT NOT NULL, namespace TEXT NOT NULL,
            alive INTEGER NOT NULL DEFAULT 1,
            last_committed_gen INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL, updated_at REAL NOT NULL,
            UNIQUE (sa_id, namespace, name));
        CREATE TABLE rotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, sa_id INTEGER NOT NULL,
            old_generation INTEGER NOT NULL, new_generation INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', grace_seconds INTEGER
            NOT NULL, created_at REAL NOT NULL, completed_at REAL,
            finished_reason TEXT NOT NULL DEFAULT '');
        CREATE TABLE confirmations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, rotation_id INTEGER NOT NULL,
            pod_id INTEGER NOT NULL, generation INTEGER NOT NULL,
            confirmed_at REAL NOT NULL, UNIQUE (rotation_id, pod_id));
        INSERT INTO service_accounts VALUES
            (1, 'sa', 'ns', '', 2, 1.0);
        INSERT INTO token_generations VALUES
            (1, 1, 1, 'tok-1', 'superseded', 1.0, 1000.0, 2.0, NULL, ''),
            (2, 1, 2, 'tok-2', 'active', 2.0, NULL, NULL, NULL, '');
        INSERT INTO pods VALUES (5, 1, 'pod-a', 'ns', 1, 2, 1.0, 2.0);
        INSERT INTO rotations VALUES (7, 1, 1, 2, 'pending', 300, 2.0, NULL, '');
        INSERT INTO confirmations VALUES (9, 7, 5, 2, 3.0);
        """
    )
    conn.commit()
    conn.close()

    conn = database.connect(db)
    database.init_db(conn)
    row = conn.execute("SELECT * FROM pods WHERE id = 5").fetchone()
    assert row["name"] == "pod-a"
    assert row["uid"] is None
    assert row["source"] == "manual"
    assert row["retired"] == 0
    assert row["last_committed_gen"] == 2
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM confirmations WHERE pod_id = 5"
    ).fetchone()["n"] == 1
    sa = conn.execute("SELECT * FROM service_accounts WHERE id = 1").fetchone()
    assert sa["active_roster_id"] is None
    # Roster tables now exist and are usable.
    conn.execute(
        "INSERT INTO roster_imports(sa_id, namespace, created_at) "
        "VALUES (1, 'ns', 1.0)"
    )
    conn.close()

    # init_db is idempotent: running it again must not lose or duplicate data.
    conn = database.connect(db)
    database.init_db(conn)
    assert conn.execute("SELECT COUNT(*) AS n FROM pods").fetchone()["n"] == 1
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM confirmations"
    ).fetchone()["n"] == 1
    conn.close()


def test_database_from_intermediate_release_gets_new_column(tmp_path):
    """A DB that already has roster pods but no retired_reason is patched."""
    from app import database

    db = tmp_path / "mid.db"
    conn = database.connect(db)
    conn.executescript(
        """
        CREATE TABLE pods (
            id INTEGER PRIMARY KEY AUTOINCREMENT, sa_id INTEGER NOT NULL,
            name TEXT NOT NULL, namespace TEXT NOT NULL, uid TEXT,
            source TEXT NOT NULL DEFAULT 'manual', retired INTEGER NOT NULL
            DEFAULT 0, alive INTEGER NOT NULL DEFAULT 1,
            last_committed_gen INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL, updated_at REAL NOT NULL);
        INSERT INTO pods VALUES (1, 1, 'p', 'ns', 'u', 'roster', 0, 1, 1,
                                 1.0, 1.0);
        """
    )
    database.init_db(conn)
    row = conn.execute("SELECT * FROM pods WHERE id = 1").fetchone()
    assert row["retired_reason"] == ""
    conn.close()
