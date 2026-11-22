"""Stale-confirmation protection and restart / failure recovery."""


def _setup(client, n_pods=2):
    sa = client.post(
        "/service-accounts",
        json={"name": "sa", "namespace": "ns", "description": ""},
    ).json()
    pods = [
        client.post(
            f"/service-accounts/{sa['id']}/pods",
            json={"name": f"pod-{i}", "namespace": "ns"},
        ).json()
        for i in range(n_pods)
    ]
    return sa, pods


def test_stale_confirmation_for_old_generation_rejected(client):
    sa, pods = _setup(client)
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    rid = rotation["id"]

    # Pod reports generation 1 (the old one) instead of 2.
    r = client.post(
        f"/rotations/{rid}/confirm?pod_id={pods[0]['id']}",
        json={"generation": 1},
    )
    assert r.status_code == 409
    assert "stale generation" in r.json()["detail"]

    # No state changed: rotation still pending with zero confirmations.
    body = client.get(f"/rotations/{rid}").json()
    assert body["status"] == "pending"
    assert body["confirmed_pods"] == []
    assert len(body["pending_pods"]) == 2

    # Old generation was not revoked by the rejected confirmation.
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    assert detail["generations"][0]["status"] == "superseded"


def test_late_confirmation_after_rotation_completed_is_rejected(client):
    sa, pods = _setup(client)
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    rid = rotation["id"]
    for pod in pods:
        client.post(
            f"/rotations/{rid}/confirm?pod_id={pod['id']}",
            json={"generation": 2},
        )
    assert client.get(f"/rotations/{rid}").json()["status"] == "completed"

    # A late duplicate confirmation must not change anything.
    r = client.post(
        f"/rotations/{rid}/confirm?pod_id={pods[0]['id']}",
        json={"generation": 2},
    )
    assert r.status_code == 409
    assert "late confirmation" in r.json()["detail"]


def test_second_rotation_makes_old_pending_rotation_uncompletable(client):
    sa, pods = _setup(client, n_pods=1)
    r1 = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()

    # Operator starts another rotation before the pod confirmed the first one.
    r2 = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    assert r2["new_generation"] == 3

    # Confirming against the superseded rotation is rejected.
    r = client.post(
        f"/rotations/{r1['id']}/confirm?pod_id={pods[0]['id']}",
        json={"generation": 2},
    )
    assert r.status_code == 409
    assert client.get(f"/rotations/{r1['id']}").json()["status"] == "superseded"

    # The new rotation can complete normally and only revokes generation 2.
    r = client.post(
        f"/rotations/{r2['id']}/confirm?pod_id={pods[0]['id']}",
        json={"generation": 3},
    )
    assert r.json()["status"] == "completed"
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    by_gen = {g["generation"]: g["status"] for g in detail["generations"]}
    assert by_gen == {1: "superseded", 2: "revoked", 3: "active"}


def test_restart_reuses_last_committed_generation_and_clears_confirmation(client):
    sa, pods = _setup(client, n_pods=2)
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    rid = rotation["id"]
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    gen1_token = detail["generations"][0]["token"]

    # Pod 0 confirms gen 2, then crashes and restarts.
    client.post(
        f"/rotations/{rid}/confirm?pod_id={pods[0]['id']}",
        json={"generation": 2},
    )
    r = client.post(f"/pods/{pods[0]['id']}/restart")
    assert r.status_code == 200, r.text
    result = r.json()
    assert result["reused_generation"] == 2
    assert result["token"] != gen1_token
    assert result["needs_confirmation_for_rotation"] == rid

    # Restarted pod must confirm again: rotation back to zero confirmed pods.
    body = client.get(f"/rotations/{rid}").json()
    assert body["status"] == "pending"
    assert body["confirmed_pods"] == []
    assert len(body["pending_pods"]) == 2

    # Re-confirm, then pod 1 confirms -> completion.
    client.post(
        f"/rotations/{rid}/confirm?pod_id={pods[0]['id']}",
        json={"generation": 2},
    )
    r = client.post(
        f"/rotations/{rid}/confirm?pod_id={pods[1]['id']}",
        json={"generation": 2},
    )
    assert r.json()["status"] == "completed"


def test_restart_before_any_confirmation_keeps_old_generation(client):
    sa, pods = _setup(client, n_pods=1)
    client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    )
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    gen1_token = detail["generations"][0]["token"]

    result = client.post(f"/pods/{pods[0]['id']}/restart").json()
    # Never committed gen 2, so it comes back on gen 1.
    assert result["reused_generation"] == 1
    assert result["token"] == gen1_token


def test_restart_after_grace_expiry_keeps_last_committed_token(client, clock):
    sa, pods = _setup(client, n_pods=1)
    client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 10}
    )
    # Grace elapses without confirmation: gen 1 expires.
    clock.advance(11)
    service_sa = client.get(f"/service-accounts/{sa['id']}").json()
    gen1 = service_sa["generations"][0]["token"]
    assert client.post("/verify", json={"token": gen1}).json()["valid"] is False

    # Restart still reports the last committed generation; the operator can see
    # the pod needs a fresh rotation to recover a valid token.
    result = client.post(f"/pods/{pods[0]['id']}/restart").json()
    assert result["reused_generation"] == 1

    # A new rotation hands out gen 3 and the pod can move forward after restart.
    r3 = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    r = client.post(
        f"/rotations/{r3['id']}/confirm?pod_id={pods[0]['id']}",
        json={"generation": 3},
    )
    assert r.json()["status"] == "completed"
