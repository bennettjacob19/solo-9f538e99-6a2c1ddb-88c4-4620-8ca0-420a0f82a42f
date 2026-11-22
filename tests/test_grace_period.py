"""Grace-period semantics: old generation valid until expiry, rejected after."""


def _setup_with_rotation(client, grace):
    sa = client.post(
        "/service-accounts",
        json={"name": "sa", "namespace": "ns", "description": ""},
    ).json()
    pod = client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "pod-0", "namespace": "ns"},
    ).json()
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations",
        json={"grace_seconds": grace},
    ).json()
    return sa, pod, rotation


def test_old_token_valid_inside_grace_and_rejected_at_expiry(client, clock):
    sa, pod, rotation = _setup_with_rotation(client, grace=10)
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    old_token = detail["generations"][0]["token"]
    new_token = detail["generations"][1]["token"]

    clock.advance(9)
    body = client.post("/verify", json={"token": old_token}).json()
    assert body["valid"] is True
    assert "grace period" in body["reason"]

    # Exactly at / past expires_at -> immediate rejection.
    clock.advance(1)
    body = client.post("/verify", json={"token": old_token}).json()
    assert body["valid"] is False
    assert body["reason"] == "grace period elapsed; old generation expired"

    # New generation unaffected.
    assert client.post("/verify", json={"token": new_token}).json()["valid"] is True

    # Persisted state converges to revoked via the lazy expiry path.
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    assert detail["generations"][0]["status"] == "revoked"
    assert detail["generations"][0]["revoke_reason"] == "grace period elapsed"

    rot = client.get(f"/rotations/{rotation['id']}").json()
    assert rot["status"] == "expired"


def test_background_sweeper_expires_generation(client, clock, service):
    sa, pod, rotation = _setup_with_rotation(client, grace=5)
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    old_token = detail["generations"][0]["token"]

    clock.advance(6)
    swept = service.sweep_expired()
    assert swept == 1

    body = client.post("/verify", json={"token": old_token}).json()
    assert body["valid"] is False
    rot = client.get(f"/rotations/{rotation['id']}").json()
    assert rot["status"] == "expired"


def test_rotation_completed_before_expiry_revokes_immediately(client, clock):
    sa, pod, rotation = _setup_with_rotation(client, grace=100)
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    old_token = detail["generations"][0]["token"]

    r = client.post(
        f"/rotations/{rotation['id']}/confirm?pod_id={pod['id']}",
        json={"generation": 2},
    )
    assert r.json()["status"] == "completed"

    # Revoked outright, no grace remaining for the old generation.
    clock.advance(50)
    body = client.post("/verify", json={"token": old_token}).json()
    assert body["valid"] is False
    assert body["reason"] == "token generation has been revoked"


def test_short_grace_end_to_end_with_real_time(client, service):
    """No fake clock: 1s grace, verify real timer behavior."""
    import time

    # Restore a real clock for this test (the fixture installs a fake one).
    service._now = time.time

    sa = client.post(
        "/service-accounts",
        json={"name": "rt", "namespace": "ns", "description": ""},
    ).json()
    old_token = sa["generations"][0]["token"]
    client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "p", "namespace": "ns"},
    )
    client.post(
        f"/service-accounts/{sa['id']}/rotations",
        json={"grace_seconds": 1},
    )
    assert client.post("/verify", json={"token": old_token}).json()["valid"] is True
    # Sweep interval is 1s; allow timer slack for the background task and verify.
    body = {}
    deadline = 4.0
    while deadline > 0:
        time.sleep(0.2)
        deadline -= 0.2
        body = client.post("/verify", json={"token": old_token}).json()
        if not body["valid"]:
            break
    assert body["valid"] is False
    assert body["reason"] == "grace period elapsed; old generation expired"
