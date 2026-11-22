"""Edge case: rotation when a service account has no alive pods."""


def test_rotation_without_pods_completes_immediately(client):
    sa = client.post(
        "/service-accounts",
        json={"name": "lonely", "namespace": "ns", "description": ""},
    ).json()
    old_token = sa["generations"][0]["token"]

    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    # Nothing needs to load the new generation -> old gen revoked at once.
    assert rotation["status"] == "completed"
    assert rotation["confirmed_pods"] == []
    assert rotation["pending_pods"] == []
    assert rotation["old_generation_status"] == "revoked"

    body = client.post("/verify", json={"token": old_token}).json()
    assert body["valid"] is False


def test_pod_registered_later_mounts_current_generation(client):
    sa = client.post(
        "/service-accounts",
        json={"name": "late", "namespace": "ns", "description": ""},
    ).json()
    client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    )
    pod = client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "late-pod", "namespace": "ns"},
    ).json()
    assert pod["last_committed_gen"] == 2


def test_revived_pod_must_reconfirm_pending_rotation(client):
    sa = client.post(
        "/service-accounts",
        json={"name": "sa", "namespace": "ns", "description": ""},
    ).json()
    pod = client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "p", "namespace": "ns"},
    ).json()
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()

    # Pod marked dead -> rotation would complete with zero alive pods.
    client.post(f"/pods/{pod['id']}/alive?alive=false")
    # Bring it back before that happens (no confirmation was sent anyway).
    client.post(f"/pods/{pod['id']}/alive?alive=true")

    body = client.get(f"/rotations/{rotation['id']}").json()
    assert body["status"] == "pending"
    assert len(body["pending_pods"]) == 1

    r = client.post(
        f"/rotations/{rotation['id']}/confirm?pod_id={pod['id']}",
        json={"generation": 2},
    )
    assert r.json()["status"] == "completed"
