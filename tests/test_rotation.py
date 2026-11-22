"""Happy-path rotation and interleaved confirmations from multiple pods."""


def _setup(client, n_pods=2):
    r = client.post(
        "/service-accounts",
        json={"name": "sa", "namespace": "ns", "description": ""},
    )
    sa = r.json()
    pods = []
    for i in range(n_pods):
        r = client.post(
            f"/service-accounts/{sa['id']}/pods",
            json={"name": f"pod-{i}", "namespace": "ns"},
        )
        pods.append(r.json())
    return sa, pods


def test_rotation_creates_new_gen_and_supersedes_old(client):
    sa, _ = _setup(client, n_pods=1)
    old_token = sa["generations"][0]["token"]

    r = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    )
    assert r.status_code == 201, r.text
    rotation = r.json()
    assert rotation["old_generation"] == 1
    assert rotation["new_generation"] == 2
    assert rotation["status"] == "pending"
    assert rotation["old_generation_status"] == "superseded"
    assert rotation["old_generation_expires_at"] is not None
    assert len(rotation["pending_pods"]) == 1

    detail = client.get(f"/service-accounts/{sa['id']}").json()
    assert detail["current_gen"] == 2
    statuses = {g["generation"]: g["status"] for g in detail["generations"]}
    assert statuses == {1: "superseded", 2: "active"}

    # Old token still validates inside the grace window.
    assert client.post("/verify", json={"token": old_token}).json()["valid"] is True


def test_old_generation_revoked_only_after_all_alive_pods_confirm(client):
    sa, pods = _setup(client, n_pods=2)
    old_token = sa["generations"][0]["token"]

    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    rid = rotation["id"]

    # First pod confirms: rotation stays pending, old token stays valid.
    r = client.post(
        f"/rotations/{rid}/confirm?pod_id={pods[0]['id']}",
        json={"generation": 2},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "pending"
    assert len(body["confirmed_pods"]) == 1
    assert [p["id"] for p in body["pending_pods"]] == [pods[1]["id"]]
    assert client.post("/verify", json={"token": old_token}).json()["valid"] is True

    # Second pod confirms: rotation completes and old generation is revoked.
    r = client.post(
        f"/rotations/{rid}/confirm?pod_id={pods[1]['id']}",
        json={"generation": 2},
    )
    body = r.json()
    assert body["status"] == "completed"
    assert body["pending_pods"] == []
    assert body["old_generation_status"] == "revoked"

    verify = client.post("/verify", json={"token": old_token}).json()
    assert verify["valid"] is False
    assert verify["reason"] == "token generation has been revoked"

    detail = client.get(f"/service-accounts/{sa['id']}").json()
    assert {p["name"]: p["last_committed_gen"] for p in detail["pods"]} == {
        pod["name"]: 2 for pod in pods
    }


def test_dead_pod_does_not_block_quorum(client):
    sa, pods = _setup(client, n_pods=2)
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()

    # pod-1 goes down; only the alive pod needs to confirm.
    r = client.post(f"/pods/{pods[1]['id']}/alive?alive=false")
    assert r.json()["alive"] is False

    r = client.post(
        f"/rotations/{rotation['id']}/confirm?pod_id={pods[0]['id']}",
        json={"generation": 2},
    )
    assert r.json()["status"] == "completed"


def test_confirm_unknown_rotation_404(client):
    sa, pods = _setup(client, n_pods=1)
    r = client.post(
        "/rotations/9999/confirm?pod_id=" + str(pods[0]["id"]),
        json={"generation": 2},
    )
    assert r.status_code == 404


def test_confirm_from_foreign_pod_rejected(client):
    sa1, pods1 = _setup(client, n_pods=1)
    sa2 = client.post(
        "/service-accounts",
        json={"name": "other", "namespace": "ns", "description": ""},
    ).json()
    foreign = client.post(
        f"/service-accounts/{sa2['id']}/pods",
        json={"name": "x", "namespace": "ns"},
    ).json()
    rotation = client.post(
        f"/service-accounts/{sa1['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    r = client.post(
        f"/rotations/{rotation['id']}/confirm?pod_id={foreign['id']}",
        json={"generation": 2},
    )
    assert r.status_code == 409
