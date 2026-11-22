"""Registration and basic read endpoints."""


def _create_sa(client, name="metrics-reader", namespace="monitoring"):
    r = client.post(
        "/service-accounts",
        json={"name": name, "namespace": namespace, "description": "d"},
    )
    assert r.status_code == 201, r.text
    return r.json()


def _create_pod(client, sa_id, name):
    r = client.post(
        f"/service-accounts/{sa_id}/pods",
        json={"name": name, "namespace": "monitoring"},
    )
    assert r.status_code == 201, r.text
    return r.json()


def test_register_sa_issues_generation_one(client):
    sa = _create_sa(client)
    assert sa["current_gen"] == 1
    assert len(sa["generations"]) == 1
    gen1 = sa["generations"][0]
    assert gen1["status"] == "active"
    assert gen1["token"].startswith("k8s-sat-")


def test_duplicate_sa_conflict(client):
    _create_sa(client)
    r = client.post(
        "/service-accounts",
        json={"name": "metrics-reader", "namespace": "monitoring"},
    )
    assert r.status_code == 409


def test_register_pod_mounts_current_generation(client):
    sa = _create_sa(client)
    pod = _create_pod(client, sa["id"], "pod-a")
    assert pod["name"] == "pod-a"
    assert pod["namespace"] == "monitoring"
    assert pod["last_committed_gen"] == 1
    assert pod["alive"] is True


def test_duplicate_pod_conflict(client):
    sa = _create_sa(client)
    _create_pod(client, sa["id"], "pod-a")
    r = client.post(
        f"/service-accounts/{sa['id']}/pods",
        json={"name": "pod-a", "namespace": "monitoring"},
    )
    assert r.status_code == 409


def test_verify_active_token(client):
    sa = _create_sa(client)
    token = sa["generations"][0]["token"]
    r = client.post("/verify", json={"token": token})
    assert r.status_code == 200
    body = r.json()
    assert body["valid"] is True
    assert body["generation"] == 1
    assert body["service_account"] == "metrics-reader"


def test_verify_unknown_token(client):
    r = client.post("/verify", json={"token": "k8s-sat-does-not-exist"})
    assert r.json()["valid"] is False
    assert r.json()["reason"] == "unknown token"


def test_health_and_docs(client):
    assert client.get("/health").json() == {"message": "ok"}
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200
