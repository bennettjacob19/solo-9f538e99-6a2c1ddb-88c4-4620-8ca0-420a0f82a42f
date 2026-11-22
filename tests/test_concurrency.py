"""Concurrent interleavings: rotation vs confirmations vs restarts."""
import threading

from concurrent.futures import ThreadPoolExecutor


def _setup(client, n_pods):
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


def test_concurrent_confirmations_complete_exactly_once(client):
    sa, pods = _setup(client, n_pods=8)
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    rid = rotation["id"]

    barrier = threading.Barrier(len(pods))

    def confirm(pod):
        barrier.wait()
        return client.post(
            f"/rotations/{rid}/confirm?pod_id={pod['id']}",
            json={"generation": 2},
        ).status_code

    with ThreadPoolExecutor(max_workers=len(pods)) as pool:
        codes = list(pool.map(confirm, pods))

    assert codes == [200] * len(pods)
    body = client.get(f"/rotations/{rid}").json()
    assert body["status"] == "completed"
    assert len(body["confirmed_pods"]) == len(pods)
    assert body["pending_pods"] == []

    detail = client.get(f"/service-accounts/{sa['id']}").json()
    old = next(g for g in detail["generations"] if g["generation"] == 1)
    new = next(g for g in detail["generations"] if g["generation"] == 2)
    assert old["status"] == "revoked"
    assert new["status"] == "active"


def test_rotation_restart_confirm_interleaved_never_revokes_new_token(client):
    sa, pods = _setup(client, n_pods=4)
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    rid = rotation["id"]
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    gen2_token = next(g for g in detail["generations"] if g["generation"] == 2)[
        "token"
    ]

    errors = []

    def worker(i):
        try:
            pod = pods[i]
            if i % 2 == 0:
                # Restart races with the other pods' confirmations.
                client.post(f"/pods/{pod['id']}/restart")
            r = client.post(
                f"/rotations/{rid}/confirm?pod_id={pod['id']}",
                json={"generation": 2},
            )
            assert r.status_code == 200, r.text
        except Exception as exc:  # pragma: no cover - failure diagnostics
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(worker, range(4)))

    assert not errors, errors

    body = client.get(f"/rotations/{rid}").json()
    assert body["status"] == "completed"

    # The new token must remain valid regardless of interleaving.
    verify = client.post("/verify", json={"token": gen2_token}).json()
    assert verify["valid"] is True
    assert verify["generation"] == 2


def test_concurrent_rotations_serialize_to_distinct_generations(client):
    sa, pods = _setup(client, n_pods=1)

    def start(_):
        return client.post(
            f"/service-accounts/{sa['id']}/rotations",
            json={"grace_seconds": 300},
        ).json()

    with ThreadPoolExecutor(max_workers=5) as pool:
        rotations = list(pool.map(start, range(5)))

    new_gens = sorted(r["new_generation"] for r in rotations)
    assert new_gens == [2, 3, 4, 5, 6]

    # Responses are point-in-time snapshots; check the final persisted state.
    final = client.get(f"/rotations?sa_id={sa['id']}").json()
    statuses = [r["status"] for r in final]
    # Only the last rotation stays pending; earlier ones were superseded.
    assert statuses.count("pending") == 1
    assert statuses.count("superseded") == 4

    detail = client.get(f"/service-accounts/{sa['id']}").json()
    assert detail["current_gen"] == 6
    # Exactly one active generation at any time.
    active = [g for g in detail["generations"] if g["status"] == "active"]
    assert len(active) == 1 and active[0]["generation"] == 6


def test_confirm_while_restart_loop_runs_eventually_consistent(client):
    sa, pods = _setup(client, n_pods=2)
    rotation = client.post(
        f"/service-accounts/{sa['id']}/rotations", json={"grace_seconds": 300}
    ).json()
    rid = rotation["id"]
    stop = threading.Event()

    def restart_loop(pod):
        while not stop.is_set():
            client.post(f"/pods/{pod['id']}/restart")

    t = threading.Thread(target=restart_loop, args=(pods[1],))
    t.start()
    try:
        # Pod 0 confirms cleanly.
        assert (
            client.post(
                f"/rotations/{rid}/confirm?pod_id={pods[0]['id']}",
                json={"generation": 2},
            ).status_code
            == 200
        )
        # Keep trying pod 1 until a confirmation sticks between restarts.
        for _ in range(100):
            r = client.post(
                f"/rotations/{rid}/confirm?pod_id={pods[1]['id']}",
                json={"generation": 2},
            )
            if r.status_code == 200 and r.json()["status"] == "completed":
                break
        else:  # pragma: no cover
            raise AssertionError("rotation never completed under restart churn")
    finally:
        stop.set()
        t.join()

    body = client.get(f"/rotations/{rid}").json()
    assert body["status"] == "completed"
    # Completion is durable: a later restart cannot undo the revocation.
    client.post(f"/pods/{pods[1]['id']}/restart")
    detail = client.get(f"/service-accounts/{sa['id']}").json()
    assert detail["generations"][0]["status"] == "revoked"
