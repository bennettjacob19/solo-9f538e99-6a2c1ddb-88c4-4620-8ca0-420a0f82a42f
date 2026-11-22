"""Pytest fixtures: isolated temp DB, controllable clock, TestClient."""
from __future__ import annotations

import importlib
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Make the project root importable when tests run from any cwd.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


class FakeClock:
    """Deterministic replacement for service._now."""

    def __init__(self, start: float = 1_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def env(monkeypatch, clock):
    tmp = tempfile.mkdtemp()
    monkeypatch.setenv("TOKEN_ROTATION_DB", str(Path(tmp) / "test.db"))
    monkeypatch.setenv("SWEEP_INTERVAL_SECONDS", "3600")  # don't sweep in tests

    # Fresh import so module-level config picks up the env vars.
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    main = importlib.import_module("app.main")

    with TestClient(main.app) as client:
        service = client.app.state.service
        service._now = clock  # deterministic time for the whole service
        yield {
            "client": client,
            "service": service,
            "clock": clock,
        }


@pytest.fixture()
def client(env):
    return env["client"]


@pytest.fixture()
def service(env):
    return env["service"]
