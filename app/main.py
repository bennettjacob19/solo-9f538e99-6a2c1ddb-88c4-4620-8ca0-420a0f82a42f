"""FastAPI application: Kubernetes ServiceAccount token rotation API."""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, status

from .database import connect, init_db
from .errors import AppError
from .models import (
    ConfirmRequest,
    Message,
    PodCreate,
    RestartResult,
    RotationCreate,
    RotationOut,
    ServiceAccountCreate,
    ServiceAccountOut,
    ServiceAccountSummary,
    PodOut,
    VerifyRequest,
    VerifyResponse,
)
from .service import RotationService

DB_PATH = os.getenv("TOKEN_ROTATION_DB", "/data/tokens.db")
SWEEP_INTERVAL = float(os.getenv("SWEEP_INTERVAL_SECONDS", "1.0"))

_sweep_task: asyncio.Task | None = None


async def _sweeper(service: RotationService) -> None:
    # Expired superseded generations are also rejected lazily on verify();
    # this loop makes the stored state converge without traffic.
    while True:
        await asyncio.sleep(SWEEP_INTERVAL)
        try:
            await asyncio.to_thread(service.sweep_expired)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive background loop
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(DB_PATH)
    init_db(conn)
    service = RotationService(conn)
    app.state.service = service
    global _sweep_task
    _sweep_task = asyncio.create_task(_sweeper(service))
    try:
        yield
    finally:
        if _sweep_task is not None:
            _sweep_task.cancel()
        conn.close()


app = FastAPI(
    title="Kubernetes ServiceAccount Token Rotation API",
    version="1.0.0",
    description=(
        "轮换在 Pod 持续运行期间进行：旧代令牌在宽限期内仍可验证，"
        "所有存活 Pod 确认装载新代后才撤销旧代。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(AppError)
async def app_error_handler(_request, exc: AppError):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})


# ----------------------------------------------------------- service accounts

@app.post(
    "/service-accounts",
    response_model=ServiceAccountOut,
    status_code=status.HTTP_201_CREATED,
    tags=["service-accounts"],
    summary="登记服务账号（自动签发第 1 代令牌）",
)
def create_sa(body: ServiceAccountCreate):
    return app.state.service.register_sa(body.name, body.namespace, body.description)


@app.get(
    "/service-accounts",
    response_model=list[ServiceAccountSummary],
    tags=["service-accounts"],
    summary="列出服务账号",
)
def list_sas():
    return app.state.service.list_sas()


@app.get(
    "/service-accounts/{sa_id}",
    response_model=ServiceAccountOut,
    tags=["service-accounts"],
    summary="查询服务账号详情（含各代令牌与 Pod）",
)
def get_sa(sa_id: int):
    return app.state.service.get_sa(sa_id)


# ----------------------------------------------------------------------- pods

@app.post(
    "/service-accounts/{sa_id}/pods",
    response_model=PodOut,
    status_code=status.HTTP_201_CREATED,
    tags=["pods"],
    summary="登记使用该服务账号的 Pod（默认装载当前代令牌）",
)
def create_pod(sa_id: int, body: PodCreate):
    return app.state.service.register_pod(sa_id, body.name, body.namespace)


@app.post(
    "/pods/{pod_id}/restart",
    response_model=RestartResult,
    tags=["pods"],
    summary="模拟 Pod 重启/故障恢复：沿用上次已提交代数的令牌",
)
def restart_pod(pod_id: int):
    return app.state.service.restart_pod(pod_id)


@app.post(
    "/pods/{pod_id}/alive",
    response_model=PodOut,
    tags=["pods"],
    summary="标记 Pod 存活/下线（下线后不参与确认计数）",
)
def set_pod_alive(pod_id: int, alive: bool):
    return app.state.service.set_pod_alive(pod_id, alive)


# ------------------------------------------------------------------ rotations

@app.post(
    "/service-accounts/{sa_id}/rotations",
    response_model=RotationOut,
    status_code=status.HTTP_201_CREATED,
    tags=["rotations"],
    summary="发起轮换：签发新代，旧代进入宽限期（superseded）",
)
def start_rotation(sa_id: int, body: RotationCreate):
    return app.state.service.start_rotation(sa_id, body.grace_seconds)


@app.get(
    "/rotations",
    response_model=list[RotationOut],
    tags=["rotations"],
    summary="查询轮换列表（可按服务账号过滤）",
)
def list_rotations(sa_id: int | None = None):
    return app.state.service.list_rotations(sa_id)


@app.get(
    "/rotations/{rotation_id}",
    response_model=RotationOut,
    tags=["rotations"],
    summary="查询轮换状态、已确认/待确认 Pod 与旧代宽限到期时间",
)
def get_rotation(rotation_id: int):
    return app.state.service.get_rotation(rotation_id)


@app.post(
    "/rotations/{rotation_id}/confirm",
    response_model=RotationOut,
    tags=["rotations"],
    summary="Pod 确认已装载新代令牌（过期/旧代确认会被拒绝）",
)
def confirm(rotation_id: int, pod_id: int, body: ConfirmRequest):
    return app.state.service.confirm(rotation_id, pod_id, body.generation)


# -------------------------------------------------------------------- verify

@app.post(
    "/verify",
    response_model=VerifyResponse,
    tags=["verify"],
    summary="验证令牌（宽限期内旧代有效，过期立即拒绝）",
)
def verify_token(body: VerifyRequest):
    return app.state.service.verify_token(body.token)


@app.get("/health", response_model=Message, tags=["meta"])
def health():
    return {"message": "ok"}
