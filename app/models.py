"""Pydantic request / response schemas."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

GenStatus = Literal["active", "superseded", "revoked"]
RotationStatus = Literal["pending", "completed", "superseded", "expired"]


# ---------- requests ----------

class ServiceAccountCreate(BaseModel):
    name: str = Field(min_length=1, max_length=253, examples=["metrics-reader"])
    namespace: str = Field(min_length=1, max_length=253, examples=["monitoring"])
    description: str = Field(default="", max_length=1024)


class PodCreate(BaseModel):
    name: str = Field(min_length=1, max_length=253, examples=["metrics-reader-7d9f4"])
    namespace: str = Field(min_length=1, max_length=253, examples=["monitoring"])


class RotationCreate(BaseModel):
    # Grace period during which the superseded generation still validates.
    grace_seconds: int = Field(default=300, ge=1, le=7 * 24 * 3600)


class ConfirmRequest(BaseModel):
    # Generation the pod claims to have loaded. Stale generations are rejected.
    generation: int = Field(ge=1)


class VerifyRequest(BaseModel):
    token: str = Field(min_length=1)


# ---------- responses ----------

class TokenGenerationOut(BaseModel):
    generation: int
    token: str
    status: GenStatus
    created_at: float
    expires_at: Optional[float]
    superseded_at: Optional[float]
    revoked_at: Optional[float]
    revoke_reason: str


class PodOut(BaseModel):
    id: int
    name: str
    namespace: str
    alive: bool
    last_committed_gen: int
    created_at: float
    updated_at: float


class ServiceAccountOut(BaseModel):
    id: int
    name: str
    namespace: str
    description: str
    current_gen: int
    created_at: float
    generations: list[TokenGenerationOut]
    pods: list[PodOut]


class ServiceAccountSummary(BaseModel):
    id: int
    name: str
    namespace: str
    description: str
    current_gen: int
    created_at: float


class RotationOut(BaseModel):
    id: int
    sa_id: int
    old_generation: int
    new_generation: int
    status: RotationStatus
    grace_seconds: int
    created_at: float
    completed_at: Optional[float]
    finished_reason: str
    confirmed_pods: list[PodOut]
    pending_pods: list[PodOut]
    old_generation_status: GenStatus
    old_generation_expires_at: Optional[float]


class RestartResult(BaseModel):
    pod: PodOut
    # The pod keeps the last generation it had committed; it re-mounts that
    # token and must re-confirm any still-pending rotation.
    reused_generation: int
    token: str
    needs_confirmation_for_rotation: Optional[int]


class VerifyResponse(BaseModel):
    valid: bool
    reason: str
    service_account: Optional[str] = None
    namespace: Optional[str] = None
    generation: Optional[int] = None


class Message(BaseModel):
    message: str
