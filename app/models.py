"""Pydantic request / response schemas."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

GenStatus = Literal["active", "superseded", "revoked"]
RotationStatus = Literal["pending", "completed", "superseded", "expired"]
ImportStatus = Literal["collecting", "active", "superseded"]
PodSource = Literal["manual", "roster"]


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


# ---------- roster import ----------

class RosterImportCreate(BaseModel):
    """Open a roster import round.

    ``namespace`` is optional: when omitted the service account's namespace is
    used. Pages are accepted with the same continuation marker the caller got
    from the previous page (empty for the first page).
    """

    namespace: Optional[str] = Field(
        default=None, max_length=253, examples=["monitoring"]
    )


class RosterPageSubmit(BaseModel):
    """One page of ``kubectl get pods -o json`` output.

    ``pod_list`` is the PodList object itself, including the ``metadata`` map
    that carries ``resourceVersion`` and ``continue``. ``continue_token`` is
    the marker the caller used to fetch this page (empty on the first page)
    and must equal the previous page's ``metadata.continue``.
    """

    continue_token: str = Field(
        default="",
        description="续页标记：取该页时使用的 continue 值（首页为空）",
    )
    pod_list: dict


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
    uid: Optional[str] = None
    source: PodSource = "manual"
    retired: bool = False
    retired_reason: str = ""
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
    active_roster_id: Optional[int] = None
    created_at: float
    generations: list[TokenGenerationOut]
    pods: list[PodOut]


class ServiceAccountSummary(BaseModel):
    id: int
    name: str
    namespace: str
    description: str
    current_gen: int
    active_roster_id: Optional[int] = None
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


class RosterImportOut(BaseModel):
    id: int
    sa_id: int
    status: ImportStatus
    resource_version: Optional[str] = None
    page_count: int
    pod_count: int
    # The continuation marker the next page must be fetched with. Empty means
    # the list has been fully collected and may be activated.
    next_continue: str
    collected_complete: bool
    created_at: float
    activated_at: Optional[float] = None
    finished_reason: str


class RosterImportSummary(BaseModel):
    id: int
    sa_id: int
    status: ImportStatus
    resource_version: Optional[str] = None
    page_count: int
    pod_count: int
    created_at: float
    activated_at: Optional[float] = None
    finished_reason: str


class RosterPageOut(BaseModel):
    import_: RosterImportOut = Field(alias="import")
    accepted: bool
    message: str


class RosterMemberOut(BaseModel):
    pod: PodOut
    uid: str


class RosterOut(BaseModel):
    """Effective roster (or the in-flight collection round when not active)."""

    service_account_id: int
    active_roster_id: Optional[int] = None
    active_since: Optional[float] = None
    source: Literal["manual", "roster"] = "manual"
    members: list[PodOut]
    retired: list[PodOut]
    pending_import: Optional[RosterImportOut] = None


class Message(BaseModel):
    message: str
