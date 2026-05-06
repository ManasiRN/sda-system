"""
api/routes/admin.py — API key lifecycle management (create / list / revoke).

All endpoints require X-Admin-Key header matching ADMIN_API_KEY from config.
Raw keys are returned ONLY on creation; only the SHA-256 hash is stored.
"""

import hashlib
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

_admin_key_scheme = APIKeyHeader(name="X-Admin-Key", auto_error=False)

from ...config import settings
from ...db.models import APIKey
from ...db.session import get_db

router = APIRouter(prefix="/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class APIKeyResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: Optional[str]
    key_hash_prefix: str
    is_active: bool
    rate_limit_per_min: int
    created_at: datetime
    last_used_at: Optional[datetime] = None


class APIKeyCreateRequest(BaseModel):
    name: str
    rate_limit_per_min: int = 100


class APIKeyCreateResponse(APIKeyResponse):
    raw_key: str


class APIKeyUpdateRequest(BaseModel):
    rate_limit_per_min: Optional[int] = None
    is_active: Optional[bool] = None


# ---------------------------------------------------------------------------
# Admin authentication dependency
# ---------------------------------------------------------------------------

def _require_admin(x_admin_key: Optional[str] = Depends(_admin_key_scheme)) -> None:
    """
    Verify X-Admin-Key header against ADMIN_API_KEY config value.

    Uses secrets.compare_digest to prevent timing-oracle attacks.
    """
    if not x_admin_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Admin-Key header",
        )
    expected = settings.ADMIN_API_KEY.get_secret_value()
    if not secrets.compare_digest(x_admin_key, expected):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin key",
        )


def _to_response(key: APIKey) -> APIKeyResponse:
    return APIKeyResponse(
        id=key.id,
        name=key.name,
        key_hash_prefix=key.key_hash[:8] + "...",
        is_active=key.is_active,
        rate_limit_per_min=key.rate_limit_per_min,
        created_at=key.created_at,
        last_used_at=key.last_used_at,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api-keys", response_model=List[APIKeyResponse])
def list_api_keys(
    db: Session = Depends(get_db),
    _: None = Depends(_require_admin),
) -> List[APIKeyResponse]:
    """List all API keys. Raw key values are never returned."""
    keys = db.query(APIKey).order_by(APIKey.created_at.desc()).all()
    return [_to_response(k) for k in keys]


@router.post("/api-keys", response_model=APIKeyCreateResponse, status_code=status.HTTP_201_CREATED)
def create_api_key(
    payload: APIKeyCreateRequest,
    db: Session = Depends(get_db),
    _: None = Depends(_require_admin),
) -> APIKeyCreateResponse:
    """
    Create a new API key.

    The raw key is returned exactly once in this response.
    It is NOT stored — only the SHA-256 hash is persisted.
    Record the raw key immediately; it cannot be recovered later.
    """
    raw_key  = secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    key = APIKey(
        key_hash=key_hash,
        name=payload.name,
        is_active=True,
        rate_limit_per_min=payload.rate_limit_per_min,
        created_at=datetime.now(timezone.utc),
    )
    db.add(key)
    db.commit()
    db.refresh(key)

    return APIKeyCreateResponse(
        id=key.id,
        name=key.name,
        key_hash_prefix=key.key_hash[:8] + "...",
        is_active=key.is_active,
        rate_limit_per_min=key.rate_limit_per_min,
        created_at=key.created_at,
        last_used_at=key.last_used_at,
        raw_key=raw_key,
    )


@router.patch("/api-keys/{key_id}", response_model=APIKeyResponse)
def update_api_key(
    key_id: int,
    payload: APIKeyUpdateRequest,
    db: Session = Depends(get_db),
    _: None = Depends(_require_admin),
) -> APIKeyResponse:
    """Update rate limit or active status of an existing key."""
    key = db.query(APIKey).filter(APIKey.id == key_id).first()
    if not key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"API key {key_id} not found",
        )

    if payload.rate_limit_per_min is not None:
        key.rate_limit_per_min = payload.rate_limit_per_min
    if payload.is_active is not None:
        key.is_active = payload.is_active

    db.commit()
    db.refresh(key)
    return _to_response(key)


@router.post("/tasks/ingest-tles", status_code=status.HTTP_202_ACCEPTED)
async def trigger_ingest_tles(
    background_tasks: BackgroundTasks,
    _: None = Depends(_require_admin),
) -> Dict[str, Any]:
    """
    Trigger TLE ingestion directly in the API process (no Celery worker needed).
    Returns immediately; ingestion runs in the background (~20-40s).
    Check /health afterwards to confirm TLE data is present.
    """
    from ...ingestion.fetcher import TLEFetcher
    from ...db.session import SessionLocal

    async def _run_ingest() -> None:
        db = SessionLocal()
        try:
            fetcher = TLEFetcher(db)
            fetcher.init_ground_stations()
            await fetcher.fetch_all()
        finally:
            db.close()

    background_tasks.add_task(_run_ingest)
    return {"status": "accepted", "message": "TLE ingestion started — check /health in ~60s"}


@router.delete("/api-keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_api_key(
    key_id: int,
    db: Session = Depends(get_db),
    _: None = Depends(_require_admin),
) -> None:
    """
    Revoke (deactivate) an API key.

    Sets is_active=False. The key is retained in the DB for audit purposes.
    Reactivate via PATCH /admin/api-keys/{key_id} if revoked in error.
    """
    key = db.query(APIKey).filter(APIKey.id == key_id).first()
    if not key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"API key {key_id} not found",
        )

    key.is_active = False
    db.commit()
