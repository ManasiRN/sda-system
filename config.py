"""Application configuration — loaded once at import time.

Secrets (DATABASE_URL, API_KEY_SALT) use SecretStr so they are masked
as '**********' in repr(), logs, and error tracebacks.

Production safety: if ENVIRONMENT=production is set and any insecure
default value is still in place, the process refuses to start.
"""
import json
import os
from typing import Any, Dict, List

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# ---------------------------------------------------------------------------
# Ground station data — loaded from stations.json for easy editing.
# Falls back to an empty list if the file is missing (tests, CI).
# ---------------------------------------------------------------------------
def _load_stations() -> List[Dict[str, Any]]:
    path = os.path.join(os.path.dirname(__file__), "stations.json")
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []

_GROUND_STATIONS: List[Dict[str, Any]] = _load_stations()


class Settings(BaseSettings):
    """
    All runtime configuration.

    Fields are read from environment variables (case-sensitive) and from a
    .env file in the working directory.  SecretStr fields are never echoed
    in repr(), log lines, or Pydantic validation errors.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",        # silently drop unrecognised env vars
    )

    # ── Deployment environment ───────────────────────────────────────────────
    ENVIRONMENT: str = Field(default="development", description="development | staging | production")
    LOG_LEVEL: str = Field(default="INFO")

    # ── Secrets — masked in repr/logs ────────────────────────────────────────
    # Dev defaults are provided so docker-compose works out of the box.
    # Production MUST override these; the startup validator below enforces it.
    DATABASE_URL: SecretStr = Field(
        default=SecretStr("postgresql://sda_user:sda_pass@postgres:5432/sda_db"),
        description="Full Postgres DSN including credentials",
    )
    API_KEY_SALT: SecretStr = Field(
        default=SecretStr("sda-secret-salt-2024"),
        description="Secret salt for HMAC API key hashing — change in production",
    )

    # ── Infrastructure ───────────────────────────────────────────────────────
    DATABASE_POOL_SIZE: int = Field(default=20)
    REDIS_URL: str = Field(default="redis://redis:6379/0")
    REDIS_CACHE_TTL: int = Field(default=3600, description="Cache TTL in seconds")
    CELERY_BROKER_URL: str = Field(default="redis://redis:6379/1")
    CELERY_RESULT_BACKEND: str = Field(default="redis://redis:6379/2")

    # ── TLE ingestion ────────────────────────────────────────────────────────
    CELESTRAK_URL: str = Field(
        default="https://celestrak.org/NORAD/elements/gp.php?GROUP=active"
    )
    TLE_FETCH_INTERVAL_HOURS: int = Field(default=6)
    TLE_VALID_DAYS: int = Field(default=7)

    # ── Propagation ──────────────────────────────────────────────────────────
    PROPAGATION_STEP_SECONDS: int = Field(default=10, description="Coarse scan step")
    PROPAGATION_DAYS: int = Field(default=7)
    REFINEMENT_PRECISION_SECONDS: int = Field(default=1)

    # ── Visibility / pass detection ──────────────────────────────────────────
    MIN_ELEVATION_DEG: float = Field(default=10.0)
    MIN_PASS_DURATION_SECONDS: int = Field(default=5)
    EARTH_RADIUS_KM: float = Field(default=6371.0)

    # ── Scheduling ───────────────────────────────────────────────────────────
    SCHEDULER_GREEDY_WEIGHT: str = Field(default="elevation_x_duration")
    ORTOOLS_TIME_LIMIT_SECONDS: int = Field(default=30)

    # ── API ──────────────────────────────────────────────────────────────────
    API_HOST: str = Field(default="0.0.0.0")
    API_PORT: int = Field(default=8000)
    API_KEY_REQUIRED: bool = Field(default=False)
    # Accepts either a JSON array or a comma-separated string in the env var:
    #   API_KEYS='["prod-key-abc","prod-key-xyz"]'   ← JSON
    #   API_KEYS=prod-key-abc,prod-key-xyz            ← CSV
    API_KEYS: List[str] = Field(default_factory=lambda: ["dev-key-123"])

    # ── Performance ──────────────────────────────────────────────────────────
    BATCH_SIZE: int = Field(default=100)
    MAX_WORKERS: int = Field(default=4)

    # ── Admin authentication ─────────────────────────────────────────────────
    ADMIN_API_KEY: SecretStr = Field(
        default=SecretStr("dev-admin-key-change-in-prod"),
        description="Secret key for /admin/* endpoints — must be overridden in production",
    )

    # ── CORS (production) ────────────────────────────────────────────────────
    CORS_ORIGINS: List[str] = Field(
        default_factory=list,
        description="Allowed CORS origins for production — e.g. ['https://dashboard.example.com']",
    )

    # ── Monitoring (Flower) ──────────────────────────────────────────────────
    FLOWER_BASIC_AUTH: str = Field(
        default="admin:flower-dev-pass",
        description="Flower dashboard basic-auth in 'user:password' format",
    )

    # ── Auto pipeline (Railway / single-process deployments) ────────────────
    AUTO_PIPELINE_ENABLED: bool = Field(
        default=False,
        description="Run ingest + pipeline automatically every TLE_FETCH_INTERVAL_HOURS (no Celery needed)",
    )
    AUTO_PIPELINE_LIMIT: int = Field(
        default=5000,
        description="Max satellites per auto pipeline run",
    )

    # ── TLE staleness alerting ───────────────────────────────────────────────
    TLE_STALE_ALERT_HOURS: int = Field(
        default=24,
        description="Health check degrades if last TLE ingest is older than this many hours",
    )

    # ── TLE fallback sources ─────────────────────────────────────────────────
    TLE_FALLBACK_URLS: List[str] = Field(
        default_factory=list,
        description="Fallback TLE source URLs tried in order when CELESTRAK_URL fails",
    )

    # ── Ground stations (static — not read from env) ─────────────────────────
    GROUND_STATIONS: List[Dict[str, Any]] = Field(
        default_factory=lambda: list(_GROUND_STATIONS),
    )

    # ── Validators ───────────────────────────────────────────────────────────

    @field_validator("API_KEYS", mode="before")
    @classmethod
    def _parse_api_keys(cls, v: Any) -> List[str]:
        """Accept both JSON array and CSV string from the environment."""
        if isinstance(v, str):
            v = v.strip()
            if v.startswith("["):
                import json
                return json.loads(v)
            return [k.strip() for k in v.split(",") if k.strip()]
        return v

    @field_validator("CORS_ORIGINS", "TLE_FALLBACK_URLS", mode="before")
    @classmethod
    def _parse_str_list(cls, v: Any) -> List[str]:
        """Accept both JSON array and CSV string from the environment."""
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            if v.startswith("["):
                import json
                return json.loads(v)
            return [x.strip() for x in v.split(",") if x.strip()]
        return v

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _normalise_log_level(cls, v: str) -> str:
        return v.upper()

    @model_validator(mode="after")
    def _refuse_insecure_defaults_in_production(self) -> "Settings":
        """Hard-fail at startup if production is deployed with dev secrets."""
        if self.ENVIRONMENT.lower() != "production":
            return self

        problems: List[str] = []
        db_url = self.DATABASE_URL.get_secret_value()
        if "sda_pass" in db_url or "localhost" in db_url or "127.0.0.1" in db_url:
            problems.append("DATABASE_URL contains dev credentials or localhost")
        if self.API_KEY_SALT.get_secret_value() == "sda-secret-salt-2024":
            problems.append("API_KEY_SALT is still the public default value")
        if "dev-key-123" in self.API_KEYS:
            problems.append("API_KEYS contains the insecure dev-key-123 value")
        if self.ADMIN_API_KEY.get_secret_value() == "dev-admin-key-change-in-prod":
            problems.append("ADMIN_API_KEY is still the public default value")

        if problems:
            raise ValueError(
                "Refusing to start in production with insecure configuration:\n"
                + "\n".join(f"  • {p}" for p in problems)
                + "\nSet the above values in your environment or secrets manager."
            )
        return self

    # ── Backward-compatibility aliases (properties, not duplicate fields) ────
    # These keep old call-sites working without duplicating state that can drift.

    @property
    def ELEVATION_MASK_DEG(self) -> float:
        return self.MIN_ELEVATION_DEG

    @property
    def MIN_PASS_DURATION_SEC(self) -> int:
        return self.MIN_PASS_DURATION_SECONDS

    @property
    def PROPAGATION_STEP_SEC(self) -> int:
        return self.PROPAGATION_STEP_SECONDS

    @property
    def ORTOOLS_TIME_LIMIT_SEC(self) -> int:
        return self.ORTOOLS_TIME_LIMIT_SECONDS

    @property
    def ORTOOLS_NUM_WORKERS(self) -> int:
        return self.MAX_WORKERS

    @property
    def RATE_LIMIT_REQUESTS_PER_MIN(self) -> int:
        return 100

    @property
    def API_RATE_LIMIT(self) -> str:
        return "100/minute"

    def get_station_by_id(self, station_id: str) -> Dict[str, Any]:
        for station in self.GROUND_STATIONS:
            if station["id"] == station_id:
                return station
        raise ValueError(f"Station {station_id} not found")


config = Settings()
settings = config  # alias — supports both `from config import config` and `from ..config import settings`
