"""Application configuration — loaded once at import time.

Secrets (DATABASE_URL, API_KEY_SALT) use SecretStr so they are masked
as '**********' in repr(), logs, and error tracebacks.

Production safety: if ENVIRONMENT=production is set and any insecure
default value is still in place, the process refuses to start.
"""
from typing import Any, Dict, List

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# ---------------------------------------------------------------------------
# Ground station data — static infrastructure, not an env var.
# Defined at module level so the Settings default_factory can reference it
# without a lambda closure capturing a mutable list.
# ---------------------------------------------------------------------------
_GROUND_STATIONS: List[Dict[str, Any]] = [
    # Polar / High Latitude
    {"id": "GS001", "name": "Svalbard Satellite Station",  "latitude":  78.2290, "longitude":  15.6230, "altitude_m":  500},
    {"id": "GS002", "name": "Troll Satellite Station",     "latitude": -72.0117, "longitude":   2.5347, "altitude_m": 1275},
    {"id": "GS003", "name": "Fairbanks, Alaska",           "latitude":  64.8378, "longitude": -147.7164,"altitude_m":  136},
    {"id": "GS004", "name": "Tromsø, Norway",              "latitude":  69.6492, "longitude":  18.9553, "altitude_m":   10},
    {"id": "GS005", "name": "McMurdo Station",             "latitude": -77.8465, "longitude": 166.6680, "altitude_m":   10},
    {"id": "GS006", "name": "Thule Air Base",              "latitude":  76.5367, "longitude": -68.7033, "altitude_m":   77},
    {"id": "GS007", "name": "Poker Flat, Alaska",          "latitude":  65.1262, "longitude": -147.4345,"altitude_m":  165},
    {"id": "GS008", "name": "Esrange, Sweden",             "latitude":  67.8556, "longitude":  20.9548, "altitude_m":  328},
    # Major Ground Stations
    {"id": "GS009", "name": "Kourou, French Guiana",       "latitude":   5.1600, "longitude": -52.6500, "altitude_m":    0},
    {"id": "GS010", "name": "Malindi, Kenya",              "latitude":  -2.9900, "longitude":  40.1900, "altitude_m":    0},
    {"id": "GS011", "name": "Canberra, Australia",         "latitude": -35.2809, "longitude": 149.1300, "altitude_m":  580},
    {"id": "GS012", "name": "Santiago, Chile",             "latitude": -33.4489, "longitude": -70.6693, "altitude_m":  570},
    {"id": "GS013", "name": "Hawaii, USA",                 "latitude":  19.8968, "longitude": -155.5828,"altitude_m":    0},
    {"id": "GS014", "name": "Dongara, Australia",          "latitude": -29.2667, "longitude": 114.9333, "altitude_m":    0},
    # European Network
    {"id": "GS015", "name": "London, UK",                  "latitude":  51.5074, "longitude":  -0.1278, "altitude_m":   35},
    {"id": "GS016", "name": "Paris, France",               "latitude":  48.8566, "longitude":   2.3522, "altitude_m":   35},
    {"id": "GS017", "name": "Berlin, Germany",             "latitude":  52.5200, "longitude":  13.4050, "altitude_m":   34},
    {"id": "GS018", "name": "Rome, Italy",                 "latitude":  41.9028, "longitude":  12.4964, "altitude_m":   20},
    {"id": "GS019", "name": "Madrid, Spain",               "latitude":  40.4168, "longitude":  -3.7038, "altitude_m":  667},
    {"id": "GS020", "name": "Moscow, Russia",              "latitude":  55.7558, "longitude":  37.6173, "altitude_m":  156},
    {"id": "GS021", "name": "Stockholm, Sweden",           "latitude":  59.3293, "longitude":  18.0686, "altitude_m":   15},
    {"id": "GS022", "name": "Helsinki, Finland",           "latitude":  60.1695, "longitude":  24.9354, "altitude_m":   25},
    # North American Network
    {"id": "GS023", "name": "New York, USA",               "latitude":  40.7128, "longitude": -74.0060, "altitude_m":   10},
    {"id": "GS024", "name": "Los Angeles, USA",            "latitude":  34.0522, "longitude": -118.2437,"altitude_m":   71},
    {"id": "GS025", "name": "Toronto, Canada",             "latitude":  43.6532, "longitude": -79.3832, "altitude_m":   76},
    {"id": "GS026", "name": "Vancouver, Canada",           "latitude":  49.2827, "longitude": -123.1207,"altitude_m":    0},
    {"id": "GS027", "name": "Mexico City, Mexico",         "latitude":  19.4326, "longitude": -99.1332, "altitude_m": 2240},
    # South American Network
    {"id": "GS028", "name": "Sao Paulo, Brazil",           "latitude": -23.5505, "longitude": -46.6333, "altitude_m":  760},
    {"id": "GS029", "name": "Buenos Aires, Argentina",     "latitude": -34.6037, "longitude": -58.3816, "altitude_m":   25},
    {"id": "GS030", "name": "Lima, Peru",                  "latitude": -12.0464, "longitude": -77.0428, "altitude_m":  155},
    {"id": "GS031", "name": "Bogota, Colombia",            "latitude":   4.7110, "longitude": -74.0721, "altitude_m": 2640},
    # Asian Network
    {"id": "GS032", "name": "Tokyo, Japan",                "latitude":  35.6895, "longitude": 139.6917, "altitude_m":   40},
    {"id": "GS033", "name": "Beijing, China",              "latitude":  39.9042, "longitude": 116.4074, "altitude_m":   45},
    {"id": "GS034", "name": "Seoul, South Korea",          "latitude":  37.5665, "longitude": 126.9780, "altitude_m":   38},
    {"id": "GS035", "name": "Delhi, India",                "latitude":  28.7041, "longitude":  77.1025, "altitude_m":  216},
    {"id": "GS036", "name": "Singapore",                   "latitude":   1.3521, "longitude": 103.8198, "altitude_m":   15},
    {"id": "GS037", "name": "Bangkok, Thailand",           "latitude":  13.7367, "longitude": 100.5231, "altitude_m":    0},
    {"id": "GS038", "name": "Jakarta, Indonesia",          "latitude":  -6.2088, "longitude": 106.8456, "altitude_m":    8},
    # Middle East & Africa
    {"id": "GS039", "name": "Dubai, UAE",                  "latitude":  25.2769, "longitude":  55.2962, "altitude_m":    0},
    {"id": "GS040", "name": "Cairo, Egypt",                "latitude":  30.0444, "longitude":  31.2357, "altitude_m":   23},
    {"id": "GS041", "name": "Istanbul, Turkey",            "latitude":  41.0082, "longitude":  28.9784, "altitude_m":   39},
    {"id": "GS042", "name": "Cape Town, South Africa",     "latitude": -33.9249, "longitude":  18.4241, "altitude_m":    0},
    {"id": "GS043", "name": "Nairobi, Kenya",              "latitude":  -1.2921, "longitude":  36.8219, "altitude_m": 1795},
    {"id": "GS044", "name": "Lagos, Nigeria",              "latitude":   6.5244, "longitude":   3.3792, "altitude_m":    0},
    # Pacific & Oceania
    {"id": "GS045", "name": "Sydney, Australia",           "latitude": -33.8688, "longitude": 151.2093, "altitude_m":    0},
    {"id": "GS046", "name": "Auckland, New Zealand",       "latitude": -36.8485, "longitude": 174.7633, "altitude_m":    0},
    {"id": "GS047", "name": "Manila, Philippines",         "latitude":  14.5995, "longitude": 120.9842, "altitude_m":    0},
    # Additional Strategic Locations
    {"id": "GS048", "name": "Primrose, Canada",            "latitude":  58.6678, "longitude": -114.6489,"altitude_m":  210},
    {"id": "GS049", "name": "Gdansk, Poland",              "latitude":  54.3520, "longitude":  18.6466, "altitude_m":    0},
    {"id": "GS050", "name": "Andøya, Norway",              "latitude":  69.2944, "longitude":  16.0164, "altitude_m":    0},
]


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
