# SDA Pass Scheduling System

A production-grade **Space Domain Awareness** backend that ingests live TLE data, propagates orbital positions with SGP4, predicts visibility passes over 50 ground stations, and maximises unique satellite coverage using a two-stage greedy + OR-Tools CP-SAT scheduler — all served through a secured FastAPI, orchestrated with Celery, and deployed via Docker Compose.

**Live deployment:** https://sda-system-production.up.railway.app/ui  
**API docs:** https://sda-system-production.up.railway.app/docs

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Live Demo — Railway](#live-demo--railway)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Authentication](#authentication)
- [Frontend Dashboard](#frontend-dashboard)
- [Ground Station Configuration](#ground-station-configuration)
- [Monitoring](#monitoring)
- [Pipeline Walkthrough](#pipeline-walkthrough)
- [Scheduling Algorithms](#scheduling-algorithms)
- [Trade-offs and Design Decisions](#trade-offs-and-design-decisions)
- [Performance Characteristics](#performance-characteristics)
- [Production Deployment](#production-deployment)
- [Project Structure](#project-structure)
- [Known Limitations and Future Work](#known-limitations-and-future-work)

---

## Live Demo — Railway

The system is deployed on Railway at:

| | |
|---|---|
| **Dashboard** | https://sda-system-production.up.railway.app/ui |
| **API docs** | https://sda-system-production.up.railway.app/docs |
| **Health** | https://sda-system-production.up.railway.app/health |

### Access the API

Create an API key via the Admin tab in the dashboard (requires `X-Admin-Key`), then:

```bash
curl https://sda-system-production.up.railway.app/api/coverage \
  -H "X-API-Key: <your-key>"
```

### Railway deployment architecture

Railway runs a single `api` service (no Celery workers). The pipeline is triggered manually via admin endpoints instead of Celery Beat:

| Admin Endpoint | What it does |
|---|---|
| `POST /admin/tasks/ingest-tles` | Download fresh TLEs from Celestrak (~30s) |
| `POST /admin/tasks/run-pipeline?limit=N` | SGP4 propagation + pass detection for N satellites |
| `POST /admin/tasks/schedule` | Run greedy scheduler over 7-day window |
| `POST /admin/tasks/debug-pipeline` | Single-satellite diagnostic (synchronous) |

All three steps run as FastAPI background tasks — no worker containers needed. Trigger them in order from the **Admin** tab in the dashboard.

### Current results (Railway)

| Metric | Value |
|---|---|
| TLEs ingested | 15,352 active objects |
| Ground stations | 50 globally distributed |
| Propagation window | 7 days, 30-second steps |
| Visible satellites (5,000 sampled) | 65+ |
| Scheduled | 98%+ coverage |

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                          External Sources                              │
│    https://celestrak.org/NORAD/elements/gp.php?GROUP=active            │
│    (TLE data, ~15,000 active objects, refreshed every 6 h)             │
└────────────────────────────┬───────────────────────────────────────────┘
                             │ HTTPS (Celery Beat, crontab 0 */6 * * *)
                             ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         Ingestion Layer                                │
│   fetcher.py ──► validator.py ──► PostgreSQL (tles)                   │
│   Async HTTP + fallback URLs    TLE checksum + format    ON CONFLICT   │
│                                                          DO NOTHING    │
└────────────────────────────┬───────────────────────────────────────────┘
                             │ detect_passes.delay([norad_id]) per satellite
                             ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        Propagation Layer                               │
│   sgp4_engine.py ──► coordinates.py ──► pass_detector.py              │
│   SGP4, 10 s steps     GCRS→ECEF→ENU     binary-search rise/set to 1 s│
│   7-day window         vectorised numpy   elevation mask ≥ 10 °        │
│                                    │                                   │
│                                    ▼                                   │
│              PostgreSQL  satellite_passes  (ON CONFLICT DO NOTHING)    │
└────────────────────────────┬───────────────────────────────────────────┘
                             │ run_greedy.delay() on completion
                             ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        Scheduling Layer                                │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  Stage 1 — GreedyScheduler (greedy.py)                          │  │
│  │  Weighted-interval DP, O(n log n), cross-station uniqueness      │  │
│  │  → marks is_scheduled=True, scheduled_by='greedy'               │  │
│  └───────────────────────────────┬──────────────────────────────────┘  │
│                                  │ run_ortools.delay(sats, free_slots)  │
│  ┌───────────────────────────────▼──────────────────────────────────┐  │
│  │  Stage 2 — ORToolsOptimizer (ortools_optimizer.py)              │  │
│  │  CP-SAT, 30 s time limit, fills free slots for missed sats       │  │
│  │  → marks is_scheduled=True, scheduled_by='ortools'              │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────────────┐
│                           API Layer                                    │
│   FastAPI ──► Redis (5 min TTL) ──► PostgreSQL                        │
│                                                                        │
│   GET  /api/passes          paginated pass query (cached)              │
│   GET  /api/schedule        scheduled passes per station/date          │
│   GET  /api/coverage        network-wide coverage statistics           │
│   GET  /admin/api-keys      list all API keys (admin only)             │
│   POST /admin/api-keys      create API key, returns raw key once       │
│   PATCH /admin/api-keys/:id update rate limit or active status         │
│   DELETE /admin/api-keys/:id revoke key (soft delete, audit-safe)      │
│   GET  /ui                  web dashboard (static, served by FastAPI)  │
│   WS   /ws/events           WebSocket — live system snapshot every 3 s │
│   GET  /health              DB + Redis + TLE staleness liveness probe  │
│   GET  /metrics             Prometheus scrape endpoint                 │
└────────────────────────────────────────────────────────────────────────┘

Docker Compose service map:
  ┌──────────┐  ┌───────┐  ┌─────────────────┐  ┌──────┐  ┌────────┐
  │ postgres │  │ redis │  │ worker (×2)     │  │ beat │  │ flower │
  │ :5432    │  │ :6379 │  │ concurrency=1   │  │ cron │  │ :5555  │
  └──────────┘  └───────┘  │ queues: all     │  └──────┘  └────────┘
       ▲             ▲      └─────────────────┘
       │             │                │
       └─────────────┴────────────────┘
                             │
                     ┌───────▼──────┐      ┌───────────────┐
                     │  api :8000   │◄─────│ nginx :80/443 │
                     └──────────────┘      │ TLS, rate-lim │
                                           └───────────────┘
```

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| API | FastAPI + Uvicorn | Async, auto-OpenAPI, Pydantic validation |
| ORM | SQLAlchemy 2.x (Mapped[]) | Type-safe models, thread-pool session pattern |
| Database | PostgreSQL 15 | ACID, range partitioning, `ON CONFLICT DO NOTHING` |
| Cache | Redis 7 | LRU pass cache (5 min TTL), rate-limit sorted sets |
| Task queue | Celery 5 + Redis broker | Fan-out propagation, pipeline chaining, Beat cron |
| Orbital mechanics | sgp4 + Skyfield | Brandon Rhodes / Vallado SGP4 — matches AFSPC reference |
| Scheduling stage 1 | Pure Python DP | O(n log n) weighted interval scheduling, no solver overhead |
| Scheduling stage 2 | Google OR-Tools CP-SAT | CP integer programming, `NoOverlap` constraints, 30 s budget |
| Reverse proxy | nginx 1.25 | TLS 1.2/1.3, HSTS, gzip, RFC-1918 admin restriction |
| Monitoring | Celery Flower + Prometheus | Task visibility, request metrics |
| Config | Pydantic BaseSettings | `.env` parsing, `SecretStr` masking, production safety checks |

---

## Quick Start

### Prerequisites

- **Docker Desktop** >= 24.0  
- **Docker Compose** >= 2.20  
- **4 GB RAM** minimum (workers use up to 2 GB each for SGP4 + OR-Tools)

### 1 — Clone and configure

```bash
git clone <repo-url>
cd sda_system
cp .env.example .env   # then edit .env — set the three required secrets below
```

The three values you **must** set before starting:

```dotenv
POSTGRES_PASSWORD=choose-a-strong-password
ADMIN_API_KEY=choose-a-long-random-string
FLOWER_BASIC_AUTH=admin:choose-a-password
```

Everything else has safe development defaults.

### 2 — Generate TLS certificates (first run only)

```bash
bash nginx/generate_certs.sh
```

Self-signed certs are created in `nginx/ssl/`. Replace with CA-signed certs for production (see [Production Deployment](#production-deployment)).

### 3 — Build and start

```bash
docker compose up --build
```

On first boot the `api` and `worker` containers automatically:
1. Create all PostgreSQL tables and indexes
2. Seed 50 ground stations
3. Trigger TLE ingestion from Celestrak
4. Begin SGP4 propagation and scheduling

### 4 — Verify

```bash
# System health (should return HTTP 200)
curl http://localhost:8000/health

# Open the web dashboard
open http://localhost:8000/ui
```

Data is populated progressively. Greedy + OR-Tools scheduling runs automatically as each satellite's passes are detected.

### 5 — Create your first API key

```bash
curl -X POST http://localhost:8000/admin/api-keys \
  -H "X-Admin-Key: <your-ADMIN_API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-client", "rate_limit_per_min": 100}'
```

Save the `raw_key` from the response — it is shown exactly once and never stored.

---

## Configuration

All settings are read from environment variables (or `.env`). Required values are marked with *.

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_PASSWORD` * | — | PostgreSQL password (required, no default) |
| `ADMIN_API_KEY` * | — | Secret for `X-Admin-Key` header (required) |
| `FLOWER_BASIC_AUTH` * | — | `user:password` for Flower UI (required) |
| `API_KEY_SALT` | `dev-secret-salt-2024` | HMAC salt for API key tokens |
| `POSTGRES_DB` | `sda_db` | Database name |
| `POSTGRES_USER` | `sda_user` | Database user |
| `DATABASE_URL` | auto-constructed | Override for external PostgreSQL |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection string |
| `CELERY_BROKER_URL` | `redis://redis:6379/1` | Celery broker |
| `CELERY_RESULT_BACKEND` | `redis://redis:6379/2` | Celery result storage |
| `ENVIRONMENT` | `development` | `production` enables CORS restrictions + safety checks |
| `LOG_LEVEL` | `INFO` | Structlog level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `CORS_ORIGINS` | `[]` | JSON array of allowed origins (production only) |
| `CELESTRAK_URL` | `https://celestrak.org/...` | Primary TLE source |
| `TLE_FALLBACK_URLS` | `[]` | JSON array of fallback TLE URLs if Celestrak is down |
| `TLE_STALE_ALERT_HOURS` | `24` | Hours after which `/health` marks TLE data as stale |
| `MIN_ELEVATION_DEG` | `10.0` | Global default elevation mask — overridden per station in `stations.json` |
| `MIN_PASS_DURATION_SECONDS` | `5` | Discard passes shorter than this |
| `PROPAGATION_STEP_SECONDS` | `10` | SGP4 time step (seconds) |
| `PROPAGATION_DAYS` | `7` | Forward propagation window |
| `ORTOOLS_TIME_LIMIT_SECONDS` | `30` | CP-SAT solver wall-clock budget |
| `MAX_WORKERS` | `4` | CP-SAT parallel search workers |

---

## API Reference

All data endpoints require the `X-API-Key` header. Admin endpoints additionally require `X-Admin-Key`.

### `GET /api/passes`

Paginated satellite passes with optional filters.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `station_id` | string | — | Filter by station ID (e.g. `GS001`) |
| `satellite_id` | int | — | Filter by NORAD ID |
| `start_time` | ISO 8601 | now − 7 days | Window start (defaults to past week) |
| `end_time` | ISO 8601 | start + 7 days | Window end (max 7-day span) |
| `page` | int | 1 | 1-based page number |
| `page_size` | int | 50 | Items per page (max 100) |

```bash
curl "http://localhost:8000/api/passes?station_id=GS001&page=1&page_size=10" \
  -H "X-API-Key: <key>"
```

### `GET /api/schedule`

All scheduled passes for a specific station on a UTC date.

| Parameter | Type | Description |
|---|---|---|
| `station_id` | string | Required — e.g. `GS001` |
| `schedule_date` | YYYY-MM-DD | Required |

```bash
curl "http://localhost:8000/api/schedule?station_id=GS001&schedule_date=2026-05-06" \
  -H "X-API-Key: <key>"
```

### `GET /api/coverage`

Network-wide coverage statistics comparing greedy vs OR-Tools scheduling. No query parameters.

```json
{
  "total_visible_satellites": 31,
  "greedy_scheduled": 30,
  "ortools_scheduled": 10,
  "total_scheduled": 31,
  "coverage_pct": 100.0,
  "greedy_coverage_pct": 96.8,
  "ortools_improvement_pct": 3.2,
  "station_utilization": [...]
}
```

### `GET /health`

Returns HTTP 200 when healthy, HTTP 503 when degraded (safe for Kubernetes readiness probes).

```json
{
  "status": "healthy",
  "timestamp": "2026-05-06T12:00:00+00:00",
  "services": {
    "database": "connected",
    "redis": "connected",
    "tle_data": "current",
    "cache": {"hits": 142, "misses": 8, "hit_ratio": 0.95}
  },
  "last_ingest": "2026-05-06T07:37:28+00:00"
}
```

### Admin Endpoints

All require `X-Admin-Key: <ADMIN_API_KEY>`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/admin/api-keys` | List all keys (hash prefix only, never raw) |
| `POST` | `/admin/api-keys` | Create key — `raw_key` returned exactly once |
| `PATCH` | `/admin/api-keys/{id}` | Update `rate_limit_per_min` or `is_active` |
| `DELETE` | `/admin/api-keys/{id}` | Revoke — sets `is_active=False`, retained for audit |

Raw keys are never stored. Only the SHA-256 hash is persisted. If a key is lost, revoke it and create a new one.

### `WS /ws/events`

WebSocket endpoint — connects without authentication and sends a JSON snapshot every 3 seconds.

```json
{
  "type": "snapshot",
  "timestamp": "2026-05-06T14:00:00+00:00",
  "total_passes": 2101,
  "scheduled_passes": 2101,
  "satellites_tracked": 31,
  "recent_passes_60s": 0,
  "queues": { "ingestion": 0, "propagation": 0, "scheduling": 0 }
}
```

The **⬤ Live** dashboard tab connects automatically and updates in real time.

---

## Ground Station Configuration

Stations are defined in **`stations.json`** at the project root — no code changes needed to add, remove, or reconfigure a station.

Each entry supports:

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique station ID used in all API responses (e.g. `GS001`) |
| `name` | string | Human-readable name shown in the dashboard |
| `latitude` | float | Degrees north (−90 to +90) |
| `longitude` | float | Degrees east (−180 to +180) |
| `altitude_m` | float | Metres above sea level |
| `elevation_mask_deg` | float | Minimum visible elevation (5° open-sky, 10° urban, 15° mountainous) |

**Adding a station:**
```json
{ "id": "GS051", "name": "My New Station", "latitude": 48.8566, "longitude": 2.3522,
  "altitude_m": 35, "elevation_mask_deg": 10.0 }
```
Add the line to `stations.json` and restart the API container — `init_db` seeds it automatically.

---

## Authentication

### X-API-Key (data endpoints)

```
GET /api/passes
X-API-Key: <raw-key-from-POST-/admin/api-keys>
```

Keys are verified by hashing the raw value with SHA-256 and comparing to the stored hash in constant time (`secrets.compare_digest`). Redis caches the result for 5 minutes to avoid a DB round-trip on every request.

### X-Admin-Key (admin endpoints)

```
POST /admin/api-keys
X-Admin-Key: <ADMIN_API_KEY from .env>
```

The admin key is a single shared secret read from `ADMIN_API_KEY` in `.env`. It is never stored in the database. Comparison uses `secrets.compare_digest` to prevent timing oracle attacks.

### Swagger UI

Interactive API documentation with built-in authentication is available at:

```
http://localhost:8000/docs
```

Click the **Authorize** button, enter your `X-API-Key` (for data endpoints) or `X-Admin-Key` (for admin endpoints), then try any endpoint directly from the browser.

---

## Frontend Dashboard

A web dashboard is served at `http://localhost:8000/ui` (no installation required — pure HTML + vanilla JS, no build step).

**Features:**

| Tab | What it shows |
|---|---|
| **Coverage** | Total satellites visible, greedy vs OR-Tools split, per-station utilization progress bars |
| **Passes** | Searchable paginated table — filter by station, NORAD ID, date range; defaults to past 7 days |
| **Schedule** | All scheduled passes for a station on a specific date |
| **API Keys** | Create / revoke / reactivate keys (requires X-Admin-Key) |
| **Health** | Live system status for database, Redis, cache, TLE freshness |
| **⬤ Live** | WebSocket feed — real-time counters (total passes, scheduled, new in 60 s), Celery queue depths, scrolling event log |

Keys entered in the dashboard are saved to `localStorage` so you do not need to re-enter them on refresh.

---

## Monitoring

### Celery Flower

Task queue monitor at `http://localhost:5555`

```
Username: admin
Password: <FLOWER_BASIC_AUTH password from .env>
```

Shows active/queued/failed tasks, per-worker throughput, and task history.

### Prometheus Metrics

```bash
curl http://localhost:8000/metrics
```

Exposes `http_requests_total` (counter by method/endpoint/status) and `http_request_duration_seconds` (histogram). Scrape from Grafana or any Prometheus-compatible stack.

### Health Check

```bash
curl http://localhost:8000/health
```

- `status: healthy` → HTTP 200  
- `status: degraded` → HTTP 503 (TLE data stale, DB unavailable, or Redis down)

The stale TLE threshold is controlled by `TLE_STALE_ALERT_HOURS` (default 24).

### Queue Depth

The `check_queue_depth` task runs every 5 minutes via Beat and logs per-queue pending counts. A warning is logged when total pending exceeds 1,000 tasks.

---

## Pipeline Walkthrough

The full data pipeline runs automatically after startup:

```
1. Celery Beat fires ingest_tles every 6 h
       ↓
2. TLEFetcher downloads from Celestrak (with fallback URLs)
   TLEValidator checks format + checksum
   Bulk insert to tles table (ON CONFLICT DO NOTHING — idempotent)
       ↓
3. propagate_batch dispatches one detect_passes([norad_id]) per satellite
   (one satellite per task = true parallelism, bounded memory per worker)
       ↓
4. detect_passes (per satellite):
   - SGP4Engine propagates GCRS positions for 7 days at 10 s steps
   - PassDetector scans all 50 stations in one vectorised pass
   - Binary search refines rise/set times to 1 s precision
   - Bulk upsert to satellite_passes (ON CONFLICT DO NOTHING)
   → automatically chains to run_greedy
       ↓
5. run_greedy (Stage 1 scheduler):
   - Weighted-interval DP across all active stations
   - Cross-station uniqueness: each satellite scheduled at most once globally
   - Marks passes is_scheduled=True, scheduled_by='greedy'
   → passes free_slots and scheduled_sats to run_ortools
       ↓
6. run_ortools (Stage 2 scheduler):
   - Fetches passes for satellites greedy missed
   - Filters to passes fitting inside greedy's free time slots
   - CP-SAT solver maximises unique satellite count (30 s budget)
   - Marks passes is_scheduled=True, scheduled_by='ortools'
```

Results from both stages are immediately visible through the API.

---

## Scheduling Algorithms

### Stage 1 — Greedy Weighted Interval Scheduling

The greedy scheduler runs independently per ground station using weighted-interval DP (O(n log n)):

1. Sort all unscheduled passes at the station by `set_time`
2. DP recurrence: `dp[i] = max(benefit[i] + dp[prev[i]], dp[i-1])`  
   where `prev[i]` = last pass that ends at or before `rise_time[i]` (bisect, O(log n))
3. Backtrack through DP to extract the selected passes
4. A satellite scheduled at any station is excluded from all subsequent stations (cross-station uniqueness)

Benefit function: `max_elevation × duration_seconds` — rewards high-quality, long passes.

Three variants are implemented (`schedule_edf`, `schedule_weighted`, `schedule_heap`). The DB-backed wrapper uses `schedule_weighted`.

### Stage 2 — OR-Tools CP-SAT Optimizer

After greedy, unscheduled satellites may still have passes that fit in the remaining free time windows. OR-Tools fills these optimally:

**Decision variables:**
- `selected[i]` ∈ {0, 1} — pass `i` is scheduled
- `sat_var[k]` ∈ {0, 1} — satellite `k` appears at least once

**Constraints:**
- `AddNoOverlap([intervals at station s])` — no two passes at the same station overlap
- `AddAtMostOne([selected[i] for i in satellite_k])` — at most one pass per satellite
- `AddMaxEquality(sat_var[k], [selected[i] for i in satellite_k])` — links satellite var to pass vars

**Objective (lexicographic via weighted sum):**
```
Maximise: W × Σ(sat_var[k]) + Σ(benefit[i] × selected[i])
```
Where `W > max(benefit)` guarantees satellite count always dominates, and benefit breaks ties.

**Why two stages instead of pure OR-Tools?**  
Greedy runs in < 1 second and schedules ~96% of satellites on the first pass. Running CP-SAT over all 750,000 passes would exceed the time budget. Greedy prunes the problem to < 1% of its original size before OR-Tools sees it.

**Observed results (real data):**

| Stage | Passes | Unique Satellites | Avg Elevation |
|---|---|---|---|
| Greedy | 2,088 | 30 | 38.2° |
| OR-Tools | 13 | 10 | 89.1° |
| **Total** | **2,101** | **31** | — |

OR-Tools specifically picked up high-inclination satellites (Etalon, LAGEOS, HST, AO-10) that greedy missed because their passes clustered at very high elevation (near-zenith) in free time slots greedy left open.

---

## Trade-offs and Design Decisions

| Decision | Choice | Trade-off |
|---|---|---|
| Propagation step | 10 s | Accuracy vs speed: a fast LEO satellite moves ~70 km in 10 s; binary search recovers 1 s precision at boundaries |
| Per-satellite Celery tasks | 1 satellite per `detect_passes` task | True parallelism across workers vs coarser batching; prevents 16+ hour tasks and bounds per-worker memory |
| Worker concurrency | 1 per container | Prevents OOM when two SGP4 + OR-Tools jobs run simultaneously inside a 2 GB container |
| Two-stage scheduler | Greedy + OR-Tools | Greedy is O(n log n) and fast; OR-Tools fills the remaining free slots for near-optimal global coverage |
| OR-Tools time limit | 30 s | Prevents runaway solver; CP-SAT finds near-optimal solutions well within this budget for typical problem sizes |
| Cross-station uniqueness | Satellite scheduled at most once globally | Maximises unique object count (SDA objective) rather than total observation minutes |
| API key hashing | SHA-256, raw key shown once | Raw keys never stored; DB compromise reveals nothing usable |
| Admin key comparison | `secrets.compare_digest` | Prevents timing oracle attacks on the admin key |
| Redis cache TTL | 5 min (passes), 60 s (invalid keys) | Short TTL keeps data fresh after scheduling; invalid keys cached briefly to reduce DB spam |
| Free-slot filtering | Python bisect, not SQL `OR` clauses | SQL `OR(*conditions)` with 50 stations × many slots produces hundreds of clauses and degrades the Postgres query planner |
| Health endpoint | HTTP 503 when degraded | Makes `/health` usable as a Kubernetes readiness probe without body parsing |
| Soft key revocation | `is_active=False` vs DELETE | Retains audit trail; keys can be reactivated if revoked in error |
| nginx admin restriction | RFC-1918 only | Admin endpoints are accessible from the network only via VPN/bastion; X-Admin-Key still required as a second factor |

---

## Performance Characteristics

| Operation | Observed / Expected |
|---|---|
| TLE ingestion (~15,000 satellites) | 10–30 s |
| SGP4 propagation (1 satellite, 7 days, 10 s step) | ~8–12 s per satellite |
| Pass detection (1 satellite × 50 stations) | included above |
| Full catalogue propagation (15,352 sats, 2 workers) | ~21 hours |
| Greedy scheduling (all stations) | < 5 s |
| OR-Tools CP-SAT stage | ≤ 30 s (time-limited) |
| API query — single station, single day | 10–50 ms (indexed) |
| API query — coverage aggregation | < 500 ms |
| Redis cache hit | < 5 ms |
| Worker memory (per container, 1 concurrent task) | 1.2–1.5 GB of 2 GB limit |

---

## Production Deployment

### Option A — Railway (single-service, no workers)

Railway runs the API process only. No Celery workers or Beat scheduler. The pipeline is driven manually via the admin endpoints.

**Deploy steps:**
1. Connect the GitHub repo to a Railway project
2. Add a PostgreSQL plugin and a Redis plugin
3. Set environment variables (see [Configuration](#configuration)) — Railway auto-injects `DATABASE_URL` and `REDIS_URL` from the plugins
4. Set `ENVIRONMENT=production` and a strong `ADMIN_API_KEY`
5. Railway auto-deploys on every push to `main`

**Required env overrides for Railway:**
```dotenv
ENVIRONMENT=production
ADMIN_API_KEY=<strong-random-string>
PROPAGATION_DAYS=7
PROPAGATION_STEP_SECONDS=30
DATABASE_URL=${{Postgres.DATABASE_URL}}
REDIS_URL=${{Redis.REDIS_URL}}
```

**Trigger the pipeline after deploy:**
```bash
# 1. Ingest TLEs
curl -X POST https://<your-app>.up.railway.app/admin/tasks/ingest-tles \
  -H "X-Admin-Key: <ADMIN_API_KEY>"

# 2. Run propagation + pass detection (adjust limit as needed)
curl -X POST "https://<your-app>.up.railway.app/admin/tasks/run-pipeline?limit=5000" \
  -H "X-Admin-Key: <ADMIN_API_KEY>"

# 3. Schedule detected passes
curl -X POST https://<your-app>.up.railway.app/admin/tasks/schedule \
  -H "X-Admin-Key: <ADMIN_API_KEY>"
```

Or use the **Admin** tab in the dashboard UI.

### Option B — Docker Compose (full stack with workers)

### Secrets — never commit to git

```dotenv
POSTGRES_PASSWORD=use-a-password-manager
ADMIN_API_KEY=use-a-64-char-random-string
FLOWER_BASIC_AUTH=admin:use-a-strong-password
API_KEY_SALT=use-a-64-char-random-string
```

Generate safe values:
```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

### TLS certificates

Replace self-signed certs with Let's Encrypt or your CA:
```bash
# Option A — Certbot (requires public DNS)
certbot certonly --standalone -d your-domain.com
cp /etc/letsencrypt/live/your-domain.com/fullchain.pem nginx/ssl/cert.pem
cp /etc/letsencrypt/live/your-domain.com/privkey.pem   nginx/ssl/key.pem

# Option B — re-run generate_certs.sh for internal CA or staging
bash nginx/generate_certs.sh
```

### CORS

For browser-based clients set `CORS_ORIGINS` to your frontend origin:
```dotenv
CORS_ORIGINS=["https://your-domain.com"]
```

### Scaling workers

Add more replicas in `docker-compose.yml`:
```yaml
worker:
  deploy:
    replicas: 4   # 4 concurrent satellites propagating = 4× throughput
```

Each worker uses up to 2 GB. Ensure the host has `(replicas + 1) × 2 GB` free RAM.

### Database backups

```bash
# Manual snapshot
docker exec sda_system-postgres-1 \
  pg_dump -U sda_user sda_db | gzip > backup-$(date +%Y%m%d).sql.gz
```

For production, configure WAL archiving or a managed backup service.

### Environment

```dotenv
ENVIRONMENT=production
LOG_LEVEL=INFO
```

Setting `ENVIRONMENT=production` enables CORS enforcement, blocks the `["*"]` wildcard, and activates the production safety validator that refuses to start if `ADMIN_API_KEY` is set to the dev default.

---

## Project Structure

```
sda_system/
├── api/
│   ├── main.py                 FastAPI app, lifespan, metrics, health, CORS
│   ├── middleware.py           Request logging, API-key auth, sliding-window rate limiter
│   └── routes/
│       ├── passes.py           GET /api/passes, /schedule, /coverage
│       └── admin.py            GET/POST/PATCH/DELETE /admin/api-keys
├── cache/
│   └── redis_client.py         Async Redis wrapper, circuit-breaker, hit-ratio stats
├── db/
│   ├── models.py               SQLAlchemy ORM: TLE, GroundStation, SatellitePass, APIKey
│   ├── session.py              SessionLocal factory + get_db dependency
│   └── init_db.py              Schema creation + ground station seeding
├── frontend/
│   └── index.html              Dashboard (vanilla JS, no build step)
├── ingestion/
│   ├── fetcher.py              Async Celestrak downloader + fallback URL support
│   └── validator.py            TLE line format + Mod-10 checksum validation
├── propagation/
│   ├── sgp4_engine.py          SGP4 via Skyfield, 10 s steps, 7-day window
│   ├── coordinates.py          GCRS → ECEF → ENU vectorised transforms
│   └── pass_detector.py        Coarse scan + binary-search rise/set + elevation mask
├── scheduling/
│   ├── greedy.py               Weighted-interval DP + EDF + heap variants
│   ├── interval_tree.py        Sweep-line conflict detector
│   └── ortools_optimizer.py    CP-SAT: NoOverlap + AtMostOne + MaxEquality + dual objective
├── workers/
│   └── celery_app.py           Task chain, Beat schedule, dead-letter queue, queue-depth monitor
├── nginx/
│   ├── nginx.conf              TLS 1.2/1.3, HSTS, gzip, RFC-1918 admin restriction
│   └── generate_certs.sh       Self-signed cert generator (via Docker alpine + openssl)
├── docs/
│   ├── architecture.md         Full design rationale, algorithm derivation, scalability analysis
│   └── sample_outputs/         Example JSON responses from each endpoint
├── stations.json               Ground station definitions with per-station elevation masks
├── config.py                   Pydantic BaseSettings, SecretStr, env validators (loads stations.json)
├── requirements.txt            Python dependencies
├── docker-compose.yml          All 8 services: postgres, redis, api, worker×2, beat, flower, nginx
├── Dockerfile.api              Multi-stage FastAPI image
├── Dockerfile.worker           Multi-stage Celery worker/beat/flower image
├── entrypoint.sh               Auto-init DB on worker startup (SKIP_DB_INIT bypasses for beat/flower)
├── init.sql                    PostgreSQL init (extensions, roles)
└── .env.example                All variables with safe development defaults
```

---

## Known Limitations and Future Work

| Limitation | Impact | Suggested Fix |
|---|---|---|
| Single antenna per station | Cannot model multi-beam ground stations | Add `antenna_count` to `GroundStation`; OR-Tools `AddNoOverlap` supports multiple resources |
| Fixed elevation mask | ~~Different missions need different minimum elevations~~ | **Resolved** — `elevation_mask_deg` per station in `stations.json`; `PassDetector` uses per-station value |
| Velocity not persisted | Cannot compute range-rate or Doppler shift | Store velocity columns in `satellite_passes`; compute on the fly from SGP4 |
| No manoeuvre handling | TLEs invalid after orbit changes | Integrate space-track.org manoeuvre alerts to trigger re-propagation |
| No link budget model | Passes selected by elevation only, not SNR | Add antenna gain + `EIRP` as a secondary objective term in CP-SAT |
| Sequential station seeding | ~~Fixed 50 stations coded in `config.py`~~ | **Resolved** — stations defined in `stations.json`; add/edit/remove without touching Python code |
| No real-time updates | ~~Schedule is static until next propagation run~~ | **Resolved** — `/ws/events` WebSocket pushes live snapshots every 3 s; Live tab in dashboard |
| Full-catalogue propagation time | 15,352 satellites takes ~21 hours on 2 workers | Scale workers horizontally; coarsen time step for deep-space objects |
