# Architecture & Design Document — SDA Pass Scheduling System

## 1. Problem Framing

The assignment asks for a prototype of a scalable system that:
1. Fetches live TLE data from Celestrak
2. Propagates satellite positions using SGP4 for 7 days
3. Predicts visibility passes over 50 ground stations
4. Schedules passes to maximise the number of unique satellites tracked across the network
5. Stores and serves results with sub-second query latency at millions-of-combinations scale

The key insight is that this is a **weighted interval scheduling problem**: given a set of time intervals (passes) across 50 stations, assign at most one pass per time slot per station while maximising the count of distinct satellites scheduled at least once anywhere in the network.

---

## 2. Assumptions and Scope Decisions

| Parameter | Assumed Value | Rationale |
|---|---|---|
| Propagation window | 7 days | As specified |
| Propagation time step | 10 seconds | Balances accuracy vs. computation. A satellite in LEO moves ~70 km in 10 s — acceptable for coarse pass detection; binary search recovers 1 s precision at boundaries |
| Elevation mask | 10 degrees | Industry standard minimum for useful signal quality above the horizon |
| Minimum pass duration | 5 seconds | As specified; shorter passes are geometrically marginal and operationally unusable |
| Ground station altitude | 0 m (sea level) | Not specified; assumed flat-earth baseline. Easily overridden via DB seeding |
| Ground station placement | Uniform random, seed=42 | Not specified; seed ensures reproducibility across environments |
| TLE source | Celestrak GP active group (~6,000 objects) | Largest publicly available active satellite catalogue |
| TLE refresh cadence | Every 6 hours | TLEs degrade in accuracy over time; 6 h is a standard operational refresh rate |
| Scheduling objective | Maximise unique satellites with at least one scheduled pass | Maximises SDA network coverage breadth rather than total observation time |
| Concurrent passes per station | 1 (no multi-beam assumption) | Most ground stations operate a single antenna; multi-beam would require hardware parameterisation |

---

## 3. System Architecture

### 3.1 Five-Layer Pipeline

```
[Celestrak] → Ingestion → Propagation → Scheduling → API
                  |             |             |         |
               PostgreSQL   PostgreSQL   PostgreSQL   Redis
               (tles)     (sat_passes) (sat_passes) (cache)
```

Each layer is decoupled via Celery task chains. The API layer is read-only against the database; all writes happen in Celery workers.

### 3.2 Component Responsibilities

**Ingestion** (`ingestion/`)
- `TLERetriever`: async HTTP download from Celestrak, parse 3-line TLE format
- `TLEValidator`: regex + checksum validation (Mod-10 checksum per TLE standard)
- Idempotent insert: `ON CONFLICT DO NOTHING` on `(norad_id, epoch)`
- Celery Beat triggers ingestion every 6 hours

**Propagation** (`propagation/`)
- `SGP4Engine`: wraps the `sgp4` library (python-sgp4 v2.22, Vallado SGP4 implementation)
  - Produces shape `(N, 3)` numpy array of TEME-frame positions per satellite
  - N = 7 × 86400 / 10 = 60,480 timesteps
- `CoordinateTransformer`: vectorised TEME → ECEF → ENU chain
  - TEME→ECEF: rotate by Greenwich Mean Sidereal Time (GMST)
  - ECEF→ENU: station-relative frame using geodetic latitude/longitude
- `PassDetector`: finds visibility windows
  - Coarse scan: O(N) mask `elevation >= 10°`
  - Binary search: refine rise and set boundaries to 1 s precision (O(log N))
  - Golden-section search: find exact max-elevation time

**Scheduling** (`scheduling/`)
- Stage 1 — `GreedyScheduler`:
  - Classic earliest-deadline-first interval scheduling per station
  - Global satellite set prevents the same satellite from being scheduled at multiple stations
  - O(n log n) per station; runs across all 50 stations sequentially
- Stage 2 — `ORToolsOptimizer`:
  - Input: only unscheduled satellites + free time slots left by greedy
  - CP-SAT decision variables: `selected[i]` ∈ {0, 1} per candidate pass
  - Constraint: `NoOverlap` intervals per station (native CP-SAT interval constraint)
  - Objective: maximise `sum(sat_selected)` where `sat_selected[s] = OR(selected[i] for i in sat_passes[s])`
  - Time limit: 30 s; typically finds optimal or near-optimal solution within 10 s

**API** (`api/`)
- FastAPI with async handlers and SQLAlchemy synchronous sessions (thread-pool pattern)
- Three query endpoints: `/passes`, `/schedule`, `/coverage`
- Redis cache: station-level pass queries cached for 5 minutes
- Middleware: request logging with trace IDs, API key authentication, rate limiting

**Workers** (`workers/`)
- Celery task chain: `ingest_tles` → `propagate_batch` → `detect_passes` → `run_greedy` → `run_ortools`
- Three queues: `ingestion`, `propagation`, `scheduling` (separate workers can consume each)
- `task_acks_late=True` + `task_reject_on_worker_lost=True` for at-least-once delivery

---

## 4. Database Design

### 4.1 Schema

```sql
tles (
    id           SERIAL PRIMARY KEY,
    norad_id     INTEGER NOT NULL,
    name         VARCHAR(255) NOT NULL,
    line1        VARCHAR(69) NOT NULL,
    line2        VARCHAR(69) NOT NULL,
    epoch        TIMESTAMP NOT NULL,
    ingested_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (norad_id, epoch)
)

ground_stations (
    id           SERIAL PRIMARY KEY,
    name         VARCHAR(50) NOT NULL UNIQUE,
    latitude     FLOAT NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude    FLOAT NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    altitude_m   FLOAT NOT NULL DEFAULT 0.0
)

satellite_passes (
    id                 SERIAL PRIMARY KEY,
    satellite_id       INTEGER NOT NULL,
    station_id         INTEGER NOT NULL REFERENCES ground_stations(id),
    rise_time          TIMESTAMP NOT NULL,
    set_time           TIMESTAMP NOT NULL CHECK (set_time > rise_time),
    max_elevation      FLOAT NOT NULL,
    max_elevation_time TIMESTAMP NOT NULL,
    duration_seconds   FLOAT NOT NULL,
    is_scheduled       INTEGER NOT NULL DEFAULT 0,
    scheduled_by       VARCHAR(16),           -- 'greedy' or 'ortools'
    computed_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (satellite_id, station_id, rise_time)
)
```

### 4.2 Index Strategy

| Index | Columns | Purpose |
|---|---|---|
| `idx_tle_norad_epoch` | `(norad_id, epoch DESC)` | Fetch latest TLE per satellite in O(log N) |
| `idx_pass_station_rise` | `(station_id, rise_time)` | Primary query path for `/api/passes?station_id=X` |
| `idx_pass_satellite_rise` | `(satellite_id, rise_time)` | Satellite-centric queries |
| `idx_pass_time_range` | `(rise_time, set_time)` | Time-range scans |
| `idx_pass_scheduled` | `(is_scheduled, station_id)` | Schedule queries |

All hot read paths hit an index. The `satellite_passes` table is expected to hold ~750,000 rows (6,000 satellites × 50 stations × ~2.5 avg passes per week) — well within PostgreSQL's comfortable range for indexed queries at < 50 ms.

---

## 5. Scalability Strategy

### 5.1 Data Volume Estimation

- 6,000 active satellites × 50 stations × ~2.5 passes/week = **750,000 pass records**
- TLE table: 6,000 rows (latest only effectively queried)
- At full scale (millions of combinations): partition `satellite_passes` by `rise_time` week. PostgreSQL native range partitioning keeps each partition to ~750,000 rows.

### 5.2 Query Performance

- Single-station, single-day query: index hit on `(station_id, rise_time)` → **10–50 ms**
- Coverage aggregation with 50 stations × 6,000 satellites: multiple `COUNT(DISTINCT)` queries → **< 500 ms** (cacheable)
- Redis caches station-level pass lists for 5 minutes, absorbing repeated API traffic

### 5.3 Propagation Scaling

- Each satellite propagation is independent — embarrassingly parallelisable
- Celery workers scale horizontally: add more `worker` replicas in `docker-compose.yml`
- Numpy vectorisation gives 10–100× speedup over Python loops for coordinate transforms
- Memory: ~4.4 MB per satellite for 60,480 × 3 float64 positions. Process in batches of 100 to cap per-worker memory at ~440 MB

### 5.4 Scheduling Scaling

- Greedy runs in O(n log n) per station — fast enough for millions of passes
- OR-Tools CP-SAT handles ~50,000 reduced variables (after greedy prunes) in < 30 s
- For truly massive scale (millions of passes per run): partition the OR-Tools problem by geographic region or time window and run in parallel

---

## 6. Algorithm Details

### 6.1 SGP4 Propagation

SGP4 (Simplified General Perturbations 4) is the standard algorithm for propagating TLE-based orbits. It accounts for:
- Earth's oblateness (J2–J6 zonal harmonics)
- Atmospheric drag
- Solar and lunar gravitational perturbations (for deep-space objects)

The python-sgp4 library (Brandon Rhodes, Vallado implementation) is used because it matches the reference AFSPC SGP4 implementation used operationally.

### 6.2 Coordinate Transforms

```
SGP4 output → TEME frame (True Equator, Mean Equinox)
    ↓ rotate by GMST
ECEF frame (Earth-Centred, Earth-Fixed)
    ↓ translate + rotate by station geodetic lat/lon
ENU frame (East-North-Up, station-relative)
    ↓ atan2
Elevation angle (degrees above horizon)
```

All transforms are vectorised across the full 60,480-step array using numpy, avoiding Python-level loops.

### 6.3 Pass Detection

1. **Coarse scan**: boolean mask `elevation >= 10°` across all N timesteps → O(N)
2. **Window extraction**: group consecutive True indices into pass windows
3. **Binary search rise boundary**: find exact index where elevation crosses 10° on the rising side → O(log N)
4. **Binary search set boundary**: same for the descending side → O(log N)
5. **Golden-section max elevation**: find peak elevation time within window → O(log N)
6. **Duration filter**: discard passes < 5 seconds

Binary search gives 1-second precision using the 10-second step array as the search space, then interpolating sub-step timestamps.

### 6.4 Two-Stage Scheduler

**Stage 1 — Greedy (Earliest Deadline First)**

For each station independently:
- Sort unscheduled passes by `set_time` (earliest deadline first)
- Use a min-heap of `(set_time, pass)` for O(log n) extraction
- Select a pass if: (a) no overlap with the last scheduled pass, and (b) this satellite has not been scheduled at any station yet
- Mark selected passes `is_scheduled=1, scheduled_by='greedy'`

The cross-station global satellite set is the key to maximising unique coverage rather than pass count.

**Stage 2 — OR-Tools CP-SAT**

After greedy, free time slots remain. Stage 2 fills them:
- Candidate passes: only those for satellites not yet scheduled + only in free slots
- This reduces the problem size by ~99% (from ~750k to ~50k variables)
- CP-SAT `NoOverlap` constraints enforce non-overlapping intervals per station
- Objective: maximise `sum(sat_selected)` (each satellite counts once regardless of how many passes are selected for it)
- 30-second wall-clock limit; in practice near-optimal solutions are found in < 10 seconds

---

## 7. Known Limitations and Future Work

| Limitation | Impact | Mitigation / Future Work |
|---|---|---|
| Velocity not stored | Cannot compute access rate or manoeuvre windows | Add velocity columns to `satellite_passes` or store in a separate trajectory cache |
| Single-antenna assumption | Multi-beam stations would allow overlapping passes | Add `antenna_count` to `GroundStation` model |
| Fixed elevation mask | Different missions have different minimum elevation requirements | Add `min_elevation_deg` column to `GroundStation` |
| Sequential propagation | Full 6,000-satellite run takes 30-60 min | Distribute across more Celery workers or use a dedicated propagation cluster |
| No real-time updates | TLEs are refreshed every 6 h | Add a webhook or SSE endpoint to push new passes to subscribers |
| No manoeuvre handling | TLEs become invalid after a manoeuvre | Integrate space-track.org manoeuvre alerts to trigger re-propagation |
| No link budget | Passes are selected purely on elevation; no SNR or data-rate model | Add antenna gain + link budget as a secondary optimisation objective |
