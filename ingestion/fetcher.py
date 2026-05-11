"""
ingestion/fetcher.py — TLE fetcher and ground-station seeder.

Fixes vs. original
------------------
  - N+1 query eliminated — original fired one SELECT per satellite (up to
    9 000+) inside _parse_and_store.  Now pre-fetches all current TLEs in
    one query and all (norad_id, epoch) pairs in a second query; per-satellite
    existence checks become O(1) in-memory dict/set lookups.
  - Bulk retirement UPDATE — the original fired one UPDATE per new-epoch
    satellite to flip is_current=False.  Now batched into a single
    UPDATE ... WHERE norad_id IN (...) per flush cycle.
  - UNIQUE constraint violation prevented — pre-fetching all (norad_id,
    epoch) pairs guards against re-inserting a previously retired TLE epoch
    that reappears in the Celestrak feed (epoch regression), which would
    raise IntegrityError on the uix_tles_norad_epoch constraint.
  - Duplicate norad_id in feed guarded — if Celestrak sends two blocks for
    the same satellite in one response, the second block is skipped rather
    than inserting a second is_current=True row for the same norad_id.
  - Batch commit every _BATCH_SIZE rows — a single end-of-loop commit for
    9 000 rows loses all progress if the DB connection drops; periodic
    commits bound re-work to one batch on retry.
  - Rollback on exception in _parse_and_store — without it a mid-loop DB
    failure left the session dirty; the next operation raised
    InFailedSqlTransaction instead of the real error.
  - Rollback on exception in init_ground_stations — same dirty-session risk.
  - Empty response guard — Celestrak occasionally returns HTTP 200 with an
    empty body; without a guard the ingestion silently committed nothing and
    reported success.
  - Partial-triplet warning — if the payload line count is not divisible by
    3, the trailing lines are silently discarded.  Now logged explicitly so
    operators know data was lost.
  - HTTP retry for transient errors (429 / 5xx) — a single failed attempt
    re-queues the entire Celery task; lightweight in-process retries with
    exponential back-off absorb transient Celestrak hiccups without burning
    task-queue retries.
  - Separate connect vs. read timeouts — original total=30 s could be
    exhausted entirely by a slow DNS/TCP handshake with zero bytes received;
    connect=10 sock_read=60 gives each phase its own budget.
  - Explicit UTF-8 encoding on response.text() — aiohttp auto-detects from
    Content-Type, but Celestrak occasionally omits the charset field causing
    garbled satellite names from non-ASCII characters.
  - TCPConnector with connection limit — no connector cap lets aiohttp open
    unbounded connections to Celestrak; limit=4 is generous for a single URL.
  - Optional[TLE] replaces TLE | None — union-type syntax requires Python
    3.10+; Optional is compatible with Python 3.8+.
  - unused_count added to result — callers can now distinguish "nothing new"
    (all unchanged) from actual new/updated ingestion without a second query.
  - Unused `and_` import removed.
  - name field updated in-place on checksum change — if Celestrak renames a
    satellite (common for newly-identified debris), the previous code kept the
    stale name forever because it only updated line1/line2/checksum/fetched_at.
  - User-Agent header added — Celestrak rate-limits anonymous requests;
    identifying the client is required for reliable production access.
  - Content-Type guard on 200 responses — Celestrak occasionally returns an
    HTML maintenance page with HTTP 200.  Without the guard, the parser treats
    every HTML tag as an invalid TLE triplet, silently inflating invalid_count
    with no actionable error message.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
from sqlalchemy.orm import Session
from structlog import get_logger

from ..config import config
from ..db.models import GroundStation, TLE
from .validator import TLEValidator

logger = get_logger()

_BATCH_SIZE         = 500   # commit to DB every N satellites; bounds re-work on retry
_HTTP_MAX_RETRIES   = 3     # in-process retries before propagating to the Celery task
_TRANSIENT_STATUSES = {429, 500, 502, 503, 504}


class TLEFetcher:
    """Downloads TLEs from Celestrak and persists them to the database."""

    def __init__(self, db_session: Session) -> None:
        self.session   = db_session
        self.validator = TLEValidator()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_all(self) -> Dict[str, int]:
        """
        Fetch the active-satellite TLE catalog and ingest it.

        Try the primary CELESTRAK_URL first (with per-attempt exponential
        back-off).  If all attempts fail, iterate through TLE_FALLBACK_URLS
        in config order.  Raises only if every source has been exhausted.
        """
        urls = [config.CELESTRAK_URL] + list(config.TLE_FALLBACK_URLS)
        last_exc: Optional[Exception] = None

        for url_index, url in enumerate(urls):
            is_fallback = url_index > 0
            if is_fallback:
                logger.warning("tle_primary_failed_trying_fallback", url=url, index=url_index)
            else:
                logger.info("fetch_all_started", url=url)

            text = await self._fetch_url(url)
            if text is not None:
                if is_fallback:
                    logger.info("tle_fallback_succeeded", url=url)
                return self._parse_and_store(text)

        raise RuntimeError(
            f"All TLE sources failed ({len(urls)} tried). "
            "Check CELESTRAK_URL and TLE_FALLBACK_URLS."
        )

    async def _fetch_url(self, url: str) -> Optional[str]:
        """
        Attempt to download a TLE catalog from url.

        Returns the text content on success, or None if all retries failed.
        Logs warnings for transient failures; never raises.
        """
        connector = aiohttp.TCPConnector(limit=4, enable_cleanup_closed=True)
        timeout   = aiohttp.ClientTimeout(connect=10, sock_read=60)
        headers   = {"User-Agent": "SDA-System/1.0 (satellite-pass-scheduler)"}

        async with aiohttp.ClientSession(connector=connector, headers=headers) as http:
            for attempt in range(1, _HTTP_MAX_RETRIES + 1):
                try:
                    async with http.get(url, timeout=timeout) as resp:
                        if resp.status == 200:
                            ct = resp.content_type or ""
                            if "html" in ct:
                                body = await resp.text()
                                logger.warning(
                                    "tle_source_returned_html",
                                    url=url,
                                    content_type=ct,
                                    preview=body[:200],
                                )
                                return None

                            text = await resp.text(encoding="utf-8")
                            if not text or not text.strip():
                                logger.warning("tle_source_empty_response", url=url)
                                return None
                            return text

                        body = await resp.text()
                        if resp.status in _TRANSIENT_STATUSES and attempt < _HTTP_MAX_RETRIES:
                            wait = 2 ** attempt
                            logger.warning(
                                "tle_source_transient_error",
                                url=url,
                                status=resp.status,
                                attempt=attempt,
                                retry_in_sec=wait,
                            )
                            await asyncio.sleep(wait)
                            continue

                        logger.warning(
                            "tle_source_http_error",
                            url=url,
                            status=resp.status,
                            body_preview=body[:200],
                        )
                        return None

                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    if attempt < _HTTP_MAX_RETRIES:
                        wait = 2 ** attempt
                        logger.warning(
                            "tle_source_network_error",
                            url=url,
                            error=str(exc),
                            attempt=attempt,
                            retry_in_sec=wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.warning(
                            "tle_source_all_attempts_failed",
                            url=url,
                            error=str(exc),
                            attempts=attempt,
                        )
                        return None

        return None

    def init_ground_stations(self) -> None:
        """
        Seed ground stations into the database (idempotent).

        Single bulk SELECT to find missing stations; inserts only the absent
        ones.  Rolls back and re-raises on commit failure so the caller's
        session is never left in a dirty state.
        """
        configured = {s["id"]: s for s in config.GROUND_STATIONS}

        existing_ids: Set[str] = {
            row[0]
            for row in self.session.query(GroundStation.station_id).all()
        }

        missing = [s for sid, s in configured.items() if sid not in existing_ids]
        if not missing:
            logger.info("ground_stations_already_seeded")
            return

        for station in missing:
            self.session.add(GroundStation(
                station_id = station["id"],
                name       = station["name"],
                latitude   = station["latitude"],
                longitude  = station["longitude"],
                altitude_m = station["altitude_m"],
                is_active  = True,
            ))

        try:
            self.session.commit()
        except Exception as exc:
            self.session.rollback()
            logger.error("ground_station_seed_failed", error=str(exc))
            raise

        logger.info(
            "ground_stations_seeded",
            inserted=len(missing),
            total=len(configured),
        )

    # ------------------------------------------------------------------
    # Private — parse + DB persistence
    # ------------------------------------------------------------------

    def _parse_and_store(self, text: str) -> Dict[str, int]:
        """
        Parse a 3LE text block and upsert into the database.

        Algorithm
        ---------
        1. Pre-fetch current TLEs → dict keyed by (norad_id, epoch).
        2. Pre-fetch ALL (norad_id, epoch) pairs → set for unique-constraint guard.
        3. For each parsed TLE triplet (O(1) lookups — no per-row SELECT):
             a. Already current + same checksum → skip (unchanged).
             b. Already current + different checksum → update in place.
             c. (norad_id, epoch) exists but not current → skip (retired epoch
                reappeared; re-inserting would violate the unique constraint).
             d. New epoch, norad_id seen in this run → skip (duplicate in feed).
             e. New epoch, norad_id not yet seen → retire old current + insert.
        4. Commit + bulk-retire every _BATCH_SIZE rows.
        """
        lines       = text.splitlines()
        total_lines = len(lines)

        # Trailing lines not divisible by 3 are silently lost — warn operators
        if total_lines % 3 != 0:
            logger.warning(
                "tle_payload_not_multiple_of_3",
                total_lines=total_lines,
                trailing_lines_dropped=total_lines % 3,
            )

        # ------------------------------------------------------------------
        # Two bulk pre-fetches replace up to 9 000 per-satellite SELECTs
        # ------------------------------------------------------------------

        # (norad_id, epoch) → TLE object for every is_current=True row
        current_tles: Dict[Tuple[int, datetime], TLE] = {
            (t.norad_id, t.epoch): t
            for t in self.session.query(TLE).filter(TLE.is_current == True).all()  # noqa: E712
        }
        # Which norad_ids have an active current TLE (used for retirement)
        current_norad_ids: Set[int] = {norad_id for norad_id, _ in current_tles}

        # ALL (norad_id, epoch) pairs — lightweight, guards UNIQUE constraint
        all_epochs: Set[Tuple[int, datetime]] = {
            (norad_id, epoch)
            for norad_id, epoch in self.session.query(TLE.norad_id, TLE.epoch).all()
        }

        new_count     = 0
        updated_count = 0
        invalid_count = 0
        skipped_count = 0
        unused_count  = 0

        # norad_ids added in this run — prevents double-insertion from duplicate feed entries
        added_this_run: Set[int]   = set()
        retire_this_batch: List[int] = []
        batch_pending = 0

        try:
            i = 0
            while i + 2 < total_lines:
                name  = lines[i].strip()
                line1 = lines[i + 1].strip()
                line2 = lines[i + 2].strip()
                i += 3

                # Skip fully-blank triplets (Celestrak pads EOF with blank lines)
                if not name and not line1 and not line2:
                    skipped_count += 1
                    continue

                ok, err = self.validator.validate_pair(name, line1, line2)
                if not ok:
                    logger.debug("tle_rejected", name=name, reason=err)
                    invalid_count += 1
                    continue

                norad_id = self.validator.extract_norad_id(line1)
                epoch    = self.validator.extract_epoch(line1)

                if norad_id is None or epoch is None:
                    logger.warning(
                        "tle_extraction_failed_post_validation",
                        name=name,
                        line1_excerpt=line1[:32],
                    )
                    invalid_count += 1
                    continue

                pair_hash = self.validator.compute_pair_hash(line1, line2)
                key       = (norad_id, epoch)

                if key in current_tles:
                    # Case b/unchanged: current TLE for this exact epoch
                    tle_obj    = current_tles[key]
                    name_changed     = tle_obj.name != name
                    checksum_changed = tle_obj.checksum != pair_hash

                    if checksum_changed or name_changed:
                        # Always update name — Celestrak renames debris / newly
                        # identified objects; stale names cause operator confusion.
                        tle_obj.name       = name
                        tle_obj.line1      = line1
                        tle_obj.line2      = line2
                        tle_obj.checksum   = pair_hash
                        tle_obj.fetched_at = datetime.now(timezone.utc)
                        self.session.add(tle_obj)
                        updated_count += 1
                        batch_pending += 1
                    else:
                        unused_count += 1   # identical — nothing to do

                elif key in all_epochs:
                    # Case c: this (norad_id, epoch) was previously retired.
                    # Re-inserting it would violate uix_tles_norad_epoch.
                    logger.debug(
                        "tle_retired_epoch_reappeared",
                        norad_id=norad_id,
                        epoch=epoch.isoformat(),
                    )
                    skipped_count += 1

                elif norad_id in added_this_run:
                    # Case d: duplicate norad_id in the same feed response.
                    # Inserting again would leave two is_current=True rows.
                    logger.warning(
                        "tle_duplicate_norad_in_feed",
                        norad_id=norad_id,
                        name=name,
                    )
                    skipped_count += 1

                else:
                    # Case e: genuinely new epoch — retire old current, insert new
                    if norad_id in current_norad_ids:
                        retire_this_batch.append(norad_id)
                        current_norad_ids.discard(norad_id)

                    self.session.add(TLE(
                        norad_id   = norad_id,
                        name       = name,
                        line1      = line1,
                        line2      = line2,
                        epoch      = epoch,
                        is_current = True,
                        fetched_at = datetime.now(timezone.utc),
                        checksum   = pair_hash,
                    ))
                    all_epochs.add(key)
                    added_this_run.add(norad_id)
                    new_count    += 1
                    batch_pending += 1

                if batch_pending >= _BATCH_SIZE:
                    self._flush_batch(retire_this_batch)
                    retire_this_batch = []
                    batch_pending     = 0

            # Final flush for the remainder
            self._flush_batch(retire_this_batch)

        except Exception as exc:
            self.session.rollback()
            logger.error("tle_parse_and_store_failed", error=str(exc))
            raise

        result = {
            "new":     new_count,
            "updated": updated_count,
            "invalid": invalid_count,
            "skipped": skipped_count,
            "unused":  unused_count,
            "total":   new_count + updated_count,
        }
        logger.info("tle_ingestion_complete", **result)
        return result

    def _flush_batch(self, retire_norad_ids: List[int]) -> None:
        """
        Bulk-retire superseded TLEs and commit all pending session changes.

        A single UPDATE ... WHERE norad_id IN (...) replaces N individual
        UPDATEs — one round-trip regardless of how many satellites are retired.
        synchronize_session=False is safe here because the retired TLE objects
        are not accessed again after this point.
        """
        if retire_norad_ids:
            self.session.query(TLE).filter(
                TLE.norad_id.in_(retire_norad_ids),
                TLE.is_current == True,  # noqa: E712
            ).update({"is_current": False}, synchronize_session=False)

        self.session.commit()
