from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Mapping, Optional
from urllib.parse import urljoin, urlsplit

import aiohttp

from pricer.core.exceptions import (
    BackMarketAPIError,
    BackMarketAuthError,
    BackMarketBadRequestError,
    BackMarketDataError,
    BackMarketForbiddenError,
    BackMarketNetworkError,
    BackMarketNotFoundError,
    BackMarketRateLimitError,
    BackMarketServerError,
    BackMarketWAFError,
)
from pricer.core.settings import Settings
from pricer.bm.requester.identity import default_headers
from pricer.bm.requester.rate_limiter import TokenBucket
from pricer.bm.requester.circuit_breaker import CircuitBreaker
from pricer.bm.requester.retry_policy import backoff_delay_ms
from pricer.bm.requester.learning_tracker import LearningTracker
from pricer.utils.logging import log_json, sanitize_headers, summarize_proxy

logger = logging.getLogger(__name__)

ALLOWED_METHODS = {"GET", "POST", "PUT"}


def _is_html(content_type: str | None) -> bool:
    return (content_type or "").lower().startswith("text/html")


def _looks_like_json(text: str) -> bool:
    t = text.strip()
    return (t.startswith("{") and t.endswith("}")) or (t.startswith("[") and t.endswith("]"))


def _looks_like_waf_text(text: str) -> bool:
    t = text.lower()
    return (
        ("cloudflare" in t and "attention required" in t)
        or ("just a moment" in t)
        or ("checking your browser" in t)
        or ("cf-ray" in t)
        or ("ddos" in t and "protection" in t)
    )


def _parse_retry_after(h: str | None) -> int | None:
    if not h:
        return None
    try:
        return int(h)
    except Exception:
        return None


def _effective_proxy_for_url(url: str) -> str | None:
    scheme = urlsplit(url).scheme.lower()
    if scheme == "https":
        return (
            os.environ.get("HTTPS_PROXY")
            or os.environ.get("https_proxy")
            or os.environ.get("HTTP_PROXY")
            or os.environ.get("http_proxy")
        )
    if scheme == "http":
        return os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    return None


class Requester:
    """
    Run-scoped requester: owns session, rate limits & circuit breakers.
    Public methods: send(), paginate().
    """

    def __init__(self, settings: Settings, run_id: str, endpoint_rates_repo=None) -> None:
        self.settings = settings
        self.run_id = run_id
        self.endpoint_rates_repo = endpoint_rates_repo

        # ----- Per-category token buckets -----
        self._buckets: dict[str, TokenBucket] = {
            "seller_generic": TokenBucket(
                max_per_window=settings.seller_generic_max_per_window,
                window_seconds=settings.rate_window_seconds,
            ),
            "seller_mutations": TokenBucket(
                max_per_window=getattr(settings, "seller_mutations_max_per_window", 20),
                window_seconds=settings.rate_window_seconds,
            ),
        }

        self._global_sem = asyncio.Semaphore(self.settings.global_max_concurrency)

        # ----- Circuit breakers -----
        self._breakers: dict[str, CircuitBreaker] = {
            "seller_generic": CircuitBreaker(fail_threshold=5, cooldown_seconds=60),
            "seller_mutations": CircuitBreaker(fail_threshold=5, cooldown_seconds=60),
        }

        self._session: aiohttp.ClientSession | None = None
        self._connector: aiohttp.TCPConnector | None = None

        # Learning tracker
        self.tracker = LearningTracker()

    async def __aenter__(self) -> "Requester":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def start(self) -> None:
        if self._session and not self._session.closed:
            return
        self._connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, enable_cleanup_closed=True)
        timeout = aiohttp.ClientTimeout(
            total=self.settings.total_timeout_ms / 1000.0,
            connect=self.settings.connect_timeout_ms / 1000.0,
            sock_read=self.settings.read_timeout_ms / 1000.0,
        )
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            headers=default_headers(self.settings),
            timeout=timeout,
            raise_for_status=False,
            trust_env=True,
        )
        log_json("requester_started", run_id=self.run_id)

    async def close(self) -> None:
        # ensure we persist a baseline snapshot even if no 429s occurred
        await self._maybe_flush_learning(force=True)
        if self._session and not self._session.closed:
            await self._session.close()
        if self._connector:
            await self._connector.close()
        log_json("requester_closed", run_id=self.run_id)

    # ---------------- Learning flush ----------------

    async def _maybe_flush_learning(self, force: bool = False) -> None:
        snap = self.tracker.maybe_flush(self.run_id, force=force)
        if snap and self.endpoint_rates_repo:
            try:
                await self.endpoint_rates_repo.insert_snapshots(snap["snapshots"])
                log_json(
                    "learning_snapshot_persisted",
                    run_id=self.run_id,
                    count=len(snap["snapshots"]),
                )
            except Exception as e:
                log_json(
                    "learning_persist_error",
                    run_id=self.run_id,
                    error=str(e)[:300],
                )

    # ---------------- Public API ----------------

    async def send(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Any | None = None,
        headers: Optional[Mapping[str, str]] = None,
        endpoint_tag: str,
        category: str,
        timeout_ms: int | None = None,
        idempotency_key: str | None = None,
        retry_404: bool = False,
    ) -> dict[str, Any] | list[Any] | str | None:
        assert self._session is not None, "Requester not started"

        method_upper = method.upper()
        if method_upper not in ALLOWED_METHODS:
            raise BackMarketAPIError(f"HTTP method '{method}' not allowed")

        # Circuit breaker gate
        br = self._breakers.get(category)
        if br and not br.allow():
            sleep = br.remaining_cooldown()
            raise BackMarketAPIError(f"Circuit open for {category}; retry after {sleep:.1f}s")

        # Rate bucket
        bucket = self._buckets.get(category)
        if bucket is None:
            raise BackMarketAPIError(f"No rate bucket configured for category '{category}'")

        # Build URL
        url = path if path.startswith(("http://", "https://")) else urljoin(self.settings.bm_base_url, path.lstrip("/"))

        # Build headers
        merged_headers = dict(default_headers(self.settings))
        if headers:
            merged_headers.update(headers)
        if json_body is not None and method_upper in {"POST", "PUT"}:
            merged_headers.setdefault("Content-Type", "application/json")
        if idempotency_key and method_upper in {"POST", "PUT"}:
            merged_headers["Idempotency-Key"] = idempotency_key

        timeout = aiohttp.ClientTimeout(total=timeout_ms / 1000.0) if timeout_ms else self._session.timeout

        attempts = 0
        last_exc: Exception | None = None

        while True:
            attempts += 1
            slept = await bucket.acquire()
            if slept:
                log_json(
                    "rate_sleep",
                    run_id=self.run_id,
                    endpoint_tag=endpoint_tag,
                    category=category,
                    sleep_ms=int(slept * 1000),
                )

            effective_proxy = _effective_proxy_for_url(url)
            log_json(
                "http_request",
                run_id=self.run_id,
                endpoint_tag=endpoint_tag,
                method=method_upper,
                url=url,
                params=(dict(params) if params else {}),
                headers=sanitize_headers(merged_headers),
                proxy=summarize_proxy(effective_proxy),
            )

            started = time.perf_counter()
            try:
                async with self._global_sem:
                    async with self._session.request(
                        method_upper,
                        url,
                        params=params,
                        json=json_body,
                        headers=merged_headers,
                        timeout=timeout,
                    ) as resp:
                        status = resp.status
                        ct = (resp.headers.get("Content-Type") or "").lower()
                        body_text = await resp.text()
                        elapsed_ms = int((time.perf_counter() - started) * 1000)
                        cf_ray = resp.headers.get("cf-ray")

                        log_json(
                            "http_response",
                            run_id=self.run_id,
                            endpoint_tag=endpoint_tag,
                            status=status,
                            content_type=ct,
                            elapsed_ms=elapsed_ms,
                            cf_ray=cf_ray,
                        )

                        # --- Handle success ---
                        if 200 <= status < 300:
                            br and br.record_success()
                            if hasattr(bucket, "record_ok"):
                                bucket.record_ok(getattr(self.settings, "rl_recover_after_ok", 10))

                            # record ok in tracker and opportunistically flush
                            self.tracker.record_ok(endpoint_tag, category)
                            await self._maybe_flush_learning()

                            if status == 204:
                                return None
                            if "application/json" in ct or _looks_like_json(body_text):
                                return await resp.json(content_type=None)
                            return body_text.strip() or None

                        # --- Handle errors ---
                        if status == 429:
                            # track rate-limit & penalize
                            self.tracker.record_429(endpoint_tag, category)
                            br and br.record_failure()
                            if hasattr(bucket, "penalize"):
                                bucket.penalize(getattr(self.settings, "rl_penalty_factor", 0.5))
                            raise BackMarketRateLimitError("429 Too Many Requests")

                        if 500 <= status < 600:
                            # track 5xx as well
                            self.tracker.record_5xx(endpoint_tag, category)
                            br and br.record_failure()
                            raise BackMarketServerError(body_text[:500])

                        if status == 400:
                            raise BackMarketBadRequestError(body_text[:500])
                        if status == 401:
                            raise BackMarketAuthError(body_text[:500])
                        if status == 403:
                            raise BackMarketForbiddenError(body_text[:500])
                        if status == 404:
                            if retry_404 and attempts < 3:
                                # let retry loop handle; fall through to exception below
                                pass
                            else:
                                raise BackMarketNotFoundError(body_text[:500])

                        raise BackMarketAPIError(f"Unexpected status {status}: {body_text[:300]}")

            except (BackMarketRateLimitError, BackMarketServerError, BackMarketNetworkError, aiohttp.ClientError) as exc:
                last_exc = exc
                delay_ms = backoff_delay_ms(attempts, base_ms=500, max_ms=10_000)
                await asyncio.sleep(delay_ms / 1000.0)
                continue
            except Exception as exc:
                last_exc = exc
                break

        if isinstance(last_exc, Exception):
            raise last_exc
        raise BackMarketAPIError("Unknown requester failure")

    async def paginate(
        self,
        path: str,
        *,
        page_param: str = "page",
        size_param: str = "page-size",
        page_size: int = 50,
        params: Optional[Mapping[str, Any]] = None,
        endpoint_tag: str,
        category: str,
        max_pages_guard: int | None = None,
        max_attempts_per_page: int = 12,
    ) -> list[dict[str, Any]]:
        """
        Sequential pagination following 'next' links.
        Retries each page with backoff.
        """
        assert self._session is not None, "Requester not started"

        pages: list[dict[str, Any]] = []
        current_url = urljoin(self.settings.bm_base_url, path.lstrip("/"))
        qs: Optional[Mapping[str, Any]] = dict(params or {})
        qs[size_param] = page_size
        qs[page_param] = 1

        seen_pages = 0
        expected_pages: int | None = None

        log_json(
            "paginate_start",
            run_id=self.run_id,
            endpoint_tag=endpoint_tag,
            url=current_url,
            page_size=page_size,
            params=(dict(params) if params else {}),
        )

        while True:
            attempt = 0
            last_exc: Exception | None = None

            while attempt < max_attempts_per_page:
                attempt += 1
                try:
                    log_json(
                        "paginate_request_debug",
                        run_id=self.run_id,
                        endpoint_tag=endpoint_tag,
                        page_index=seen_pages + 1,
                        current_url=current_url,
                        qs=qs,
                    )

                    payload = await self.send(
                        "GET",
                        current_url,
                        params=qs,
                        endpoint_tag=endpoint_tag,
                        category=category,
                    )
                    if not isinstance(payload, dict):
                        raise BackMarketDataError("Expected JSON object page")

                    pages.append(payload)
                    seen_pages += 1

                    if expected_pages is None and isinstance(payload.get("count"), int):
                        total = payload["count"]
                        expected_pages = max(1, (total + page_size - 1) // page_size)

                    log_json(
                        "paginate_page_ok",
                        run_id=self.run_id,
                        endpoint_tag=endpoint_tag,
                        page_index=seen_pages,
                        expected_pages=expected_pages,
                        results_count=len(payload.get("results") or []),
                    )

                    nxt = payload.get("next")
                    log_json(
                        "paginate_next_debug",
                        run_id=self.run_id,
                        endpoint_tag=endpoint_tag,
                        page_index=seen_pages,
                        next_url=nxt,
                    )

                    if not nxt:
                        log_json(
                            "paginate_complete",
                            run_id=self.run_id,
                            endpoint_tag=endpoint_tag,
                            pages=seen_pages,
                            expected_pages=expected_pages,
                        )
                        # force a snapshot at the end of a clean run
                        await self._maybe_flush_learning(force=True)
                        return pages

                    if max_pages_guard is None and expected_pages:
                        guard = expected_pages * 5
                    else:
                        guard = max_pages_guard or 100
                    if seen_pages >= guard:
                        raise BackMarketAPIError("Pagination guard tripped")

                    current_url = nxt
                    qs = None
                    break

                except Exception as exc:
                    last_exc = exc
                    delay_ms = backoff_delay_ms(attempt, base_ms=600, max_ms=12_000)
                    log_json(
                        "paginate_page_retry",
                        run_id=self.run_id,
                        endpoint_tag=endpoint_tag,
                        page_index=seen_pages + 1,
                        attempt=attempt,
                        delay_ms=delay_ms,
                        error=str(exc)[:300],
                    )
                    await asyncio.sleep(delay_ms / 1000.0)

            if attempt >= max_attempts_per_page:
                err_msg = f"Failed to fetch page {seen_pages + 1} after {max_attempts_per_page} attempts"
                log_json(
                    "paginate_page_failed",
                    run_id=self.run_id,
                    endpoint_tag=endpoint_tag,
                    page_index=seen_pages + 1,
                    attempts=max_attempts_per_page,
                    error=str(last_exc)[:500] if last_exc else "unknown",
                )
                raise BackMarketAPIError(err_msg) from last_exc









