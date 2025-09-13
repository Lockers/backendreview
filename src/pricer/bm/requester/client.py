from __future__ import annotations

import asyncio
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


def _looks_like_json(text: str) -> bool:
    t = (text or "").strip()
    return (t.startswith("{") and t.endswith("}")) or (t.startswith("[") and t.endswith("]"))


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


class _TrackerAdapter:
    """Guarantees `.record(**event)` and proxies `.maybe_flush(run_id)` for whatever tracker impl you have."""

    def __init__(self, inner: Any):
        self._inner = inner
        self._buffer: list[dict[str, Any]] = []

    def record(self, **event: Any) -> None:
        for name in ("record", "record_event", "add", "add_event", "track", "log", "note"):
            fn = getattr(self._inner, name, None)
            if not fn:
                continue
            try:
                fn(**event)
                return
            except TypeError:
                try:
                    fn(event)
                    return
                except Exception:
                    continue
            except Exception:
                continue
        self._buffer.append(dict(event))

    def maybe_flush(self, run_id: str) -> dict[str, Any] | None:
        inner_flush = getattr(self._inner, "maybe_flush", None)
        snap = None
        if callable(inner_flush):
            try:
                snap = inner_flush(run_id)
            except Exception:
                snap = None
        if self._buffer:
            buf = self._buffer
            self._buffer = []
            return {"run_id": run_id, "snapshots": buf}
        return snap

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)


class Requester:
    """Run-scoped requester with per-category rate buckets, circuit breakers, and a learning tracker."""

    def __init__(self, settings: Settings, run_id: str, endpoint_rates_repo=None) -> None:
        self.settings = settings
        self.run_id = run_id
        self.endpoint_rates_repo = endpoint_rates_repo

        # Per-category token buckets (buyback is deliberately conservative)
        self._buckets: dict[str, TokenBucket] = {
            "seller_generic": TokenBucket(
                max_per_window=settings.seller_generic_max_per_window,
                window_seconds=settings.rate_window_seconds,
            ),
            "seller_mutations": TokenBucket(
                max_per_window=getattr(settings, "seller_mutations_max_per_window", 20),
                window_seconds=settings.rate_window_seconds,
            ),
            "buyback": TokenBucket(
                max_per_window=getattr(settings, "buyback_max_per_window", 30),  # ~1 req / 2s if window=60s
                window_seconds=settings.rate_window_seconds,
            ),
        }

        self._global_sem = asyncio.Semaphore(self.settings.global_max_concurrency)

        # Circuit breakers (do NOT open the breaker on 429; see send())
        self._breakers: dict[str, CircuitBreaker] = {
            "seller_generic": CircuitBreaker(fail_threshold=5, cooldown_seconds=60),
            "seller_mutations": CircuitBreaker(fail_threshold=5, cooldown_seconds=60),
            "buyback": CircuitBreaker(fail_threshold=5, cooldown_seconds=60),
        }

        self._session: aiohttp.ClientSession | None = None
        self._connector: aiohttp.TCPConnector | None = None

        # Learning tracker with compatibility wrapper
        self.tracker = _TrackerAdapter(LearningTracker())

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
        await self._maybe_flush_learning()
        if self._session and not self._session.closed:
            await self._session.close()
        if self._connector:
            await self._connector.close()
        log_json("requester_closed", run_id=self.run_id)

    async def _maybe_flush_learning(self) -> None:
        snap = self.tracker.maybe_flush(self.run_id)
        if snap and self.endpoint_rates_repo:
            try:
                await self.endpoint_rates_repo.insert_snapshots(snap["snapshots"])
                log_json("learning_snapshot_persisted", run_id=self.run_id, count=len(snap["snapshots"]))
            except Exception as e:
                log_json("learning_persist_error", run_id=self.run_id, error=str(e)[:300])

    def _track_event(self, *, endpoint_tag: str, status: int, elapsed_ms: int) -> None:
        try:
            self.tracker.record(endpoint_tag=endpoint_tag, status=status, elapsed_ms=elapsed_ms, ts=time.time())
        except Exception:
            pass

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
                log_json("rate_sleep", run_id=self.run_id, endpoint_tag=endpoint_tag, category=category, sleep_ms=int(slept * 1000))

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

                        log_json(
                            "http_response",
                            run_id=self.run_id,
                            endpoint_tag=endpoint_tag,
                            status=status,
                            content_type=ct,
                            elapsed_ms=elapsed_ms,
                            cf_ray=resp.headers.get("cf-ray"),
                        )

                        # Success
                        if 200 <= status < 300:
                            br and br.record_success()
                            if hasattr(bucket, "record_ok"):
                                bucket.record_ok(getattr(self.settings, "rl_recover_after_ok", 10))
                            self._track_event(endpoint_tag=endpoint_tag, status=status, elapsed_ms=elapsed_ms)
                            await self._maybe_flush_learning()

                            if status == 204:
                                return None
                            if "application/json" in ct or _looks_like_json(body_text):
                                return await resp.json(content_type=None)
                            return body_text.strip() or None

                        # Rate limit: DO NOT trip the circuit breaker for 429.
                        if status == 429:
                            if hasattr(bucket, "penalize"):
                                bucket.penalize(getattr(self.settings, "rl_penalty_factor", 0.5))
                            self._track_event(endpoint_tag=endpoint_tag, status=status, elapsed_ms=elapsed_ms)
                            await self._maybe_flush_learning()

                            # Respect Retry-After if present; otherwise fall back to ~1.2s
                            retry_after = resp.headers.get("Retry-After")
                            delay_s = 0.0
                            try:
                                if retry_after:
                                    delay_s = float(retry_after)
                            except Exception:
                                delay_s = 0.0
                            delay_s = delay_s or (getattr(self.settings, "buyback_429_sleep_ms", 1200) / 1000.0)
                            await asyncio.sleep(delay_s)

                            raise BackMarketRateLimitError("429 Too Many Requests")

                        # Server errors -> breaker failure
                        if 500 <= status < 600:
                            br and br.record_failure()
                            self._track_event(endpoint_tag=endpoint_tag, status=status, elapsed_ms=elapsed_ms)
                            await self._maybe_flush_learning()
                            raise BackMarketServerError(body_text[:500])

                        if status == 400:
                            raise BackMarketBadRequestError(body_text[:500])
                        if status == 401:
                            raise BackMarketAuthError(body_text[:500])
                        if status == 403:
                            br and br.record_failure()
                            raise BackMarketForbiddenError(body_text[:500])
                        if status == 404:
                            if retry_404:
                                br and br.record_failure()
                                self._track_event(endpoint_tag=endpoint_tag, status=status, elapsed_ms=elapsed_ms)
                                await self._maybe_flush_learning()
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
        cursor_param: str | None = None,
        next_field: str = "next",
        sleep_between_pages_ms: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Generic sequential pagination. Supports page-number OR cursor style.
        In cursor mode, the API must return an absolute 'next' URL in `next_field`.

        `sleep_between_pages_ms` – optional fixed pause after a successful page to keep endpoints happy.
        """
        assert self._session is not None, "Requester not started"

        pages: list[dict[str, Any]] = []
        base_url = urljoin(self.settings.bm_base_url, path.lstrip("/"))
        qs: Optional[Mapping[str, Any]] = dict(params or {})

        if cursor_param:
            if size_param:
                qs[size_param] = page_size
        else:
            qs[size_param] = page_size
            qs[page_param] = 1

        seen_pages = 0
        expected_pages: int | None = None

        log_json(
            "paginate_start",
            run_id=self.run_id,
            endpoint_tag=endpoint_tag,
            url=base_url,
            page_size=page_size,
            params=(dict(params) if params else {}),
        )

        current_url = base_url
        while True:
            attempt = 0
            last_exc: Exception | None = None

            while attempt < max_attempts_per_page:
                attempt += 1
                log_json(
                    "paginate_request_debug",
                    run_id=self.run_id,
                    endpoint_tag=endpoint_tag,
                    page_index=seen_pages + 1,
                    current_url=current_url,
                    qs=qs,
                )
                try:
                    payload = await self.send("GET", current_url, params=qs, endpoint_tag=endpoint_tag, category=category)
                    if not isinstance(payload, dict):
                        raise BackMarketDataError("Expected JSON object page")

                    pages.append(payload)
                    seen_pages += 1

                    if not cursor_param and expected_pages is None and isinstance(payload.get("count"), int):
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

                    nxt = payload.get(next_field)
                    log_json("paginate_next_debug", run_id=self.run_id, endpoint_tag=endpoint_tag, page_index=seen_pages, next_url=nxt)

                    if not nxt:
                        log_json("paginate_complete", run_id=self.run_id, endpoint_tag=endpoint_tag, pages=seen_pages, expected_pages=expected_pages)
                        return pages

                    # Friendly fixed pause between pages (esp. for buyback cursor)
                    if sleep_between_pages_ms > 0:
                        await asyncio.sleep(sleep_between_pages_ms / 1000.0)

                    if max_pages_guard is None and expected_pages:
                        guard = expected_pages * 5
                    else:
                        guard = max_pages_guard or 100
                    if seen_pages >= guard:
                        raise BackMarketAPIError("Pagination guard tripped")

                    current_url = nxt
                    qs = None  # absolute next URL already includes params
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
                raise BackMarketAPIError(f"Failed to fetch page {seen_pages + 1} after {max_attempts_per_page} attempts") from last_exc















