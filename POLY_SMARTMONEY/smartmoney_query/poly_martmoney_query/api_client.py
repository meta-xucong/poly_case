from __future__ import annotations

import datetime as dt
import email.utils
import math
import os
import random
import time
from typing import Any, Dict, Iterable, List, Optional

import requests

from .models import ClosedPosition, Position, Trade, TradeAction

MAX_BACKOFF_SECONDS = float(os.environ.get("SMART_QUERY_MAX_BACKOFF", "60"))
MAX_REQUESTS_PER_SECOND = float(os.environ.get("SMART_QUERY_MAX_RPS", "2"))
MIN_REQUEST_INTERVAL = 1.0 / MAX_REQUESTS_PER_SECOND if MAX_REQUESTS_PER_SECOND > 0 else 0.0
BASE_PAGE_SLEEP = float(os.environ.get("SMART_QUERY_BASE_SLEEP", "0.3"))
HFT_MAX_ACTIVITY_RECORDS = int(os.environ.get("SMART_HFT_MAX_ACTIVITY_RECORDS", "50000"))
HFT_MAX_UNIQUE_TX = int(os.environ.get("SMART_HFT_MAX_UNIQUE_TX", "20000"))


class RateLimiter:
    """简单的全局限速器，用于跨用户共享节流。"""

    def __init__(self, rps: float) -> None:
        self.rps = max(rps, 0.1)
        self.min_interval = 1.0 / self.rps
        self._next_ts = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        if now < self._next_ts:
            time.sleep(self._next_ts - now)
        self._next_ts = max(self._next_ts, now) + self.min_interval


_GLOBAL_LIMITER = RateLimiter(MAX_REQUESTS_PER_SECOND if MIN_REQUEST_INTERVAL > 0 else 1000.0)


def _sleep_with_jitter(wait: float, jitter_ratio: float = 0.1) -> None:
    if wait <= 0:
        return
    jitter = min(wait * jitter_ratio, 1.0)
    time.sleep(wait + random.random() * jitter)


def _parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    if not header_value:
        return None
    try:
        return float(header_value)
    except ValueError:
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(header_value)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return max(parsed.timestamp() - dt.datetime.now(tz=dt.timezone.utc).timestamp(), 0.0)


def _coerce_int(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return int(value.timestamp())
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    try:
        if isinstance(value, str):
            s = value.strip()
            if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
                return int(s)
            try:
                f = float(s)
                if math.isfinite(f):
                    return int(f)
            except Exception:
                pass
    except Exception:
        pass
    return value


def _sanitize_query_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not params:
        return params
    sanitized = dict(params)
    for key in ("start", "end", "limit", "offset"):
        if key in sanitized:
            sanitized[key] = _coerce_int(sanitized[key])
    return sanitized


def _request_with_backoff(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 15.0,
    retries: int = 3,
    backoff: float = 2.0,
    max_backoff: float = MAX_BACKOFF_SECONDS,
    session: Optional[requests.Session] = None,
    limiter: Optional[RateLimiter] = None,
) -> tuple[Optional[requests.Response], Optional[str]]:
    """复用原版脚本的指数回退 + 抖动请求封装。"""

    attempt = 1
    client = session or requests
    limiter = limiter or _GLOBAL_LIMITER
    while True:
        try:
            limiter.wait()
            params = _sanitize_query_params(params)
            resp = client.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp, None
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            body = None
            try:
                if exc.response is not None:
                    body = exc.response.text
            except Exception:
                pass

            retryable_statuses = {408, 429}
            if status is not None and 500 <= status < 600:
                retryable_statuses.add(status)

            if status is not None and 400 <= status < 500 and status not in retryable_statuses:
                extra = f" | body={body[:200]}" if body else ""
                msg = f"HTTP {status} {exc}{extra}"
                print(
                    f"[WARN] 请求失败（{attempt}/{retries}）：{url} params={params} -> {msg}",
                )
                return None, msg

            if attempt >= retries:
                extra = f" | body={body[:200]}" if body else ""
                msg = f"HTTP {status} {exc}{extra}"
                print(
                    f"[WARN] 请求失败（{attempt}/{retries}）：{url} params={params} -> {msg}",
                )
                return None, msg

            retry_after = None
            if status == 429 and exc.response is not None:
                retry_after = _parse_retry_after(exc.response.headers.get("Retry-After"))
            wait = (
                max(retry_after, backoff * (2 ** (attempt - 1)))
                if retry_after is not None
                else min(max_backoff, backoff * (2 ** (attempt - 1)))
            )
            _sleep_with_jitter(wait)
            attempt += 1
        except requests.RequestException as exc:
            if attempt >= retries:
                msg = str(exc)
                print(
                    f"[WARN] 请求失败（{attempt}/{retries}）：{url} params={params} -> {msg}",
                )
                return None, msg

            wait = min(max_backoff, backoff * (2 ** (attempt - 1)))
            _sleep_with_jitter(wait)
            attempt += 1


def _to_timestamp(value: Optional[dt.datetime]) -> Optional[float]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.timestamp()


def _to_timestamp_int(value: Optional[dt.datetime]) -> Optional[int]:
    ts = _to_timestamp(value)
    return None if ts is None else int(ts)


def _shift_before(timestamp: dt.datetime) -> dt.datetime:
    return timestamp - dt.timedelta(microseconds=1)


def _parse_timestamp(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            if value > 1e12:
                value /= 1000.0
            return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = dt.datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=dt.timezone.utc)
            return parsed
    except Exception:
        return None
    return None


class DataApiClient:
    """Polymarket Data-API 客户端，覆盖 leaderboard 与 trades。"""

    def __init__(
        self,
        host: str = "https://data-api.polymarket.com",
        session: Optional[requests.Session] = None,
    ) -> None:
        self.host = host.rstrip("/")
        self.session = session or requests.Session()

    def fetch_leaderboard(
        self,
        *,
        period: str = "ALL",
        order_by: str = "vol",
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        url = f"{self.host}/v1/leaderboard"
        period_key = period.upper()
        period_aliases = {
            "MONTHLY": "MONTH",
            "WEEKLY": "WEEK",
            "DAILY": "DAY",
        }
        order_key = order_by.strip().lower()
        order_aliases = {
            "profit": "PNL",
            "pnl": "PNL",
            "vol": "VOL",
            "volume": "VOL",
        }
        limit = max(1, min(limit, 50))
        params = {
            "timePeriod": period_aliases.get(period_key, period_key),
            "orderBy": order_aliases.get(order_key, order_by.strip().upper()),
            "limit": limit,
            "offset": offset,
        }
        resp, _ = _request_with_backoff(url, params=params, session=self.session)
        if resp is None:
            return []

        try:
            data = resp.json()
        except Exception:
            return []

        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            payload = data.get("data") or data.get("leaderboard")
            return payload if isinstance(payload, list) else []
        return []

    def iter_leaderboard(
        self,
        *,
        period: str = "ALL",
        order_by: str = "vol",
        page_size: int = 100,
        max_pages: Optional[int] = 20,
    ) -> Iterable[Dict[str, Any]]:
        page = 0
        offset = 0
        page_size = max(1, min(page_size, 50))
        while True:
            batch = self.fetch_leaderboard(
                period=period, order_by=order_by, limit=page_size, offset=offset
            )
            if not batch:
                break
            for item in batch:
                yield item
            offset += len(batch)
            page += 1
            if max_pages is not None and page >= max_pages:
                break
            if len(batch) < page_size:
                break

    def fetch_trades(
        self,
        user: str,
        *,
        start_time: Optional[dt.datetime] = None,
        end_time: Optional[dt.datetime] = None,
        page_size: int = 100,
        max_pages: Optional[int] = 200,
        taker_only: bool = False,
    ) -> List[Trade]:
        url = f"{self.host}/trades"
        page_size = max(1, min(int(page_size), 10000))
        offset = 0
        page = 0
        results: List[Trade] = []
        start_ts = start_time.timestamp() if start_time else None
        end_ts = end_time.timestamp() if end_time else None

        while True:
            if offset >= 10000:
                break
            params = {
                "user": user,
                "limit": page_size,
                "offset": offset,
                "takerOnly": taker_only,
            }
            resp, _ = _request_with_backoff(url, params=params, session=self.session)
            if resp is None:
                break

            try:
                payload = resp.json()
            except Exception:
                break

            raw_trades = []
            if isinstance(payload, list):
                raw_trades = payload
            elif isinstance(payload, dict):
                raw_trades = payload.get("data") or payload.get("trades") or []
            if not isinstance(raw_trades, list):
                break

            parsed_batch: List[Trade] = []
            reached_earliest = False
            for item in raw_trades:
                trade = Trade.from_api(item)
                if trade is None:
                    continue
                ts = trade.timestamp.timestamp()
                if end_ts is not None and ts > end_ts:
                    continue
                if start_ts is not None and ts < start_ts:
                    reached_earliest = True
                    continue
                parsed_batch.append(trade)

            results.extend(parsed_batch)

            if reached_earliest or len(raw_trades) < page_size:
                break

            offset += len(raw_trades)
            page += 1
            if max_pages is not None and page >= max_pages:
                break

        results.sort(key=lambda t: t.timestamp)
        return results

    def fetch_trade_actions_window(
        self,
        user: str,
        *,
        start_time: Optional[dt.datetime] = None,
        end_time: Optional[dt.datetime] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        taker_only: bool = False,
        return_info: bool = False,
    ) -> List[TradeAction] | tuple[List[TradeAction], Dict[str, object]]:
        raise NotImplementedError(
            "fetch_trade_actions_window() 已禁用：/trades 的 start/end 不可靠。"
            "请使用 fetch_trade_actions_window_from_activity()。"
        )

    def fetch_trade_actions_window_from_activity(
        self,
        user: str,
        *,
        start_time: Optional[dt.datetime] = None,
        end_time: Optional[dt.datetime] = None,
        page_size: int = 300,
        max_offset: int = 10000,
        progress_every: Optional[int] = None,
        return_info: bool = False,
    ) -> List[TradeAction] | tuple[List[TradeAction], Dict[str, object]]:
        url = f"{self.host}/activity"
        page_size = max(1, min(int(page_size), 500))
        max_offset = max(0, min(int(max_offset), 10000))
        start_ts_sec = _to_timestamp_int(start_time)
        end_ts_sec = _to_timestamp_int(end_time)
        actions: Dict[str, dt.datetime] = {}
        ok = True
        incomplete = False
        hit_max_pages = False
        last_error: Optional[str] = None
        pages_fetched = 0
        activity_records_fetched = 0
        hit_cap = False
        suspected_hft = False
        cap_reason: Optional[str] = None
        cursor_end_sec = (
            end_ts_sec
            if end_ts_sec is not None
            else int(dt.datetime.now(tz=dt.timezone.utc).timestamp())
        )
        offset = 0
        progress_every = progress_every or 0

        while True:
            params = {
                "user": user,
                "type": "TRADE",
                "limit": page_size,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
                "end": int(cursor_end_sec),
            }
            if start_ts_sec is not None:
                params["start"] = int(start_ts_sec)

            resp, error_msg = _request_with_backoff(url, params=params, session=self.session)
            if resp is None:
                error_msg = error_msg or "request_failed"
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, error_msg)
                break

            try:
                payload = resp.json()
            except Exception:
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_json")
                break

            raw_items = []
            if isinstance(payload, list):
                raw_items = payload
            elif isinstance(payload, dict):
                raw_items = payload.get("data") or payload.get("activity") or []
            if not isinstance(raw_items, list):
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_payload")
                break

            if not raw_items:
                break
            activity_records_fetched += len(raw_items)

            min_ts_sec = None
            for item in raw_items:
                tx_hash = (
                    str(
                        item.get("transactionHash")
                        or item.get("txHash")
                        or item.get("tx_hash")
                        or ""
                    ).strip()
                )
                ts = _parse_timestamp(item.get("timestamp") or item.get("time") or item.get("createdAt"))
                if not tx_hash or ts is None:
                    continue
                ts_sec = int(ts.timestamp())
                if start_ts_sec is not None and ts_sec < start_ts_sec:
                    continue
                if end_ts_sec is not None and ts_sec > end_ts_sec:
                    continue
                existing = actions.get(tx_hash)
                if existing is None or ts < existing:
                    actions[tx_hash] = ts
                if min_ts_sec is None or ts_sec < min_ts_sec:
                    min_ts_sec = ts_sec

            pages_fetched += 1
            if progress_every and pages_fetched % progress_every == 0:
                print(
                    f"[INFO] trade_actions 翻页进度：pages={pages_fetched} "
                    f"actions={len(actions)} cursor_end={cursor_end_sec} offset={offset}",
                    flush=True,
                )

            if activity_records_fetched >= HFT_MAX_ACTIVITY_RECORDS:
                hit_cap = True
                suspected_hft = True
                cap_reason = (
                    f"hft_records records>={HFT_MAX_ACTIVITY_RECORDS} "
                    f"(records={activity_records_fetched}, unique_tx={len(actions)}, "
                    f"pages={pages_fetched})"
                )
                ok = True
                incomplete = True
                last_error = _combine_error(last_error, cap_reason)
                break

            if len(actions) >= HFT_MAX_UNIQUE_TX:
                hit_cap = True
                suspected_hft = True
                cap_reason = (
                    f"hft_unique_tx unique_tx>={HFT_MAX_UNIQUE_TX} "
                    f"(records={activity_records_fetched}, unique_tx={len(actions)}, "
                    f"pages={pages_fetched})"
                )
                ok = True
                incomplete = True
                last_error = _combine_error(last_error, cap_reason)
                break

            if len(raw_items) < page_size:
                break

            if min_ts_sec is None:
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "missing_timestamps")
                break

            offset += len(raw_items)

            if offset >= max_offset:
                if min_ts_sec is None:
                    hit_max_pages = True
                    incomplete = True
                    ok = False
                    last_error = _combine_error(
                        last_error, "missing_timestamps_for_window_shift"
                    )
                    break

                if min_ts_sec >= cursor_end_sec:
                    hit_max_pages = True
                    incomplete = True
                    ok = False
                    last_error = _combine_error(
                        last_error, f"hit_max_offset_same_end={max_offset}"
                    )
                    break

                cursor_end_sec = int(min_ts_sec)
                offset = 0

            if start_ts_sec is not None and cursor_end_sec < start_ts_sec:
                break

            _sleep_with_jitter(BASE_PAGE_SLEEP)

        action_list = [
            TradeAction(tx_hash=tx_hash, timestamp=timestamp)
            for tx_hash, timestamp in actions.items()
        ]
        action_list.sort(key=lambda t: t.timestamp)
        info = {
            "ok": ok,
            "incomplete": incomplete,
            "error_msg": last_error,
            "hit_max_pages": hit_max_pages,
            "pages_fetched": pages_fetched,
            "cursor_end_sec": cursor_end_sec,
            "actions_count": len(actions),
            "activity_records_fetched": activity_records_fetched,
            "hit_cap": hit_cap,
            "suspected_hft": suspected_hft,
            "cap_reason": cap_reason,
        }

        return (action_list, info) if return_info else action_list

    def fetch_activity_actions(
        self,
        user: str,
        *,
        start_time: Optional[dt.datetime] = None,
        end_time: Optional[dt.datetime] = None,
        page_size: int = 300,
        max_offset: int = 10000,
        return_info: bool = False,
    ) -> List[Dict[str, Any]] | tuple[List[Dict[str, Any]], Dict[str, object]]:
        url = f"{self.host}/activity"
        page_size = max(1, min(int(page_size), 500))
        max_offset = max(0, min(int(max_offset), 10000))
        start_ts_sec = _to_timestamp_int(start_time)
        end_ts_sec = _to_timestamp_int(end_time)
        ok = True
        incomplete = False
        hit_max_pages = False
        last_error: Optional[str] = None
        pages_fetched = 0
        cursor_end_sec = (
            end_ts_sec
            if end_ts_sec is not None
            else int(dt.datetime.now(tz=dt.timezone.utc).timestamp())
        )
        offset = 0
        results: List[Dict[str, Any]] = []

        while True:
            params = {
                "user": user,
                "type": "TRADE",
                "limit": page_size,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
                "end": int(cursor_end_sec),
            }
            if start_ts_sec is not None:
                params["start"] = int(start_ts_sec)

            resp, error_msg = _request_with_backoff(url, params=params, session=self.session)
            if resp is None:
                error_msg = error_msg or "request_failed"
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, error_msg)
                break

            try:
                payload = resp.json()
            except Exception:
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_json")
                break

            raw_items = []
            if isinstance(payload, list):
                raw_items = payload
            elif isinstance(payload, dict):
                raw_items = payload.get("data") or payload.get("activity") or []
            if not isinstance(raw_items, list):
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_payload")
                break

            if not raw_items:
                break

            min_ts_sec = None
            for item in raw_items:
                ts = _parse_timestamp(item.get("timestamp") or item.get("time") or item.get("createdAt"))
                if ts is None:
                    continue
                ts_sec = int(ts.timestamp())
                if start_ts_sec is not None and ts_sec < start_ts_sec:
                    continue
                if end_ts_sec is not None and ts_sec > end_ts_sec:
                    continue
                results.append(item)
                if min_ts_sec is None or ts_sec < min_ts_sec:
                    min_ts_sec = ts_sec

            pages_fetched += 1
            if len(raw_items) < page_size:
                break

            offset += len(raw_items)
            if offset >= max_offset:
                if min_ts_sec is None:
                    hit_max_pages = True
                    incomplete = True
                    ok = False
                    last_error = _combine_error(last_error, "missing_timestamps_for_window_shift")
                    break
                if min_ts_sec >= cursor_end_sec:
                    hit_max_pages = True
                    incomplete = True
                    ok = False
                    last_error = _combine_error(
                        last_error, f"hit_max_offset_same_end={max_offset}"
                    )
                    break
                cursor_end_sec = int(min_ts_sec)
                offset = 0

            if start_ts_sec is not None and cursor_end_sec < start_ts_sec:
                break

            _sleep_with_jitter(BASE_PAGE_SLEEP)

        info = {
            "ok": ok,
            "incomplete": incomplete,
            "error_msg": last_error,
            "hit_max_pages": hit_max_pages,
            "pages_fetched": pages_fetched,
            "cursor_end_sec": cursor_end_sec,
            "total": len(results),
            "limit": page_size,
        }

        return (results, info) if return_info else results

    def fetch_account_start_time_from_activity(
        self,
        user: str,
    ) -> Optional[dt.datetime]:
        url = f"{self.host}/activity"
        params = {
            "user": user,
            "type": "TRADE",
            "limit": 1,
            "offset": 0,
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
        }
        resp, _ = _request_with_backoff(url, params=params, session=self.session)
        if resp is None:
            return None
        try:
            payload = resp.json()
        except Exception:
            return None

        raw_items = []
        if isinstance(payload, list):
            raw_items = payload
        elif isinstance(payload, dict):
            raw_items = payload.get("data") or payload.get("activity") or []
        if not isinstance(raw_items, list) or not raw_items:
            return None
        ts = _parse_timestamp(
            raw_items[0].get("timestamp") or raw_items[0].get("time") or raw_items[0].get("createdAt")
        )
        return ts

    def fetch_positions(
        self,
        user: str,
        *,
        size_threshold: float = 0.0,
        page_size: int = 200,
        max_pages: Optional[int] = 50,
        sort_by: str = "TOKENS",
        sort_dir: str = "DESC",
        return_info: bool = False,
    ) -> List[Position] | tuple[List[Position], Dict[str, object]]:
        url = f"{self.host}/positions"
        page_size = max(1, min(int(page_size), 500))
        offset = 0
        page = 0
        results: List[Position] = []
        ok = True
        incomplete = False
        hit_max_pages = False
        last_error: Optional[str] = None
        pages_fetched = 0

        while True:
            if offset >= 10000:
                hit_max_pages = True
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "offset_exceeded_10000")
                break
            params = {
                "user": user,
                "limit": page_size,
                "offset": offset,
                "sizeThreshold": size_threshold,
                "sortBy": sort_by,
                "sortDirection": sort_dir,
            }
            resp, error_msg = _request_with_backoff(url, params=params, session=self.session)
            if resp is None:
                error_msg = error_msg or "request_failed"
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, error_msg)
                break

            try:
                payload = resp.json()
            except Exception:
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_json")
                break

            raw_positions = []
            if isinstance(payload, list):
                raw_positions = payload
            elif isinstance(payload, dict):
                raw_positions = payload.get("data") or payload.get("positions") or []
            if not isinstance(raw_positions, list):
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_payload")
                break

            for item in raw_positions:
                position = Position.from_api(item, user=user)
                if position is None:
                    continue
                results.append(position)

            pages_fetched += 1
            if len(raw_positions) < page_size:
                break

            offset += len(raw_positions)
            page += 1
            if max_pages is not None and page >= max_pages:
                hit_max_pages = True
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, f"hit_max_pages={max_pages}")
                print(
                    f"[WARN] positions 分页被截断：user={user} hit max_pages={max_pages}",
                    flush=True,
                )
                break

        info = {
            "ok": ok,
            "incomplete": incomplete,
            "error_msg": last_error,
            "hit_max_pages": hit_max_pages,
            "pages_fetched": pages_fetched,
            "total": len(results),
            "limit": page_size,
            "max_pages": max_pages,
        }

        return (results, info) if return_info else results

    def fetch_closed_positions(
        self,
        user: str,
        *,
        start_time: Optional[dt.datetime] = None,
        end_time: Optional[dt.datetime] = None,
        page_size: int = 100,
        max_offset: int = 100000,
        max_pages: Optional[int] = 2000,
        sort_by: str = "TIMESTAMP",
        sort_dir: str = "DESC",
        return_info: bool = False,
    ) -> List[ClosedPosition] | tuple[List[ClosedPosition], Dict[str, object]]:
        url = f"{self.host}/closed-positions"
        page_size = max(1, min(int(page_size), 50))
        max_offset = max(0, int(max_offset))
        max_pages_cap = math.ceil(max_offset / page_size) if max_offset > 0 else 0
        if max_pages is None:
            max_pages = max_pages_cap or None
        elif max_pages_cap:
            max_pages = min(max_pages, max_pages_cap)
        offset = 0
        page = 0
        results: List[ClosedPosition] = []
        start_ts = start_time.timestamp() if start_time else None
        end_ts = end_time.timestamp() if end_time else None
        ok = True
        incomplete = False
        hit_max_pages = False
        last_error: Optional[str] = None
        pages_fetched = 0

        while True:
            if max_offset and offset >= max_offset:
                hit_max_pages = True
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, f"hit_max_offset={max_offset}")
                break
            params = {
                "user": user,
                "limit": page_size,
                "offset": offset,
                "sortBy": sort_by,
                "sortDirection": sort_dir,
            }
            resp, error_msg = _request_with_backoff(url, params=params, session=self.session)
            if resp is None:
                error_msg = error_msg or "request_failed"
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, error_msg)
                break

            try:
                payload = resp.json()
            except Exception:
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_json")
                break

            raw_positions = []
            if isinstance(payload, list):
                raw_positions = payload
            elif isinstance(payload, dict):
                raw_positions = payload.get("data") or payload.get("positions") or []
            if not isinstance(raw_positions, list):
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_payload")
                break

            reached_earliest = False
            for item in raw_positions:
                position = ClosedPosition.from_api(item, user=user)
                if position is None:
                    continue
                ts = position.timestamp.timestamp()
                if end_ts is not None and ts > end_ts:
                    continue
                if start_ts is not None and ts < start_ts:
                    reached_earliest = True
                    continue
                results.append(position)

            pages_fetched += 1
            if reached_earliest or len(raw_positions) < page_size:
                break

            offset += len(raw_positions)
            page += 1
            if max_pages is not None and page >= max_pages:
                hit_max_pages = True
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, f"hit_max_pages={max_pages}")
                print(
                    f"[WARN] closed-positions 分页被截断：user={user} hit max_pages={max_pages}",
                    flush=True,
                )
                break

        results.sort(key=lambda p: p.timestamp)
        info = {
            "ok": ok,
            "incomplete": incomplete,
            "error_msg": last_error,
            "hit_max_pages": hit_max_pages,
            "pages_fetched": pages_fetched,
        }

        return (results, info) if return_info else results

    def fetch_closed_positions_window(
        self,
        user: str,
        *,
        start_time: Optional[dt.datetime] = None,
        end_time: Optional[dt.datetime] = None,
        page_size: int = 50,
        max_pages: Optional[int] = None,
        sort_by: str = "TIMESTAMP",
        sort_dir: str = "DESC",
        return_info: bool = False,
    ) -> List[ClosedPosition] | tuple[List[ClosedPosition], Dict[str, object]]:
        url = f"{self.host}/closed-positions"
        start_ts = _to_timestamp(start_time)
        end_ts = _to_timestamp(end_time)
        results: List[ClosedPosition] = []
        ok = True
        incomplete = False
        hit_max_pages = False
        last_error: Optional[str] = None
        pages_fetched = 0
        offset = 0
        page_size = max(1, min(page_size, 50))

        while True:
            params = {
                "user": user,
                "limit": page_size,
                "offset": offset,
                "sortBy": sort_by,
                "sortDirection": sort_dir,
            }

            resp, error_msg = _request_with_backoff(url, params=params, session=self.session)
            if resp is None:
                error_msg = error_msg or "request_failed"
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, error_msg)
                break

            try:
                payload = resp.json()
            except Exception:
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_json")
                break

            raw_positions = []
            if isinstance(payload, list):
                raw_positions = payload
            elif isinstance(payload, dict):
                raw_positions = payload.get("data") or payload.get("positions") or []
            if not isinstance(raw_positions, list):
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, "invalid_payload")
                break

            if not raw_positions:
                break

            reached_earliest = False
            min_ts: Optional[dt.datetime] = None
            for item in raw_positions:
                position = ClosedPosition.from_api(item, user=user)
                if position is None:
                    continue
                ts = position.timestamp
                if end_ts is not None and ts.timestamp() > end_ts:
                    continue
                if start_ts is not None and ts.timestamp() < start_ts:
                    reached_earliest = True
                    min_ts = ts if min_ts is None else min(min_ts, ts)
                    continue
                results.append(position)
                min_ts = ts if min_ts is None else min(min_ts, ts)

            pages_fetched += 1
            if reached_earliest or len(raw_positions) < page_size or min_ts is None:
                break
            offset += len(raw_positions)
            if max_pages is not None and pages_fetched >= max_pages:
                hit_max_pages = True
                incomplete = True
                ok = False
                last_error = _combine_error(last_error, f"hit_max_pages={max_pages}")
                print(
                    f"[WARN] closed-positions 分页被截断：user={user} hit max_pages={max_pages}",
                    flush=True,
                )
                break

            _sleep_with_jitter(BASE_PAGE_SLEEP)

        results.sort(key=lambda p: p.timestamp)
        info = {
            "ok": ok,
            "incomplete": incomplete,
            "error_msg": last_error,
            "hit_max_pages": hit_max_pages,
            "pages_fetched": pages_fetched,
        }

        return (results, info) if return_info else results


def _combine_error(existing: Optional[str], new_msg: Optional[str]) -> Optional[str]:
    if not new_msg:
        return existing
    if not existing:
        return new_msg
    if new_msg in existing:
        return existing
    return f"{existing}; {new_msg}"
