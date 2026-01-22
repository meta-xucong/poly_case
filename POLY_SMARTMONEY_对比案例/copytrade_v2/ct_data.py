from __future__ import annotations

import datetime as dt
import random
import time
from typing import Dict, List, Tuple, Optional

import requests

from smartmoney_query.poly_martmoney_query.api_client import DataApiClient
from smartmoney_query.poly_martmoney_query.models import Position, Trade


def _extract_token_id_from_raw(raw: Dict[str, object] | None) -> Optional[str]:
    if not isinstance(raw, dict):
        return None
    for key in (
        "tokenId",
        "token_id",
        "clobTokenId",
        "clob_token_id",
        "asset",  # CRITICAL FIX: Add support for 'asset' field from Polymarket Data API
        "assetId",
        "asset_id",
        "outcomeTokenId",
        "outcome_token_id",
    ):
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    value = raw.get("id")
    if value is not None:
        text = str(value).strip()
        if text:
            return text
    return None


def _normalize_position(pos: Position) -> Dict[str, object] | None:
    raw = pos.raw
    if pos.outcome_index is None:
        raw_norm = _normalize_position_raw(raw) if isinstance(raw, dict) else None
        if raw_norm is not None:
            return raw_norm
        token_id = _extract_token_id_from_raw(raw)
        if token_id:
            return {
                "token_key": "",
                "token_id": token_id,
                "condition_id": pos.condition_id,
                "outcome_index": None,
                "size": float(pos.size),
                "avg_price": float(pos.avg_price),
                "cur_price": float(getattr(pos, "cur_price", 0.0) or 0.0),
                "slug": pos.slug,
                "title": pos.title,
                "end_date": pos.end_date.isoformat() if pos.end_date is not None else None,
                "raw": raw,
            }
        return None
    token_key = f"{pos.condition_id}:{pos.outcome_index}"
    end_date = pos.end_date.isoformat() if pos.end_date is not None else None
    return {
        "token_key": token_key,
        "token_id": _extract_token_id_from_raw(raw),
        "condition_id": pos.condition_id,
        "outcome_index": int(pos.outcome_index),
        "size": float(pos.size),
        "avg_price": float(pos.avg_price),
        "cur_price": float(getattr(pos, "cur_price", 0.0) or 0.0),
        "slug": pos.slug,
        "title": pos.title,
        "end_date": end_date,
        "raw": raw,
    }


def _normalize_position_raw(raw: Dict[str, object]) -> Dict[str, object] | None:
    condition_id = raw.get("conditionId") or raw.get("condition_id") or raw.get("marketId")
    outcome_index = raw.get("outcomeIndex") or raw.get("outcome_index")

    if condition_id is None or outcome_index is None:
        return None
    try:
        idx = int(outcome_index)
    except Exception:
        return None

    size = raw.get("size") or raw.get("shares") or raw.get("positionSize")
    avg_price = raw.get("avgPrice") or raw.get("avg_price") or raw.get("averagePrice") or 0.0
    cur_price = (
        raw.get("curPrice")
        or raw.get("cur_price")
        or raw.get("currentPrice")
        or raw.get("price")
        or 0.0
    )
    try:
        size_f = float(size or 0.0)
        avg_f = float(avg_price or 0.0)
        cur_f = float(cur_price or 0.0)
    except Exception:
        return None

    slug = raw.get("slug") or raw.get("marketSlug")
    title = raw.get("title") or raw.get("question") or raw.get("marketTitle")
    end_date = raw.get("endDate") or raw.get("end_date")

    token_key = f"{condition_id}:{idx}"
    token_id = _extract_token_id_from_raw(raw)
    return {
        "token_key": token_key,
        "token_id": token_id,
        "condition_id": str(condition_id),
        "outcome_index": idx,
        "size": size_f,
        "avg_price": avg_f,
        "cur_price": cur_f,
        "slug": slug,
        "title": title,
        "end_date": end_date,
        "raw": raw,
    }


def _make_cb(refresh_sec: int | None, mode: str) -> int:
    mode = (mode or "bucket").lower()
    if mode == "bucket":
        if refresh_sec and refresh_sec > 0:
            return int(time.time() // int(refresh_sec))
        return int(time.time())
    if mode == "sec":
        return int(time.time())
    if mode == "ms":
        return int(time.time() * 1000)
    if mode == "nonce":
        return random.randint(1, 2_147_483_647)
    return int(time.time())


def _pick_headers(h: dict | None, keys: list[str]) -> dict:
    if not h:
        return {}
    out = {}
    for k in keys:
        v = h.get(k) or h.get(k.lower())
        if v is not None:
            out[k] = str(v)
    return out


def _cache_hit_hint(h: dict) -> str | None:
    if not h:
        return None
    age = h.get("Age") or h.get("age")
    cf = (h.get("CF-Cache-Status") or h.get("cf-cache-status") or "").upper()
    xcache = (h.get("X-Cache") or h.get("x-cache") or "").upper()
    try:
        if age is not None and float(age) > 0:
            return f"age={age}"
    except Exception:
        pass
    if "HIT" in cf:
        return f"cf={cf}"
    if "HIT" in xcache:
        return f"xcache={xcache}"
    return None


def _fetch_positions_norm_http(
    client: DataApiClient,
    user: str,
    size_threshold: float,
    *,
    positions_limit: int,
    positions_max_pages: int,
    refresh_sec: int | None,
    cache_bust_mode: str = "bucket",
    header_keys: list[str] | None = None,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    host = getattr(client, "host", "https://data-api.polymarket.com").rstrip("/")
    url = f"{host}/positions"

    session = requests.Session()

    page_size = max(1, min(int(positions_limit), 500))
    max_pages = max(1, int(positions_max_pages))
    header_keys = header_keys or [
        "Age",
        "CF-Cache-Status",
        "X-Cache",
        "Via",
        "Cache-Control",
        "ETag",
        "Last-Modified",
        "Date",
        "Server",
    ]

    cb = _make_cb(refresh_sec, cache_bust_mode)

    ok = True
    incomplete = False
    last_error = None
    pages = 0
    offset = 0
    out: List[Dict[str, object]] = []
    first_headers = None
    last_headers = None
    last_status = None
    last_url = None
    cache_disable_ctx = None
    try:
        import requests_cache  # noqa: F401

        cache_disable_ctx = getattr(session, "cache_disabled", None)
    except Exception:
        cache_disable_ctx = None

    while pages < max_pages:
        params = {
            "user": user,
            "limit": page_size,
            "offset": offset,
            "sizeThreshold": size_threshold,
            "_cb": cb,
        }
        headers = {
            "Cache-Control": "no-cache, no-store, max-age=0, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        }

        payload = None
        resp = None
        last_exc = None
        retry_delays = [0.5, 1.0, 2.0]
        max_attempts = len(retry_delays) + 1
        for attempt in range(max_attempts):
            retryable = False
            try:
                if callable(cache_disable_ctx):
                    with cache_disable_ctx():
                        resp = session.get(url, params=params, headers=headers, timeout=15.0)
                else:
                    resp = session.get(url, params=params, headers=headers, timeout=15.0)
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    retryable = True
                    raise requests.HTTPError(
                        f"retryable_status_{resp.status_code}",
                        response=resp,
                    )
                resp.raise_for_status()
                payload = resp.json()
                last_exc = None
                break
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = exc
                retryable = True
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code == 429 or (status_code and 500 <= status_code < 600):
                    last_exc = exc
                    retryable = True
                else:
                    last_exc = exc
            except Exception as exc:
                last_exc = exc
            if last_exc and retryable and attempt < len(retry_delays):
                time.sleep(retry_delays[attempt])
                continue
            break

        if last_exc is not None or payload is None:
            if resp is not None:
                try:
                    last_status = resp.status_code
                    last_url = resp.url
                except Exception:
                    pass
            ok = False
            incomplete = True
            last_error = str(last_exc)
            break

        if pages == 0:
            first_headers = dict(resp.headers)
        last_headers = dict(resp.headers)
        last_status = resp.status_code
        last_url = resp.url

        items = None
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            items = payload.get("data") or payload.get("positions") or payload.get("results") or []
        if not isinstance(items, list):
            ok = False
            incomplete = True
            last_error = "invalid_positions_payload"
            break

        if not items:
            break

        for raw in items:
            if not isinstance(raw, dict):
                continue
            norm = _normalize_position_raw(raw)
            if norm:
                out.append(norm)

        pages += 1
        if len(items) < page_size:
            break
        offset += len(items)

    info = {
        "ok": ok,
        "incomplete": incomplete,
        "error_msg": last_error,
        "pages_fetched": pages,
        "limit": page_size,
        "max_pages": max_pages,
        "total": len(out),
        "source": "http_positions",
        "cache_bucket": cb,
        "refresh_sec": refresh_sec,
        "cache_bust_mode": cache_bust_mode,
        "http_status": last_status,
        "request_url": last_url,
        "cache_headers_first": _pick_headers(first_headers, header_keys),
        "cache_headers_last": _pick_headers(last_headers, header_keys),
    }
    info["cache_hit_hint"] = _cache_hit_hint(info["cache_headers_first"]) or _cache_hit_hint(
        info["cache_headers_last"]
    )
    return out, info


def fetch_positions_norm(
    client: DataApiClient,
    user: str,
    size_threshold: float,
    positions_limit: int = 500,
    positions_max_pages: int = 20,
    *,
    refresh_sec: int | None = None,
    force_http: bool = False,
    cache_bust_mode: str = "bucket",
    header_keys: list[str] | None = None,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    if force_http:
        return _fetch_positions_norm_http(
            client,
            user,
            size_threshold,
            positions_limit=positions_limit,
            positions_max_pages=positions_max_pages,
            refresh_sec=refresh_sec,
            cache_bust_mode=cache_bust_mode,
            header_keys=header_keys,
        )
    positions, info = fetch_positions_all(
        client,
        user,
        size_threshold,
        positions_limit=positions_limit,
        positions_max_pages=positions_max_pages,
    )
    normalized: List[Dict[str, object]] = []
    for pos in positions:
        normalized_pos = _normalize_position(pos)
        if normalized_pos is None:
            continue
        normalized.append(normalized_pos)
    info.setdefault("limit", positions_limit)
    info.setdefault("max_pages", positions_max_pages)
    info.setdefault("total", len(positions))
    return normalized, info


def fetch_positions_all(
    client: DataApiClient,
    user: str,
    size_threshold: float,
    *,
    positions_limit: int = 500,
    positions_max_pages: int = 20,
) -> Tuple[List[Position], Dict[str, object]]:
    positions, info = client.fetch_positions(
        user,
        size_threshold=size_threshold,
        page_size=positions_limit,
        max_pages=positions_max_pages,
        return_info=True,
    )
    info.setdefault("limit", positions_limit)
    info.setdefault("max_pages", positions_max_pages)
    info.setdefault("total", len(positions))
    return positions, info


def _parse_timestamp(value: object) -> dt.datetime | None:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            if value > 1e12:
                value /= 1000.0
            return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
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


def _deep_find_first(
    obj: object,
    keys: tuple[str, ...],
    *,
    max_depth: int = 6,
) -> object | None:
    keyset = set(keys)
    stack: list[tuple[object, int]] = [(obj, 0)]
    seen: set[int] = set()
    while stack:
        cur, depth = stack.pop()
        if depth > max_depth:
            continue
        oid = id(cur)
        if oid in seen:
            continue
        seen.add(oid)
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k in keyset and v is not None:
                    return v
                if isinstance(v, (dict, list)):
                    stack.append((v, depth + 1))
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    stack.append((v, depth + 1))
    return None


def _deep_extract_token_id(obj: object, *, max_depth: int = 6) -> object | None:
    # 不要无脑 deep 搜 "id"（会误抓到用户/订单/市场 id）
    direct_keys = (
        "tokenId",
        "token_id",
        "clobTokenId",
        "clob_token_id",
        "asset",
        "assetId",
        "asset_id",
        "outcomeTokenId",
        "outcome_token_id",
    )
    hit = _deep_find_first(obj, direct_keys, max_depth=max_depth)
    if hit is not None:
        return hit

    # 允许在 asset/token/outcome 这类父节点下取 "id"
    id_parent_ok = {"asset", "token", "outcomeToken", "outcome_token", "clobToken", "clob_token"}
    stack: list[tuple[object, int, str | None]] = [(obj, 0, None)]
    seen: set[int] = set()
    while stack:
        cur, depth, parent = stack.pop()
        if depth > max_depth:
            continue
        oid = id(cur)
        if oid in seen:
            continue
        seen.add(oid)
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k == "id" and parent in id_parent_ok and v is not None:
                    return v
                if isinstance(v, (dict, list)):
                    stack.append((v, depth + 1, k))
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    stack.append((v, depth + 1, parent))
    return None


def _normalize_action(raw: Dict[str, object]) -> Dict[str, object] | None:
    side = str(raw.get("side") or raw.get("action") or raw.get("type") or "").upper()
    event_type = str(
        raw.get("eventType")
        or raw.get("event_type")
        or raw.get("activityType")
        or raw.get("activity_type")
        or raw.get("type")
        or ""
    ).upper()
    if side not in ("BUY", "SELL"):
        return None
    if event_type and event_type not in ("TRADE", "FILL", "BUY", "SELL"):
        return None

    size = raw.get("size") or raw.get("amount") or raw.get("quantity") or raw.get("fillSize")
    try:
        size_val = float(size or 0.0)
    except Exception:
        return None
    if size_val <= 0:
        return None

    token_id = (
        raw.get("tokenId")
        or raw.get("token_id")
        or raw.get("clobTokenId")
        or raw.get("clob_token_id")
        or raw.get("asset")
        or raw.get("assetId")
        or raw.get("asset_id")
        or raw.get("outcomeTokenId")
        or raw.get("outcome_token_id")
    )
    if token_id is None:
        token_id = _deep_extract_token_id(raw)
    token_id_text = str(token_id).strip() if token_id is not None else ""
    condition_id = (
        raw.get("conditionId")
        or raw.get("condition_id")
        or raw.get("marketId")
        or raw.get("market_id")
    )
    if condition_id is None:
        condition_id = _deep_find_first(
            raw,
            ("conditionId", "condition_id", "marketId", "market_id"),
            max_depth=6,
        )
    outcome_index = raw.get("outcomeIndex") or raw.get("outcome_index")
    if outcome_index is None:
        outcome_index = _deep_find_first(raw, ("outcomeIndex", "outcome_index"), max_depth=6)
    token_key = None
    if condition_id is not None and outcome_index is not None:
        try:
            token_key = f"{condition_id}:{int(outcome_index)}"
        except Exception:
            token_key = None

    ts = _parse_timestamp(raw.get("timestamp") or raw.get("time") or raw.get("createdAt"))
    if ts is None:
        return None

    return {
        "token_id": token_id_text or None,
        "token_key": token_key,
        "condition_id": str(condition_id) if condition_id is not None else None,
        "outcome_index": int(outcome_index) if outcome_index is not None else None,
        "side": side,
        "size": size_val,
        "price": raw.get("price") or raw.get("fillPrice") or raw.get("avgPrice"),
        "timestamp": ts,
        "raw": raw,
    }


def _normalize_trade_action(trade: Trade) -> Dict[str, object] | None:
    side = str(trade.side or "").upper()
    if side not in ("BUY", "SELL"):
        return None
    if trade.size <= 0:
        return None

    raw = trade.raw or {}
    token_id = (
        raw.get("tokenId")
        or raw.get("token_id")
        or raw.get("clobTokenId")
        or raw.get("clob_token_id")
        or raw.get("asset")
        or raw.get("assetId")
        or raw.get("asset_id")
        or raw.get("outcomeTokenId")
        or raw.get("outcome_token_id")
    )
    if token_id is None:
        token_id = _deep_extract_token_id(raw)
    token_id_text = str(token_id).strip() if token_id is not None else ""
    condition_id = (
        raw.get("conditionId")
        or raw.get("condition_id")
        or raw.get("marketId")
        or raw.get("market_id")
        or trade.market_id
    )
    if condition_id is None:
        condition_id = _deep_find_first(
            raw,
            ("conditionId", "condition_id", "marketId", "market_id"),
            max_depth=6,
        )
    outcome_index = raw.get("outcomeIndex") or raw.get("outcome_index")
    if outcome_index is None:
        outcome_index = _deep_find_first(raw, ("outcomeIndex", "outcome_index"), max_depth=6)

    token_key = None
    if condition_id is not None and outcome_index is not None:
        try:
            token_key = f"{condition_id}:{int(outcome_index)}"
        except Exception:
            token_key = None

    return {
        "token_id": token_id_text or None,
        "token_key": token_key,
        "condition_id": str(condition_id) if condition_id is not None else None,
        "outcome_index": int(outcome_index) if outcome_index is not None else None,
        "side": side,
        "size": float(trade.size),
        "price": float(trade.price),
        "timestamp": trade.timestamp,
        "raw": raw,
    }


def fetch_target_actions_since(
    client: DataApiClient,
    user: str,
    since_ms: int,
    *,
    page_size: int = 300,
    max_offset: int = 10000,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    start_time = dt.datetime.fromtimestamp(since_ms / 1000.0, tz=dt.timezone.utc)
    end_time = dt.datetime.now(tz=dt.timezone.utc)
    records, info = _fetch_activity_actions(
        client,
        user,
        start_time=start_time,
        end_time=end_time,
        page_size=page_size,
        max_offset=max_offset,
    )

    normalized: List[Dict[str, object]] = []
    latest_ms = 0
    for raw in records:
        action = _normalize_action(raw)
        if action is None:
            continue
        action_ms = int(action["timestamp"].timestamp() * 1000)
        if action_ms <= since_ms:
            continue
        latest_ms = max(latest_ms, action_ms)
        normalized.append(action)

    incomplete = bool(info.get("incomplete"))
    total_records = int(info.get("total") or len(records))
    maxed_offset = bool(info.get("max_offset_reached") or info.get("reached_max_offset"))
    if not incomplete and (maxed_offset or total_records >= max_offset):
        incomplete = True

    info.setdefault("ok", True)
    info.setdefault("limit", page_size)
    info.setdefault("total", total_records)
    info.setdefault("normalized", len(normalized))
    info["latest_ms"] = latest_ms
    info["incomplete"] = incomplete
    return normalized, info


def fetch_target_trades_since(
    client: DataApiClient,
    user: str,
    since_ms: int,
    *,
    page_size: int = 500,
    max_offset: int = 10000,
    taker_only: bool = False,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    start_time = dt.datetime.fromtimestamp(since_ms / 1000.0, tz=dt.timezone.utc)
    max_pages = max(1, int(max_offset // max(1, page_size)))
    trades = client.fetch_trades(
        user,
        start_time=start_time,
        page_size=page_size,
        max_pages=max_pages,
        taker_only=taker_only,
    )

    normalized: List[Dict[str, object]] = []
    latest_ms = 0
    for trade in trades:
        action = _normalize_trade_action(trade)
        if action is None:
            continue
        action_ms = int(action["timestamp"].timestamp() * 1000)
        if action_ms <= since_ms:
            continue
        latest_ms = max(latest_ms, action_ms)
        normalized.append(action)

    info: Dict[str, object] = {
        "ok": True,
        "incomplete": False,
        "limit": page_size,
        "total": len(trades),
        "normalized": len(normalized),
        "latest_ms": latest_ms,
        "source": "trades",
        "taker_only": taker_only,
    }
    return normalized, info


def _fetch_activity_actions(
    client: DataApiClient,
    user: str,
    *,
    start_time: dt.datetime,
    end_time: dt.datetime,
    page_size: int,
    max_offset: int,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    fetcher = getattr(client, "fetch_activity_actions", None)
    if callable(fetcher):
        return fetcher(
            user,
            start_time=start_time,
            end_time=end_time,
            page_size=page_size,
            max_offset=max_offset,
            return_info=True,
        )
    return _fetch_activity_actions_fallback(
        client,
        user,
        start_time=start_time,
        end_time=end_time,
        page_size=page_size,
        max_offset=max_offset,
    )


def _fetch_activity_actions_fallback(
    client: DataApiClient,
    user: str,
    *,
    start_time: dt.datetime,
    end_time: dt.datetime,
    page_size: int,
    max_offset: int,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    host = getattr(client, "host", "https://data-api.polymarket.com").rstrip("/")
    session = getattr(client, "session", None) or requests.Session()
    url = f"{host}/activity"
    page_size = max(1, min(int(page_size), 500))
    max_offset = max(0, min(int(max_offset), 10000))
    start_ts_sec = int(start_time.timestamp())
    end_ts_sec = int(end_time.timestamp())
    ok = True
    incomplete = False
    hit_max_pages = False
    last_error = None
    pages_fetched = 0
    cursor_end_sec = end_ts_sec or int(dt.datetime.now(tz=dt.timezone.utc).timestamp())
    offset = 0
    results: List[Dict[str, object]] = []

    while True:
        params = {
            "user": user,
            "type": "TRADE",
            "limit": page_size,
            "offset": offset,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
            "end": int(cursor_end_sec),
            "start": int(start_ts_sec),
        }
        try:
            resp = session.get(url, params=params, timeout=15.0)
            resp.raise_for_status()
        except requests.RequestException as exc:
            incomplete = True
            ok = False
            last_error = str(exc)
            break

        try:
            payload = resp.json()
        except Exception:
            incomplete = True
            ok = False
            last_error = "invalid_json"
            break

        raw_items = []
        if isinstance(payload, list):
            raw_items = payload
        elif isinstance(payload, dict):
            raw_items = payload.get("data") or payload.get("activity") or []
        if not isinstance(raw_items, list):
            incomplete = True
            ok = False
            last_error = "invalid_payload"
            break
        if not raw_items:
            break

        min_ts_sec = None
        for item in raw_items:
            ts = _parse_timestamp(item.get("timestamp") or item.get("time") or item.get("createdAt"))
            if ts is None:
                continue
            ts_sec = int(ts.timestamp())
            if ts_sec < start_ts_sec or ts_sec > end_ts_sec:
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
                last_error = "missing_timestamps_for_window_shift"
                break
            if min_ts_sec >= cursor_end_sec:
                hit_max_pages = True
                incomplete = True
                ok = False
                last_error = f"hit_max_offset_same_end={max_offset}"
                break
            cursor_end_sec = int(min_ts_sec)
            offset = 0

        if cursor_end_sec < start_ts_sec:
            break

    info = {
        "ok": ok,
        "incomplete": incomplete,
        "error_msg": last_error,
        "hit_max_pages": hit_max_pages,
        "pages_fetched": pages_fetched,
        "cursor_end_sec": cursor_end_sec,
        "total": len(results),
        "limit": page_size,
        "source": "fallback_activity_http",
    }
    return results, info
