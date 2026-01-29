from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import requests

GAMMA_ROOT = "https://gamma-api.polymarket.com"
_GAMMA_RATE_LIMIT_SEC = 1.0
_last_gamma_request_ts = 0.0


def _enforce_gamma_rate_limit() -> None:
    global _last_gamma_request_ts
    now = time.monotonic()
    elapsed = now - _last_gamma_request_ts
    remaining = _GAMMA_RATE_LIMIT_SEC - elapsed
    if remaining > 0:
        time.sleep(remaining)
    _last_gamma_request_ts = time.monotonic()


def _http_json(url: str, params: Optional[dict] = None) -> Optional[Any]:
    try:
        _enforce_gamma_rate_limit()
        resp = requests.get(url, params=params or {}, timeout=10)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def _extract_token_id_from_raw(raw: Dict[str, Any]) -> Optional[str]:
    id_keys = (
        "tokenId",
        "token_id",
        "clobTokenId",
        "clob_token_id",
        "assetId",
        "asset_id",
        "outcomeTokenId",
        "outcome_token_id",
        "id",
    )
    for key in id_keys:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _extract_token_ids_from_market(market: Dict[str, Any]) -> list[str]:
    ids = market.get("clobTokenIds") or market.get("clobTokens")
    if isinstance(ids, str):
        try:
            ids = json.loads(ids)
        except Exception:
            ids = None
    if isinstance(ids, (list, tuple)):
        return [str(x) for x in ids if str(x).strip()]

    outcomes = market.get("outcomes") or market.get("tokens") or []
    if isinstance(outcomes, list):
        ordered: list[tuple[int, str]] = []
        for item in outcomes:
            if not isinstance(item, dict):
                continue
            token_id = _extract_token_id_from_raw(item)
            if not token_id:
                continue
            idx = item.get("outcomeIndex") or item.get("outcome_index")
            if isinstance(idx, (int, float)):
                ordered.append((int(idx), token_id))
        if ordered:
            return [token_id for _, token_id in sorted(ordered, key=lambda t: t[0])]
    return []


def _gamma_fetch_by_slug(slug: str) -> Optional[dict]:
    if not slug:
        return None
    return _http_json(f"{GAMMA_ROOT}/markets/slug/{slug}")


def _gamma_fetch_by_condition(condition_id: str) -> Optional[dict]:
    if not condition_id:
        return None
    data = _http_json(f"{GAMMA_ROOT}/markets", params={"conditionId": condition_id, "limit": 1})
    markets = []
    if isinstance(data, dict) and "data" in data:
        markets = data.get("data") or []
    elif isinstance(data, list):
        markets = data
    if isinstance(markets, list) and markets:
        return markets[0]
    data = _http_json(f"{GAMMA_ROOT}/markets", params={"search": condition_id, "limit": 1})
    markets = []
    if isinstance(data, dict) and "data" in data:
        markets = data.get("data") or []
    elif isinstance(data, list):
        markets = data
    if isinstance(markets, list) and markets:
        return markets[0]
    return None


def gamma_fetch_markets_by_clob_token_ids(token_ids: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not token_ids:
        return out

    chunk_size = 50
    for idx in range(0, len(token_ids), chunk_size):
        chunk = token_ids[idx : idx + chunk_size]
        data = _http_json(
            f"{GAMMA_ROOT}/markets",
            params={"clob_token_ids": chunk, "limit": len(chunk)},
        )
        markets = []
        if isinstance(data, dict) and "data" in data:
            markets = data.get("data") or []
        elif isinstance(data, list):
            markets = data

        if not isinstance(markets, list):
            continue

        for market in markets:
            if not isinstance(market, dict):
                continue
            token_ids_in_market = _extract_token_ids_from_market(market) or []
            for token_id in token_ids_in_market:
                if token_id in chunk and token_id not in out:
                    out[token_id] = market
    return out


def market_is_tradeable(market: dict) -> bool:
    if not isinstance(market, dict):
        return False
    if market.get("archived") is True:
        return False
    if market.get("acceptingOrders") is False:
        return False
    if market.get("closed") is True:
        return False
    if market.get("active") is False:
        return False
    return True


def market_tradeable_state(market: Optional[Dict[str, Any]]) -> Optional[bool]:
    if not isinstance(market, dict):
        return None

    if market.get("enableOrderBook") is False:
        return False
    if market.get("archived") is True:
        return False
    if market.get("acceptingOrders") is False:
        return False
    if market.get("closed") is True:
        return False
    if market.get("active") is False:
        return False

    if (
        market.get("enableOrderBook") is None
        and market.get("acceptingOrders") is None
        and market.get("closed") is None
        and market.get("active") is None
    ):
        return None

    return True


def resolve_token_id(token_key: str, pos: Dict[str, Any], cache: Dict[str, str]) -> str:
    if token_key in cache:
        return cache[token_key]

    raw = pos.get("raw") or {}
    if isinstance(raw, dict):
        token_id = _extract_token_id_from_raw(raw)
        if token_id:
            cache[token_key] = token_id
            return token_id

    outcome_index = pos.get("outcome_index")
    if outcome_index is None:
        raise ValueError(f"token_key={token_key} 缺少 outcome_index")

    slug = pos.get("slug")
    market = _gamma_fetch_by_slug(str(slug)) if slug else None
    if market is None:
        market = _gamma_fetch_by_condition(str(pos.get("condition_id") or ""))
    if not isinstance(market, dict):
        raise ValueError(f"无法解析 token_id: {token_key}")

    token_ids = _extract_token_ids_from_market(market)
    if not token_ids:
        raise ValueError(f"市场未包含 token_id: {token_key}")

    idx = int(outcome_index)
    if idx < 0 or idx >= len(token_ids):
        raise ValueError(f"outcome_index 超出范围: {token_key}")

    token_id = token_ids[idx]
    cache[token_key] = token_id
    return token_id
