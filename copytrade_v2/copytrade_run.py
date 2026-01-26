from __future__ import annotations

import argparse
import json
import logging
import os
import re
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from smartmoney_query.poly_martmoney_query.api_client import DataApiClient

from ct_data import (
    fetch_positions_norm,
    fetch_target_actions_since,
    fetch_target_trades_since,
)
from ct_exec import (
    apply_actions,
    fetch_open_orders_norm,
    get_orderbook,
    reconcile_one,
)
from ct_resolver import (
    gamma_fetch_markets_by_clob_token_ids,
    market_tradeable_state,
    resolve_token_id,
)
from ct_risk import accumulator_check, risk_check
from ct_state import load_state, save_state


DEFAULT_CONFIG_PATH = Path(__file__).with_name("copytrade_config.json")
DEFAULT_STATE_PATH = Path(__file__).with_name("state.json")


def _state_path_for_target(state_path: Path, target_address: str) -> Path:
    """Derive per-target state file path when user didn't explicitly provide one."""
    addr = str(target_address or "").strip().lower()
    if addr.startswith("0x") and len(addr) >= 10:
        fname = f"state_{addr[2:6]}_{addr[-4:]}.json"
    else:
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", addr)[:32] or "unknown"
        fname = f"state_{safe}.json"
    return state_path.with_name(fname)


def _load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("配置文件必须为 JSON dict")
    return payload


def _normalize_privkey(key: str) -> str:
    return key[2:] if key.startswith(("0x", "0X")) else key


_EVM_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def _is_placeholder_addr(value: Optional[str]) -> bool:
    if not value:
        return True
    text = value.strip()
    if text.lower() in ("0x...", "0x…", "0x"):
        return True
    if "..." in text or "…" in text:
        return True
    return False


def _is_pure_reprice(actions: Optional[list[dict]]) -> bool:
    if not actions:
        return False
    places = [action for action in actions if action.get("type") == "place"]
    if len(places) != 1:
        return False
    if not bool(places[0].get("_reprice")):
        return False
    for action in actions:
        action_type = action.get("type")
        if action_type in ("cancel", "place"):
            continue
        return False
    return True


def _is_evm_address(value: Optional[str]) -> bool:
    if not value:
        return False
    return bool(_EVM_ADDR_RE.match(value.strip()))


def _get_env_first(keys: list[str]) -> Optional[str]:
    for key in keys:
        env_value = os.getenv(key)
        if env_value and env_value.strip():
            return env_value.strip()
    return None


def _shorten_address(address: str) -> str:
    text = address.strip()
    if len(text) <= 12:
        return text
    return f"{text[:6]}..{text[-4:]}"


def _setup_logging(
    cfg: Dict[str, Any],
    target_address: str,
    base_dir: Path,
) -> logging.Logger:
    log_dir_value = cfg.get("log_dir") or "logs"
    log_dir = Path(log_dir_value)
    if not log_dir.is_absolute():
        log_dir = base_dir / log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    pid = os.getpid()
    short = _shorten_address(target_address)
    log_path = log_dir / f"copytrade_{short}_{timestamp}_pid{pid}.log"

    level_name = str(cfg.get("log_level") or "INFO").upper()
    level = logging.INFO
    if level_name in logging._nameToLevel:
        level = logging._nameToLevel[level_name]

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    root_logger.addHandler(stream_handler)
    root_logger.addHandler(file_handler)

    logger = logging.getLogger(__name__)
    logger.info("日志初始化完成: %s", log_path)
    return logger


def _resolve_addr(name: str, current: Optional[str], env_keys: list[str]) -> str:
    if _is_placeholder_addr(current):
        current = _get_env_first(env_keys)

    if not _is_evm_address(current):
        raise ValueError(
            f"{name} 未配置或格式不合法：{current!r}。需要 0x + 40 位十六进制地址。"
            f" 你可以在 copytrade_config.json 里填 {name}，或设置环境变量：{env_keys}"
        )
    return current.strip()


def _derive_api_creds(client):
    for name in ("derive_api_creds", "derive_api_key"):
        method = getattr(client, name, None)
        if callable(method):
            return method()
    return None


def init_clob_client():
    from py_clob_client.client import ClobClient

    host = os.getenv("POLY_HOST", "https://clob.polymarket.com")
    chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
    signature_type = int(os.getenv("POLY_SIGNATURE", "2"))
    key = _normalize_privkey(os.environ["POLY_KEY"])
    funder = os.environ["POLY_FUNDER"]

    client = ClobClient(
        host,
        key=key,
        chain_id=chain_id,
        signature_type=signature_type,
        funder=funder,
    )
    api_creds = _derive_api_creds(client)
    if not api_creds:
        api_creds = client.create_or_derive_api_creds()
    client.set_api_creds(api_creds)
    try:
        setattr(client, "api_creds", api_creds)
    except Exception:
        pass
    return client


def _mid_price(orderbook: Dict[str, Optional[float]]) -> Optional[float]:
    bid = orderbook.get("best_bid")
    ask = orderbook.get("best_ask")
    if bid is not None and bid <= 0:
        bid = None
    if ask is not None and ask <= 0:
        ask = None
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    if bid is not None:
        return bid
    if ask is not None:
        return ask
    return None


def _parse_market_end_ts(meta: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(meta, dict):
        return None
    value = (
        meta.get("end_time")
        or meta.get("endTime")
        or meta.get("end_date")
        or meta.get("endDate")
        or meta.get("endDateIso")
    )
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            num = float(value)
            if num > 1e12:
                num /= 1000.0
            return int(num)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
                parsed = datetime.strptime(text, "%Y-%m-%d").replace(
                    hour=23,
                    minute=59,
                    second=59,
                    tzinfo=ZoneInfo("America/New_York"),
                )
                return int(parsed.timestamp())
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("America/New_York"))
            return int(parsed.timestamp())
    except Exception:
        return None
    return None


def _is_closed_by_end_date(pos: Dict[str, Any], now_ts: int) -> tuple[bool, Optional[int]]:
    end_ts = _parse_market_end_ts(pos)
    if end_ts is None:
        return False, None
    return end_ts <= now_ts, end_ts


def _filter_closed_positions(
    positions: list[Dict[str, Any]],
    closed_keys: Dict[str, int],
) -> tuple[list[Dict[str, Any]], int]:
    if not positions or not closed_keys:
        return positions, 0
    kept: list[Dict[str, Any]] = []
    removed = 0
    for pos in positions:
        token_key = pos.get("token_key")
        if token_key and token_key in closed_keys:
            removed += 1
            continue
        kept.append(pos)
    return kept, removed


def _extract_mid_cache_meta(state: Dict[str, Any]) -> tuple[Optional[float], Dict[str, Any]]:
    meta_keys = (
        "mid_cache_ttl_sec",
        "mid_cache_ttl",
        "mid_cache_update_ts",
        "mid_cache_update_ms",
        "mid_cache_updated_at",
        "last_mid_price_update_ts",
        "last_mid_price_update_ms",
    )
    meta: Dict[str, Any] = {}
    for key in meta_keys:
        if key in state:
            meta[key] = state.get(key)

    ts_by_token = state.get("last_mid_price_ts_by_token_id")
    if isinstance(ts_by_token, dict):
        ts_values = [
            ts for ts in ts_by_token.values() if isinstance(ts, (int, float)) and ts > 0
        ]
        if ts_values:
            meta["last_mid_price_ts_max"] = max(ts_values)

    last_mid_update = None
    if "last_mid_price_ts_max" in meta:
        last_mid_update = meta["last_mid_price_ts_max"]
    else:
        for key in (
            "last_mid_price_update_ts",
            "last_mid_price_update_ms",
            "mid_cache_update_ts",
            "mid_cache_update_ms",
            "mid_cache_updated_at",
        ):
            value = meta.get(key)
            if isinstance(value, (int, float)) and value > 0:
                last_mid_update = value
                break
    return last_mid_update, meta


def _is_lowp_token(cfg: Dict[str, Any], ref_price: float) -> bool:
    if not bool(cfg.get("lowp_guard_enabled", False)):
        return False
    thr = float(cfg.get("lowp_price_threshold") or 0.0)
    return ref_price > 0 and thr > 0 and ref_price <= thr


def _lowp_cfg(cfg: Dict[str, Any], is_lowp: bool) -> Dict[str, Any]:
    if not is_lowp:
        return cfg
    out = dict(cfg)
    mapping = {
        "min_order_usd": "lowp_min_order_usd",
        "max_order_usd": "lowp_max_order_usd",
        "probe_order_usd": "lowp_probe_order_usd",
        "max_notional_per_token": "lowp_max_notional_per_token",
    }
    for base_key, lowp_key in mapping.items():
        if lowp_key in cfg and cfg.get(lowp_key) is not None:
            out[base_key] = cfg.get(lowp_key)
    return out


def _lowp_buy_ratio(cfg: Dict[str, Any], is_lowp: bool) -> float:
    base = float(cfg.get("follow_ratio") or 0.0)
    if not is_lowp:
        return base
    mult = float(cfg.get("lowp_follow_ratio_mult") or 1.0)
    return base * mult


def _calc_used_notional_totals(
    my_by_token_id: Dict[str, float],
    open_orders_by_token_id: Dict[str, list[dict]],
    mid_cache: Dict[str, float],
    max_position_usd_per_token: float,
    fallback_mid_price: float,
) -> tuple[float, Dict[str, float], Dict[str, Dict[str, object]]]:
    total = 0.0
    by_token: Dict[str, float] = {}
    order_info_by_id: Dict[str, Dict[str, object]] = {}

    for token_id, shares in my_by_token_id.items():
        mid = float(mid_cache.get(token_id, 0.0))
        if mid <= 0:
            # 拿不到价格/无盘口：使用 fallback_mid_price 兜底，避免持仓估值被清零
            mid = 0.0
            if fallback_mid_price > 0 and abs(shares) > 0:
                mid = fallback_mid_price
        if mid < 0:
            mid = 0.0
        elif mid > 1.0:
            mid = 1.0
        usd = abs(shares) * mid
        by_token[token_id] = by_token.get(token_id, 0.0) + usd
        total += usd

    for token_id, orders in open_orders_by_token_id.items():
        for order in orders or []:
            side = str(order.get("side") or "").upper()
            if side != "BUY":
                continue
            size = float(order.get("size") or 0.0)
            price = float(order.get("price") or 0.0)
            if price <= 0 or size <= 0:
                continue
            usd = abs(size) * price
            by_token[token_id] = by_token.get(token_id, 0.0) + usd
            total += usd
            order_id = str(order.get("order_id") or "")
            if order_id:
                order_info_by_id[order_id] = {
                    "token_id": token_id,
                    "side": "BUY",
                    "usd": usd,
                }

    return total, by_token, order_info_by_id


def _calc_shadow_buy_notional(
    state: Dict[str, Any],
    now_ts: int,
    ttl_sec: int,
) -> tuple[float, Dict[str, float]]:
    if ttl_sec <= 0:
        state["shadow_buy_orders"] = []
        return 0.0, {}
    taker_orders = state.get("taker_buy_orders")
    shadow_orders = state.get("shadow_buy_orders")
    if isinstance(taker_orders, list) and taker_orders:
        orders_key = "taker_buy_orders"
        shadow_orders = taker_orders
    elif isinstance(shadow_orders, list) and shadow_orders:
        orders_key = "shadow_buy_orders"
    else:
        orders_key = (
            "taker_buy_orders" if isinstance(taker_orders, list) else "shadow_buy_orders"
        )
        shadow_orders = taker_orders if isinstance(taker_orders, list) else shadow_orders
    if not isinstance(shadow_orders, list):
        state[orders_key] = []
        return 0.0, {}
    kept: list[dict] = []
    total = 0.0
    by_token: Dict[str, float] = {}
    for order in shadow_orders:
        if not isinstance(order, dict):
            continue
        token_id = str(order.get("token_id") or "")
        if not token_id:
            continue
        ts = int(order.get("ts") or 0)
        if ts <= 0 or (now_ts - ts) > ttl_sec:
            continue
        usd = float(order.get("usd") or 0.0)
        if usd <= 0:
            continue
        kept.append(order)
        total += usd
        by_token[token_id] = by_token.get(token_id, 0.0) + usd
    state[orders_key] = kept
    if orders_key == "shadow_buy_orders":
        state["taker_buy_orders"] = list(kept)
    return total, by_token


def _calc_recent_buy_notional(
    state: Dict[str, Any],
    now_ts: int,
    window_sec: int,
) -> tuple[float, Dict[str, float]]:
    if window_sec <= 0:
        state["recent_buy_orders"] = []
        return 0.0, {}
    recent_orders = state.get("recent_buy_orders")
    if not isinstance(recent_orders, list):
        state["recent_buy_orders"] = []
        return 0.0, {}
    kept: list[dict] = []
    total = 0.0
    by_token: Dict[str, float] = {}
    for order in recent_orders:
        if not isinstance(order, dict):
            continue
        token_id = str(order.get("token_id") or "")
        if not token_id:
            continue
        ts = int(order.get("ts") or 0)
        if ts <= 0 or (now_ts - ts) > window_sec:
            continue
        usd = float(order.get("usd") or 0.0)
        if usd <= 0:
            continue
        kept.append(order)
        total += usd
        by_token[token_id] = by_token.get(token_id, 0.0) + usd
    state["recent_buy_orders"] = kept
    return total, by_token


def _calc_planned_notional_totals(
    my_by_token_id: Dict[str, float],
    open_orders_by_token_id: Dict[str, list[dict]],
    mid_cache: Dict[str, float],
    max_position_usd_per_token: float,
    state: Dict[str, Any],
    now_ts: int,
    shadow_ttl_sec: int,
    fallback_mid_price: float,
    include_shadow: bool = True,
) -> tuple[float, Dict[str, float], Dict[str, Dict[str, object]], float]:
    total, by_token, order_info_by_id = _calc_used_notional_totals(
        my_by_token_id,
        open_orders_by_token_id,
        mid_cache,
        max_position_usd_per_token,
        fallback_mid_price,
    )
    shadow_total, shadow_by_token = _calc_shadow_buy_notional(
        state, now_ts, shadow_ttl_sec
    )
    if include_shadow and shadow_total > 0:
        total += shadow_total
        for token_id, usd in shadow_by_token.items():
            by_token[token_id] = by_token.get(token_id, 0.0) + usd
    return total, by_token, order_info_by_id, shadow_total


def _calc_used_notional_total(
    my_by_token_id: Dict[str, float],
    open_orders_by_token_id: Dict[str, list[dict]],
    mid_cache: Dict[str, float],
    max_position_usd_per_token: float,
    fallback_mid_price: float,
) -> float:
    total, _, _ = _calc_used_notional_totals(
        my_by_token_id,
        open_orders_by_token_id,
        mid_cache,
        max_position_usd_per_token,
        fallback_mid_price,
    )
    return total


def _calc_planned_notional_with_fallback(
    my_by_token_id: Dict[str, float],
    open_orders_by_token_id: Dict[str, list[dict]],
    mid_cache: Dict[str, float],
    max_position_usd_per_token: float,
    state: Dict[str, Any],
    now_ts: int,
    shadow_ttl_sec: int,
    fallback_mid_price: float,
    logger: logging.Logger,
    include_shadow: bool = True,
) -> tuple[float, Dict[str, float], Dict[str, Dict[str, object]], float]:
    total, by_token, order_info_by_id, shadow_total = _calc_planned_notional_totals(
        my_by_token_id,
        open_orders_by_token_id,
        mid_cache,
        max_position_usd_per_token,
        state,
        now_ts,
        shadow_ttl_sec,
        fallback_mid_price,
        include_shadow=include_shadow,
    )
    if total > 0 or not my_by_token_id or fallback_mid_price <= 0:
        state["planned_zero_streak"] = 0
        return total, by_token, order_info_by_id, shadow_total

    total, by_token, order_info_by_id, shadow_total = _calc_planned_notional_totals(
        my_by_token_id,
        open_orders_by_token_id,
        mid_cache,
        max_position_usd_per_token,
        state,
        now_ts,
        shadow_ttl_sec,
        fallback_mid_price,
        include_shadow=include_shadow,
    )
    planned_zero_streak = int(state.get("planned_zero_streak") or 0) + 1
    state["planned_zero_streak"] = planned_zero_streak
    if planned_zero_streak <= 3 or planned_zero_streak % 20 == 0:
        missing_mid_tokens = [
            token_id
            for token_id in my_by_token_id
            if float(mid_cache.get(token_id, 0.0) or 0.0) <= 0
        ]
        missing_mid_sample = missing_mid_tokens[:5]
        last_mid_update, mid_cache_meta = _extract_mid_cache_meta(state)
        logger.warning(
            "[ALERT] planned_notional_zero fallback_mid=%s positions=%s streak=%s "
            "token_count=%s last_mid_update=%s missing_mid_sample=%s mid_cache_meta=%s",
            fallback_mid_price,
            len(my_by_token_id),
            planned_zero_streak,
            len(mid_cache),
            last_mid_update,
            missing_mid_sample,
            mid_cache_meta,
        )
    return total, by_token, order_info_by_id, shadow_total


def _shrink_on_risk_limit(
    act: Dict[str, Any],
    max_total: float,
    planned_total: float,
    max_per_token: float,
    planned_token: float,
    min_usd: float,
    min_shares: float,
    token_key: str,
    token_id: str,
    logger: logging.Logger,
) -> Optional[tuple[Dict[str, Any], float]]:
    side = str(act.get("side") or "").upper()
    if side != "BUY":
        return None
    price = float(act.get("price") or 0.0)
    size = float(act.get("size") or 0.0)
    if price <= 0 or size <= 0:
        return None

    order_usd = abs(size) * price
    cap_total_remaining = (max_total - planned_total) if max_total > 0 else None
    cap_token_remaining = (max_per_token - planned_token) if max_per_token > 0 else None

    candidates = [order_usd]
    if cap_total_remaining is not None:
        candidates.append(cap_total_remaining)
    if cap_token_remaining is not None:
        candidates.append(cap_token_remaining)

    allowed_usd = min(candidates)
    effective_min_usd = float(min_usd or 0.0)
    if float(min_shares or 0.0) > 0:
        effective_min_usd = max(effective_min_usd, float(min_shares) * price)

    if allowed_usd <= 0 or allowed_usd + 1e-9 < effective_min_usd:
        return None
    if allowed_usd >= order_usd * (1 - 1e-9):
        return None

    new_act = dict(act)
    new_act["size"] = allowed_usd / price
    logger.warning(
        "[RISK_RESIZE] %s token=%s side=%s old_usd=%s new_usd=%s planned_total=%s",
        token_key,
        token_id,
        side,
        order_usd,
        allowed_usd,
        planned_total,
    )
    return new_act, allowed_usd


def _collect_order_ids(open_orders_by_token_id: Dict[str, list[dict]]) -> set[str]:
    order_ids: set[str] = set()
    for orders in open_orders_by_token_id.values():
        for order in orders or []:
            order_id = order.get("order_id")
            if order_id:
                order_ids.add(str(order_id))
    return order_ids


def _refresh_managed_order_ids(state: Dict[str, Any]) -> None:
    managed_ids = _collect_order_ids(state.get("open_orders", {}))
    state["managed_order_ids"] = sorted(managed_ids)


def _intent_key(phase: str, desired_side: str, desired_shares: float) -> Dict[str, Any]:
    return {
        "phase": phase,
        "desired_side": desired_side,
        "desired_shares": float(desired_shares),
    }


def _update_intent_state(
    state: Dict[str, Any],
    token_id: str,
    new_key: Dict[str, Any],
    eps: float,
    logger: logging.Logger,
) -> tuple[bool, bool]:
    intents = state.setdefault("intent_keys", {})
    prev = intents.get(token_id)
    reasons: list[str] = []
    desired_down = False
    if isinstance(prev, dict):
        if prev.get("phase") != new_key.get("phase"):
            reasons.append("phase_changed")
        if prev.get("desired_side") != new_key.get("desired_side"):
            reasons.append("side_changed")
        prev_shares = float(prev.get("desired_shares") or 0.0)
        if float(new_key.get("desired_shares") or 0.0) < prev_shares - eps:
            reasons.append("desired_shares_down")
            desired_down = True
    intents[token_id] = new_key
    if reasons:
        logger.info(
            "[INTENT] token_id=%s old=%s new=%s reasons=%s",
            token_id,
            prev,
            new_key,
            ",".join(reasons),
        )
    return bool(reasons), desired_down


def _action_identity(action: Dict[str, object]) -> str:
    raw = action.get("raw") or {}
    token_id = str(action.get("token_id") or "").strip()
    side = str(action.get("side") or "").strip().upper()
    price = action.get("price")
    size = action.get("size")
    if isinstance(raw, dict):
        tx_hash = raw.get("txHash") or raw.get("tx_hash") or raw.get("transactionHash")
        log_index = raw.get("logIndex") or raw.get("log_index")
        fill_id = raw.get("fillId") or raw.get("fill_id")
        if tx_hash and log_index is not None:
            return f"tx:{tx_hash}:{log_index}"
        if fill_id is not None:
            return f"fill:{fill_id}"
        if tx_hash:
            return f"tx:{tx_hash}:{token_id}:{side}:{price}:{size}"
    token_id = action.get("token_id") or ""
    side = action.get("side") or ""
    size = action.get("size") or ""
    ts = action.get("timestamp")
    action_ms = int(ts.timestamp() * 1000) if ts else 0
    price = ""
    if isinstance(raw, dict):
        price = raw.get("price") or raw.get("fillPrice") or raw.get("avgPrice") or ""
    return f"fallback:{token_id}:{side}:{size}:{price}:{action_ms}"


def _extract_token_id_from_raw(raw: object) -> Optional[str]:
    """从 position/raw/action.raw 中提取 token_id（只读字段，不做网络请求）。支持嵌套结构。"""
    if raw is None:
        return None

    direct_keys = (
        "tokenId",
        "token_id",
        "clobTokenId",
        "clob_token_id",
        "assetId",
        "asset_id",
        "outcomeTokenId",
        "outcome_token_id",
    )

    if isinstance(raw, dict):
        for key in direct_keys:
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

    keyset = set(direct_keys)
    id_parent_ok = {"asset", "token", "outcomeToken", "outcome_token", "clobToken", "clob_token"}
    stack: list[tuple[object, int, Optional[str]]] = [(raw, 0, None)]
    seen: set[int] = set()
    while stack:
        cur, depth, parent = stack.pop()
        if depth > 6:
            continue
        oid = id(cur)
        if oid in seen:
            continue
        seen.add(oid)

        if isinstance(cur, dict):
            for key, value in cur.items():
                if key in keyset and value is not None:
                    text = str(value).strip()
                    if text:
                        return text
                if key == "id" and parent in id_parent_ok and value is not None:
                    text = str(value).strip()
                    if text:
                        return text
                if isinstance(value, (dict, list)):
                    stack.append((value, depth + 1, key))
        elif isinstance(cur, list):
            for value in cur:
                if isinstance(value, (dict, list)):
                    stack.append((value, depth + 1, parent))
    return None


def _prune_order_ts_by_id(state: Dict[str, Any]) -> None:
    order_ts_by_id = state.get("order_ts_by_id")
    if not isinstance(order_ts_by_id, dict):
        state["order_ts_by_id"] = {}
        return
    active_ids = _collect_order_ids(state.get("open_orders", {}))
    for order_id in list(order_ts_by_id.keys()):
        if str(order_id) not in active_ids:
            order_ts_by_id.pop(order_id, None)


def _record_orderbook_empty(
    state: Dict[str, Any],
    token_id: str,
    logger: logging.Logger,
    cfg: Dict[str, Any],
    now_ts: int,
) -> bool:
    streaks = state.setdefault("orderbook_empty_streak", {})
    if not isinstance(streaks, dict):
        streaks = {}
        state["orderbook_empty_streak"] = streaks
    prev = int(streaks.get(token_id) or 0)
    current = prev + 1
    streaks[token_id] = current
    if current <= 3 or current % 10 == 0:
        logger.warning(
            "[ALERT] orderbook_empty token_id=%s streak=%s",
            token_id,
            current,
        )
    close_streak = int(cfg.get("orderbook_empty_close_streak") or 3)
    if close_streak > 0 and current >= close_streak:
        closed_token_keys = state.setdefault("closed_token_keys", {})
        if isinstance(closed_token_keys, dict) and str(token_id) not in closed_token_keys:
            closed_token_keys[str(token_id)] = int(now_ts)
            logger.warning(
                "[CLOSE] orderbook_empty token_id=%s streak=%s",
                token_id,
                current,
            )
            return True
    return False


def _clear_orderbook_empty(state: Dict[str, Any], token_id: str) -> None:
    streaks = state.get("orderbook_empty_streak")
    if isinstance(streaks, dict):
        streaks.pop(token_id, None)


def _maybe_update_target_last(
    state: Dict[str, Any],
    token_id: str,
    t_now: Optional[float],
    should_update: bool,
) -> None:
    if should_update and t_now is not None:
        state.setdefault("target_last_shares", {})[token_id] = float(t_now)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket Copytrade v1")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--target", dest="target_address")
    parser.add_argument("--my", dest="my_address")
    parser.add_argument("--ratio", type=float, dest="follow_ratio")
    parser.add_argument("--poll", type=int, dest="poll_interval_sec")
    parser.add_argument("--poll-exit", type=int, dest="poll_interval_sec_exiting")
    parser.add_argument(
        "--my-positions-force-http",
        action="store_true",
        dest="my_positions_force_http",
        default=None,
        help="Force HTTP direct fetch for my positions (override config).",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    cfg = _load_config(Path(args.config))
    arg_overrides: Dict[str, Any] = {}
    for key in (
        "target_address",
        "my_address",
        "follow_ratio",
        "poll_interval_sec",
        "poll_interval_sec_exiting",
        "my_positions_force_http",
    ):
        arg_val = getattr(args, key, None)
        if arg_val is not None:
            cfg[key] = arg_val
            arg_overrides[key] = arg_val

    cfg["my_address"] = _resolve_addr(
        "my_address",
        cfg.get("my_address"),
        env_keys=[
            "POLY_FUNDER",
            "POLY_MY_ADDRESS",
            "MY_ADDRESS",
        ],
    )
    cfg["target_address"] = _resolve_addr(
        "target_address",
        cfg.get("target_address"),
        env_keys=[
            "COPYTRADE_TARGET",
            "CT_TARGET",
            "POLY_TARGET_ADDRESS",
            "TARGET_ADDRESS",
        ],
    )

    # Per-target state file: if user didn't specify a custom --state, derive one from target address.
    orig_state_path = args.state
    try:
        sp = Path(args.state)
        if sp.name == "state.json":
            args.state = str(_state_path_for_target(sp, cfg["target_address"]))
    except Exception:
        args.state = orig_state_path

    logger = _setup_logging(cfg, cfg["target_address"], Path(args.config).parent)
    if args.state != orig_state_path:
        logger.info("[STATE] per-target state: %s -> %s", orig_state_path, args.state)

    state = load_state(args.state)
    # Safety: if user accidentally reuses a state file across targets, reset bootstrap-related fields.
    prev_target = str(state.get("target") or "").lower().strip()
    cur_target = str(cfg.get("target_address") or "").lower().strip()
    if prev_target and cur_target and prev_target != cur_target:
        logger.warning(
            "[STATE] state target mismatch (state=%s cfg=%s); resetting bootstrap fields",
            prev_target,
            cur_target,
        )
        state["bootstrapped"] = False
        state["boot_token_ids"] = []
        state["boot_token_keys"] = []
        state["target_last_shares_by_token_key"] = {}
        state["target_last_shares"] = {}
        state["target_last_seen_ts"] = {}
        state["target_missing_streak"] = {}
        state["topic_state"] = {}
        state["open_orders"] = {}
        state["open_orders_all"] = []
        state["seen_action_ids"] = []
        state["target_actions_cursor_ms"] = 0
    state.pop("cumulative_buy_usd_total", None)
    state.pop("cumulative_buy_usd_by_token", None)
    run_start_ms = int(time.time() * 1000)
    state["run_start_ms"] = run_start_ms
    logger.info("[STATE] path=%s run_start_ms=%s", args.state, run_start_ms)
    state.setdefault("sizing", {})
    state["sizing"].setdefault("ema_delta_usd", None)
    logger.info(
        "[CFG] target=%s my=%s ratio=%s",
        cfg["target_address"],
        cfg["my_address"],
        cfg.get("follow_ratio"),
    )
    state["target"] = cfg.get("target_address")
    state["my_address"] = cfg.get("my_address")
    state["follow_ratio"] = cfg.get("follow_ratio")
    state.setdefault("open_orders", {})
    state.setdefault("open_orders_all", {})
    state.setdefault("seen_my_trade_ids", [])
    state.setdefault("my_trades_cursor_ms", 0)
    state.setdefault("my_trades_unreliable_until", 0)
    state.setdefault("managed_order_ids", [])
    state.setdefault("intent_keys", {})
    state.setdefault("token_map", {})
    state.setdefault("bootstrapped", False)
    state.setdefault("boot_token_ids", [])
    state.setdefault("boot_token_keys", [])
    state.setdefault("target_last_shares_by_token_key", {})
    state.setdefault("boot_run_start_ms", 0)
    state.setdefault("probed_token_ids", [])
    state.setdefault("ignored_tokens", {})
    state.setdefault("market_status_cache", {})
    state.setdefault("target_last_shares", {})
    state.setdefault("target_last_seen_ts", {})
    state.setdefault("target_missing_streak", {})
    state.setdefault("cooldown_until", {})
    state.setdefault("target_last_event_ts", {})
    state.setdefault("topic_state", {})
    state.setdefault("target_actions_cursor_ms", 0)
    state.setdefault("last_mid_price_by_token_id", {})
    state.setdefault("last_mid_price_update_ts", 0)
    state.setdefault("orderbook_empty_streak", {})
    state.setdefault("order_ts_by_id", {})
    state.setdefault("seen_action_ids", [])
    state.setdefault("last_reprice_ts_by_token", {})
    state.setdefault("adopted_existing_orders", False)
    state.setdefault("place_fail_until", {})
    state.setdefault("missing_data_freeze", {})
    state.setdefault("resolver_fail_cache", {})
    state.setdefault("target_positions_nonce_last_ts", 0)
    state.setdefault("target_positions_nonce_actions", 0)
    if not isinstance(state.get("open_orders"), dict):
        state["open_orders"] = {}
    if not isinstance(state.get("open_orders_all"), dict):
        state["open_orders_all"] = {}
    if not isinstance(state.get("managed_order_ids"), list):
        state["managed_order_ids"] = []
    if not isinstance(state.get("intent_keys"), dict):
        state["intent_keys"] = {}
    if not isinstance(state.get("token_map"), dict):
        state["token_map"] = {}
    if not isinstance(state.get("bootstrapped"), bool):
        state["bootstrapped"] = False
    if not isinstance(state.get("boot_token_ids"), list):
        state["boot_token_ids"] = []
    if not isinstance(state.get("boot_token_keys"), list):
        state["boot_token_keys"] = []
    if not isinstance(state.get("target_last_shares_by_token_key"), dict):
        state["target_last_shares_by_token_key"] = {}
    if not isinstance(state.get("boot_run_start_ms"), (int, float)):
        state["boot_run_start_ms"] = 0
    if not isinstance(state.get("probed_token_ids"), list):
        state["probed_token_ids"] = []
    if not isinstance(state.get("ignored_tokens"), dict):
        state["ignored_tokens"] = {}
    if not isinstance(state.get("market_status_cache"), dict):
        state["market_status_cache"] = {}
    if not isinstance(state.get("target_last_shares"), dict):
        state["target_last_shares"] = {}
    if not isinstance(state.get("target_last_seen_ts"), dict):
        state["target_last_seen_ts"] = {}
    if not isinstance(state.get("target_missing_streak"), dict):
        state["target_missing_streak"] = {}
    if not isinstance(state.get("cooldown_until"), dict):
        state["cooldown_until"] = {}
    if not isinstance(state.get("target_last_event_ts"), dict):
        state["target_last_event_ts"] = {}
    if not isinstance(state.get("topic_state"), dict):
        state["topic_state"] = {}
    if not isinstance(state.get("target_actions_cursor_ms"), (int, float)):
        state["target_actions_cursor_ms"] = 0
    if not isinstance(state.get("last_mid_price_by_token_id"), dict):
        state["last_mid_price_by_token_id"] = {}
    if not isinstance(state.get("last_mid_price_update_ts"), (int, float)):
        state["last_mid_price_update_ts"] = 0
    if not isinstance(state.get("orderbook_empty_streak"), dict):
        state["orderbook_empty_streak"] = {}
    if not isinstance(state.get("order_ts_by_id"), dict):
        state["order_ts_by_id"] = {}
    if not isinstance(state.get("seen_action_ids"), list):
        state["seen_action_ids"] = []
    if not isinstance(state.get("last_reprice_ts_by_token"), dict):
        state["last_reprice_ts_by_token"] = {}
    if not isinstance(state.get("adopted_existing_orders"), bool):
        state["adopted_existing_orders"] = False
    if not isinstance(state.get("place_fail_until"), dict):
        state["place_fail_until"] = {}
    if not isinstance(state.get("target_positions_nonce_last_ts"), (int, float)):
        state["target_positions_nonce_last_ts"] = 0
    if not isinstance(state.get("target_positions_nonce_actions"), (int, float)):
        state["target_positions_nonce_actions"] = 0
    if not isinstance(state.get("missing_data_freeze"), dict):
        state["missing_data_freeze"] = {}
    if not isinstance(state.get("resolver_fail_cache"), dict):
        state["resolver_fail_cache"] = {}
    if not isinstance(state.get("closed_token_keys"), dict):
        state["closed_token_keys"] = {}

    data_client = DataApiClient()
    clob_client = init_clob_client()

    poll_interval = 20
    poll_interval_exiting = 20
    size_threshold = 0.0
    skip_closed = True
    refresh_sec = 300
    positions_limit = 500
    positions_max_pages = 20
    target_positions_refresh_sec = 25
    log_cache_headers = False
    header_keys: list[str] = [
        "Age",
        "CF-Cache-Status",
        "X-Cache",
        "Via",
        "Cache-Control",
    ]
    target_cache_bust_mode = "bucket"
    my_positions_force_http = False
    actions_page_size = 300
    actions_max_offset = 10000
    heartbeat_interval_sec = 600
    config_reload_sec = 600
    max_resolve_target_positions_per_loop = 20
    last_config_reload_ts = time.time()
    last_config_mtime: Optional[float] = None
    resolved_target_address = cfg["target_address"]
    resolved_my_address = cfg["my_address"]

    def _apply_overrides(payload: Dict[str, Any]) -> None:
        for key, value in arg_overrides.items():
            payload[key] = value

    def _apply_cfg_settings() -> None:
        nonlocal poll_interval
        nonlocal poll_interval_exiting
        nonlocal size_threshold
        nonlocal skip_closed
        nonlocal refresh_sec
        nonlocal positions_limit
        nonlocal positions_max_pages
        nonlocal target_positions_refresh_sec
        nonlocal log_cache_headers
        nonlocal header_keys
        nonlocal target_cache_bust_mode
        nonlocal my_positions_force_http
        nonlocal actions_page_size
        nonlocal actions_max_offset
        nonlocal heartbeat_interval_sec
        nonlocal config_reload_sec
        nonlocal max_resolve_target_positions_per_loop
        poll_interval = int(cfg.get("poll_interval_sec") or 20)
        poll_interval_exiting = int(cfg.get("poll_interval_sec_exiting") or poll_interval)
        size_threshold = float(cfg.get("size_threshold") or 0)
        skip_closed = bool(cfg.get("skip_closed_markets", True))
        refresh_sec = int(cfg.get("market_status_refresh_sec") or 300)
        positions_limit = int(cfg.get("positions_limit") or 500)
        positions_max_pages = int(cfg.get("positions_max_pages") or 20)
        target_positions_refresh_sec = int(cfg.get("target_positions_refresh_sec") or 25)
        log_cache_headers = bool(cfg.get("log_positions_cache_headers"))
        header_keys = cfg.get("positions_cache_header_keys") or [
            "Age",
            "CF-Cache-Status",
            "X-Cache",
            "Via",
            "Cache-Control",
        ]
        target_cache_bust_mode = str(cfg.get("target_cache_bust_mode") or "bucket")
        my_positions_force_http = bool(cfg.get("my_positions_force_http", False))
        actions_page_size = int(cfg.get("actions_page_size") or 300)
        actions_max_offset = int(cfg.get("actions_max_offset") or 10000)
        heartbeat_interval_sec = int(cfg.get("heartbeat_interval_sec") or 600)
        config_reload_sec = int(cfg.get("config_reload_sec") or 600)
        max_resolve_target_positions_per_loop = int(
            cfg.get("max_resolve_target_positions_per_loop") or 20
        )

    def _refresh_log_level() -> None:
        level_name = str(cfg.get("log_level") or "INFO").upper()
        level = logging._nameToLevel.get(level_name, logging.INFO)
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        for handler in root_logger.handlers:
            handler.setLevel(level)

    def _reload_config(reason: str) -> None:
        nonlocal cfg, last_config_reload_ts, last_config_mtime
        try:
            new_cfg = _load_config(Path(args.config))
        except Exception as exc:
            logger.warning("[CFG] reload failed (%s): %s", reason, exc)
            last_config_reload_ts = time.time()
            return
        _apply_overrides(new_cfg)
        new_target = new_cfg.get("target_address")
        new_my = new_cfg.get("my_address")
        if new_target and str(new_target).strip() != str(resolved_target_address).strip():
            logger.warning(
                "[CFG] target_address 变更将被忽略，需要重启: %s -> %s",
                resolved_target_address,
                new_target,
            )
            new_cfg["target_address"] = resolved_target_address
        if new_my and str(new_my).strip() != str(resolved_my_address).strip():
            logger.warning(
                "[CFG] my_address 变更将被忽略，需要重启: %s -> %s",
                resolved_my_address,
                new_my,
            )
            new_cfg["my_address"] = resolved_my_address
        cfg = new_cfg
        state["follow_ratio"] = cfg.get("follow_ratio")
        _apply_cfg_settings()
        _refresh_log_level()
        last_config_reload_ts = time.time()
        try:
            last_config_mtime = Path(args.config).stat().st_mtime
        except Exception:
            last_config_mtime = None
        logger.info("[CFG] reloaded (%s)", reason)

    _apply_cfg_settings()
    _refresh_log_level()
    try:
        last_config_mtime = Path(args.config).stat().st_mtime
    except Exception:
        last_config_mtime = None
    last_heartbeat_ts = 0

    if int(state.get("target_actions_cursor_ms") or 0) <= 0:
        state["target_actions_cursor_ms"] = int(state.get("run_start_ms") or time.time() * 1000)
    if int(state.get("target_actions_cursor_ms") or 0) < int(state.get("run_start_ms") or 0):
        state["target_actions_cursor_ms"] = int(state.get("run_start_ms") or 0)
    if int(state.get("my_trades_cursor_ms") or 0) <= 0:
        state["my_trades_cursor_ms"] = int(state.get("run_start_ms") or time.time() * 1000)
    if int(state.get("my_trades_cursor_ms") or 0) < int(state.get("run_start_ms") or 0):
        state["my_trades_cursor_ms"] = int(state.get("run_start_ms") or 0)
    if int(state.get("my_trades_unreliable_until") or 0) < 0:
        state["my_trades_unreliable_until"] = 0

    missing_notice_tokens: set[str] = set()

    def _get_poll_interval() -> int:
        topic_state = state.get("topic_state", {})
        if isinstance(topic_state, dict):
            for st in topic_state.values():
                if (st or {}).get("phase") == "EXITING":
                    return poll_interval_exiting
        return poll_interval

    while True:
        now_ts = int(time.time())
        now_wall = time.time()
        actions_missing_ratio = 0.0
        unresolved_trade_candidates: list[Dict[str, Any]] = []
        resolver_fail_cooldown_sec = int(cfg.get("resolver_fail_cooldown_sec") or 300)
        resolver_fail_cache = state.setdefault("resolver_fail_cache", {})
        if not isinstance(resolver_fail_cache, dict):
            resolver_fail_cache = {}
            state["resolver_fail_cache"] = resolver_fail_cache
        if resolver_fail_cooldown_sec > 0:
            expired_keys = [
                token_key
                for token_key, ts in resolver_fail_cache.items()
                if now_ts - int(ts or 0) >= resolver_fail_cooldown_sec
            ]
            for token_key in expired_keys:
                resolver_fail_cache.pop(token_key, None)
        else:
            resolver_fail_cache.clear()
        if now_wall - last_config_reload_ts >= max(config_reload_sec, 1):
            reason = "interval"
            try:
                mtime = Path(args.config).stat().st_mtime
                if last_config_mtime is None or mtime != last_config_mtime:
                    reason = "mtime"
            except Exception:
                reason = "interval"
            _reload_config(reason)
        managed_ids = {str(order_id) for order_id in (state.get("managed_order_ids") or [])}
        try:
            remote_orders, ok, err = fetch_open_orders_norm(clob_client)
            if ok:
                remote_by_token: Dict[str, list[dict]] = {}
                order_ts_by_id = state.setdefault("order_ts_by_id", {})
                remote_order_ids: set[str] = set()
                for order in remote_orders:
                    order_id = str(order["order_id"])
                    ts = order.get("ts") or order_ts_by_id.get(order_id) or now_ts
                    remote_order_ids.add(order_id)
                    order_payload = {
                        "order_id": order_id,
                        "side": order["side"],
                        "price": order["price"],
                        "size": order["size"],
                        "ts": int(ts),
                    }
                    remote_by_token.setdefault(order["token_id"], []).append(order_payload)
                adopt_existing = bool(cfg.get("adopt_existing_orders_on_boot", False))
                if adopt_existing and not state.get("adopted_existing_orders", False):
                    if len(managed_ids) < 3:
                        adoptable_ids: set[str] = set()
                        for orders in remote_by_token.values():
                            for order in orders:
                                price = float(order.get("price") or 0.0)
                                size = float(order.get("size") or 0.0)
                                if price <= 0 or price > 1.0:
                                    continue
                                if size <= 0:
                                    continue
                                order_id = order.get("order_id")
                                if order_id:
                                    adoptable_ids.add(str(order_id))
                        if adoptable_ids:
                            logger.info(
                                "[BOOT] adopt_existing_orders_on_boot: adopted=%s",
                                len(adoptable_ids),
                            )
                            managed_ids |= adoptable_ids
                    state["adopted_existing_orders"] = True
                # --- begin: ORDSYNC ledger-first (fix eventual consistency) ---
                prev_managed = state.get("open_orders")
                if not isinstance(prev_managed, dict):
                    prev_managed = {}
                managed_by_token: Dict[str, list[dict]] = {
                    str(token_id): [dict(order) for order in (orders or [])]
                    for token_id, orders in prev_managed.items()
                }

                # Merge remote visibility into ledger WITHOUT dropping unseen managed orders.
                # This makes order_visibility_grace_sec effective even when remote is partially consistent.
                managed_index: Dict[str, tuple[str, int]] = {}
                for t_id, orders in managed_by_token.items():
                    for i, o in enumerate(orders or []):
                        oid = str(o.get("order_id") or "")
                        if oid:
                            managed_index[oid] = (str(t_id), i)

                for t_id, orders in remote_by_token.items():
                    for order in orders or []:
                        oid = str(order.get("order_id") or "")
                        if not oid or oid not in managed_ids:
                            continue

                        if oid not in order_ts_by_id:
                            order_ts_by_id[oid] = int(order.get("ts") or now_ts)
                        order["ts"] = int(order.get("ts") or order_ts_by_id.get(oid) or now_ts)

                        hit = managed_index.get(oid)
                        if hit:
                            t0, i0 = hit
                            # Update the existing ledger order in-place (do NOT overwrite the whole token list)
                            try:
                                managed_by_token[t0][i0].update(order)
                            except Exception:
                                # Fallback if index drifted for any reason
                                t_id_s = str(t_id)
                                managed_by_token.setdefault(t_id_s, []).append(dict(order))
                                managed_index[oid] = (t_id_s, len(managed_by_token[t_id_s]) - 1)
                        else:
                            t_id_s = str(t_id)
                            managed_by_token.setdefault(t_id_s, []).append(dict(order))
                            managed_index[oid] = (t_id_s, len(managed_by_token[t_id_s]) - 1)

                grace_sec = int(cfg.get("order_visibility_grace_sec") or 180)
                pruned = 0

                for token_id, orders in list(managed_by_token.items()):
                    kept: list[dict] = []
                    for order in orders or []:
                        order_id = str(order.get("order_id") or "")
                        if not order_id or order_id not in managed_ids:
                            continue

                        ts = int(order.get("ts") or order_ts_by_id.get(order_id) or now_ts)
                        order["ts"] = ts

                        if order_id not in remote_order_ids and (now_ts - ts) > grace_sec:
                            pruned += 1
                            continue

                        kept.append(order)

                    if kept:
                        managed_by_token[token_id] = kept
                    else:
                        managed_by_token.pop(token_id, None)

                managed_ids = _collect_order_ids(managed_by_token)

                for order_id in list(order_ts_by_id.keys()):
                    if str(order_id) not in managed_ids:
                        order_ts_by_id.pop(order_id, None)

                state["open_orders_all"] = remote_by_token
                state["open_orders"] = managed_by_token
                state["managed_order_ids"] = sorted(managed_ids)

                if pruned:
                    logger.info(
                        "[ORDSYNC] pruned_missing_after_grace=%s grace_sec=%s",
                        pruned,
                        grace_sec,
                    )
                # --- end: ORDSYNC ledger-first ---
            else:
                logger.warning("[WARN] sync open orders failed: %s", err)
        except Exception as exc:
            logger.exception("[ERR] sync open orders failed: %s", exc)
        _prune_order_ts_by_id(state)

        has_buy_by_token: Dict[str, bool] = {}
        has_sell_by_token: Dict[str, bool] = {}
        buy_sum_by_token: Dict[str, float] = {}
        sell_sum_by_token: Dict[str, float] = {}
        actions_info: Dict[str, object] = {"ok": True, "incomplete": False}
        actions_list: list[Dict[str, object]] = []
        actions_source = str(cfg.get("actions_source") or "trades").lower()
        actions_cursor_key = (
            "target_trades_cursor_ms" if actions_source in ("trade", "trades") else "target_actions_cursor_ms"
        )
        actions_cursor_ms = int(state.get(actions_cursor_key) or 0)
        actions_cursor_ms = max(actions_cursor_ms, int(state.get("run_start_ms") or 0))
        actions_replay_window_sec = int(cfg.get("actions_replay_window_sec") or 600)
        actions_lag_threshold_sec = int(cfg.get("actions_lag_threshold_sec") or 180)
        actions_unreliable_hold_sec = int(cfg.get("actions_unreliable_hold_sec") or 120)
        sell_confirm_max = int(cfg.get("sell_confirm_max") or 5)
        sell_confirm_window_sec = int(cfg.get("sell_confirm_window_sec") or 300)
        force_ratio_raw = cfg.get("sell_confirm_force_ratio")
        sell_confirm_force_ratio = 0.5 if force_ratio_raw is None else float(force_ratio_raw)
        force_shares_raw = cfg.get("sell_confirm_force_shares")
        sell_confirm_force_shares = 0.0 if force_shares_raw is None else float(force_shares_raw)
        lag_ms = 0
        now_ms = int(now_ts * 1000)
        replay_from_ms = int(state.get("actions_replay_from_ms") or 0)
        if replay_from_ms > 0 and replay_from_ms != actions_cursor_ms:
            logger.info(
                "[ACTIONS] replay_from_ms=%s cursor_ms=%s",
                replay_from_ms,
                actions_cursor_ms,
            )
            actions_cursor_ms = replay_from_ms
        seen_actions_key = (
            "seen_trade_ids" if actions_source in ("trade", "trades") else "seen_action_ids"
        )
        def _record_action(token_id: str, side: str, size: float) -> None:
            if not token_id or size <= 0:
                return
            if side == "BUY":
                has_buy_by_token[token_id] = True
                buy_sum_by_token[token_id] = buy_sum_by_token.get(token_id, 0.0) + size
            elif side == "SELL":
                has_sell_by_token[token_id] = True
                sell_sum_by_token[token_id] = sell_sum_by_token.get(token_id, 0.0) + size

        try:
            actions_list = []
            actions_info: Dict[str, object] = {}
            retry_sleep_sec = 1.0
            for attempt in range(2):
                try:
                    if actions_source in ("trade", "trades"):
                        actions_list, actions_info = fetch_target_trades_since(
                            data_client,
                            cfg["target_address"],
                            actions_cursor_ms,
                            page_size=actions_page_size,
                            max_offset=actions_max_offset,
                            taker_only=bool(cfg.get("actions_taker_only", False)),
                        )
                    else:
                        actions_list, actions_info = fetch_target_actions_since(
                            data_client,
                            cfg["target_address"],
                            actions_cursor_ms,
                            page_size=actions_page_size,
                            max_offset=actions_max_offset,
                        )
                except Exception as exc:
                    if attempt == 0:
                        logger.warning(
                            "[ACTIONS] fetch failed, retry once after %.1fs: %s",
                            retry_sleep_sec,
                            exc,
                        )
                        time.sleep(retry_sleep_sec)
                        continue
                    raise
                actions_ok = bool(actions_info.get("ok"))
                actions_incomplete = bool(actions_info.get("incomplete"))
                if (not actions_ok) or actions_incomplete:
                    if attempt == 0:
                        logger.warning(
                            "[ACTIONS] unreliable fetch ok=%s incomplete=%s retry once after %.1fs",
                            actions_ok,
                            actions_incomplete,
                            retry_sleep_sec,
                        )
                        time.sleep(retry_sleep_sec)
                        continue
                break
            seen_action_ids = state.setdefault(seen_actions_key, [])
            seen_action_set = {str(item) for item in seen_action_ids}
            filtered_actions: list[Dict[str, object]] = []
            for action in actions_list:
                action_id = _action_identity(action)
                if action_id in seen_action_set:
                    continue
                filtered_actions.append(action)
                seen_action_ids.append(action_id)
                seen_action_set.add(action_id)
            max_seen = int(cfg.get("seen_action_ids_cap") or 5000)
            if len(seen_action_ids) > max_seen:
                del seen_action_ids[:-max_seen]
            actions_list = filtered_actions

            miss_token = 0
            miss_samples: list[list[str]] = []
            for action in actions_list:
                side = str(action.get("side") or "").upper()
                size = float(action.get("size") or 0.0)

                token_id = action.get("token_id") or _extract_token_id_from_raw(
                    action.get("raw") or {}
                )
                if token_id:
                    tid = str(token_id)
                    action["token_id"] = tid
                    _record_action(tid, side, size)
                else:
                    miss_token += 1
                    if len(miss_samples) < 3:
                        raw = action.get("raw") or {}
                        if isinstance(raw, dict):
                            miss_samples.append(sorted(list(raw.keys()))[:25])

            if actions_list:
                actions_missing_ratio = miss_token / len(actions_list)
            if miss_token:
                logger.warning(
                    "[ACT] actions_total=%s token_mapped=%s missing=%s sample_raw_keys=%s",
                    len(actions_list),
                    len(actions_list) - miss_token,
                    miss_token,
                    miss_samples,
                )
                logger.warning(
                    "[ACT] token_missing_ratio=%.3f",
                    actions_missing_ratio,
                )
            latest_action_ms = int(actions_info.get("latest_ms") or 0)
            actions_ok = bool(actions_info.get("ok"))
            actions_incomplete = bool(actions_info.get("incomplete"))
            actions_unreliable = (not actions_ok) or actions_incomplete
            if actions_unreliable:
                state["actions_unreliable_until"] = now_ts + actions_unreliable_hold_sec
                state["actions_replay_from_ms"] = max(
                    0, now_ms - actions_replay_window_sec * 1000
                )
                logger.warning(
                    "[ACTIONS] unreliable ok=%s incomplete=%s keep_cursor_ms=%s replay_from_ms=%s",
                    actions_ok,
                    actions_incomplete,
                    actions_cursor_ms,
                    state["actions_replay_from_ms"],
                )
            else:
                state.pop("actions_unreliable_until", None)
                if latest_action_ms > actions_cursor_ms:
                    state[actions_cursor_key] = latest_action_ms
                if replay_from_ms > 0 and latest_action_ms >= actions_cursor_ms:
                    state.pop("actions_replay_from_ms", None)
                lag_ms = now_ms - latest_action_ms if latest_action_ms > 0 else 0
                if lag_ms > actions_lag_threshold_sec * 1000:
                    state["actions_replay_from_ms"] = max(
                        0, now_ms - actions_replay_window_sec * 1000
                    )
                    logger.warning(
                        "[ACTIONS] lag_ms=%s replay_from_ms=%s latest_ms=%s",
                        lag_ms,
                        state["actions_replay_from_ms"],
                        latest_action_ms,
                    )
        except Exception as exc:
            logger.exception("[ERR] fetch target actions failed: %s", exc)

        my_trades_unreliable_hold_sec = int(cfg.get("my_trades_unreliable_hold_sec") or 0)
        if my_trades_unreliable_hold_sec <= 0:
            my_trades_unreliable_hold_sec = actions_unreliable_hold_sec
        try:
            my_trades_cursor_ms = int(state.get("my_trades_cursor_ms") or 0)
            my_trades, my_trades_info = fetch_target_trades_since(
                data_client,
                cfg["my_address"],
                my_trades_cursor_ms,
                page_size=actions_page_size,
                max_offset=actions_max_offset,
            )
            seen_my_trade_ids = state.setdefault("seen_my_trade_ids", [])
            seen_my_trade_set = {str(item) for item in seen_my_trade_ids}
            filtered_my_trades: list[Dict[str, object]] = []
            for trade in my_trades:
                trade_id = _action_identity(trade)
                if trade_id in seen_my_trade_set:
                    continue
                filtered_my_trades.append(trade)
                seen_my_trade_ids.append(trade_id)
                seen_my_trade_set.add(trade_id)
            max_seen = int(cfg.get("seen_action_ids_cap") or 5000)
            if len(seen_my_trade_ids) > max_seen:
                del seen_my_trade_ids[:-max_seen]
            my_trades = filtered_my_trades

            miss_trade_token = 0
            miss_trade_samples: list[list[str]] = []
            for trade in my_trades:
                side = str(trade.get("side") or "").upper()
                if side != "BUY":
                    continue
                token_key = trade.get("token_key")
                token_id = trade.get("token_id") or _extract_token_id_from_raw(
                    trade.get("raw") or {}
                )
                if not token_id:
                    miss_trade_token += 1
                    if token_key:
                        unresolved_trade_candidates.append(
                            {
                                "token_key": token_key,
                                "condition_id": trade.get("condition_id"),
                                "outcome_index": trade.get("outcome_index"),
                                "slug": None,
                                "raw": trade.get("raw") or {},
                            }
                        )
                    if len(miss_trade_samples) < 3:
                        raw = trade.get("raw") or {}
                        if isinstance(raw, dict):
                            miss_trade_samples.append(sorted(list(raw.keys()))[:25])
            if miss_trade_token:
                miss_trade_ratio = (
                    miss_trade_token / len(my_trades) if my_trades else 0.0
                )
                logger.warning(
                    "[MY_TRADES] token_missing=%s total=%s ratio=%.3f sample_raw_keys=%s",
                    miss_trade_token,
                    len(my_trades),
                    miss_trade_ratio,
                    miss_trade_samples,
                )
            trades_ok = bool(my_trades_info.get("ok", True))
            trades_incomplete = bool(my_trades_info.get("incomplete", False))
            if not trades_ok or trades_incomplete:
                state["my_trades_unreliable_until"] = now_ts + my_trades_unreliable_hold_sec
                logger.warning(
                    "[MY_TRADES] unreliable ok=%s incomplete=%s hold_sec=%s",
                    trades_ok,
                    trades_incomplete,
                    my_trades_unreliable_hold_sec,
                )
            else:
                state["my_trades_unreliable_until"] = 0
            latest_trade_ms = int(my_trades_info.get("latest_ms") or 0)
            if latest_trade_ms > my_trades_cursor_ms:
                state["my_trades_cursor_ms"] = latest_trade_ms
        except Exception as exc:
            state["my_trades_unreliable_until"] = now_ts + my_trades_unreliable_hold_sec
            logger.exception("[ERR] fetch my trades failed: %s", exc)

        has_new_actions = bool(actions_list)
        if has_new_actions:
            state["target_positions_nonce_actions"] = int(
                state.get("target_positions_nonce_actions") or 0
            ) + len(actions_list)
        nonce_min_interval_sec = max(1, min(target_positions_refresh_sec, poll_interval))
        nonce_action_window = max(
            1, int(max(target_positions_refresh_sec, poll_interval) / max(poll_interval, 1))
        )
        last_nonce_ts = float(state.get("target_positions_nonce_last_ts") or 0)
        allow_nonce = (
            has_new_actions
            and (now_ts - last_nonce_ts) >= nonce_min_interval_sec
            and int(state.get("target_positions_nonce_actions") or 0) >= nonce_action_window
        )
        if allow_nonce:
            target_cache_mode = "nonce"
            state["target_positions_nonce_last_ts"] = now_ts
            state["target_positions_nonce_actions"] = 0
        else:
            target_cache_mode = target_cache_bust_mode

        target_pos, target_info = fetch_positions_norm(
            data_client,
            cfg["target_address"],
            size_threshold,
            positions_limit=positions_limit,
            positions_max_pages=positions_max_pages,
            refresh_sec=target_positions_refresh_sec,
            force_http=True,
            cache_bust_mode=target_cache_mode,
            header_keys=header_keys,
        )
        hard_cap = positions_limit * positions_max_pages
        if len(target_pos) >= hard_cap:
            target_info["incomplete"] = True
            logger.info("[SAFE] target positions 可能截断(len>=hard_cap=%s), 跳过本轮", hard_cap)

        my_pos, my_info = fetch_positions_norm(
            data_client,
            cfg["my_address"],
            0.0,
            positions_limit=positions_limit,
            positions_max_pages=positions_max_pages,
            refresh_sec=target_positions_refresh_sec if my_positions_force_http else None,
            force_http=my_positions_force_http,
            cache_bust_mode=target_cache_bust_mode,
            header_keys=header_keys,
        )
        if len(my_pos) >= hard_cap:
            my_info["incomplete"] = True
            logger.info("[SAFE] my positions 可能截断(len>=hard_cap=%s), 跳过本轮", hard_cap)

        closed_token_keys = state.get("closed_token_keys")
        if not isinstance(closed_token_keys, dict):
            closed_token_keys = {}
            state["closed_token_keys"] = closed_token_keys
        new_closed = 0
        for pos in target_pos + my_pos:
            token_key = pos.get("token_key")
            if not token_key or token_key in closed_token_keys:
                continue
            closed, end_ts = _is_closed_by_end_date(pos, now_ts)
            if closed:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[SKIP_DEBUG] closed_by_end_date token_key=%s token_id=%s slug=%s end_date=%s end_ts=%s now_ts=%s",
                        token_key,
                        pos.get("token_id"),
                        pos.get("slug"),
                        pos.get("end_date") or pos.get("endDate"),
                        end_ts,
                        now_ts,
                    )
                closed_token_keys[str(token_key)] = int(end_ts or now_ts)
                new_closed += 1
        if new_closed:
            logger.info("[SKIP] closed_token_keys added count=%s", new_closed)

        # CRITICAL FIX: Save unfiltered my_pos for risk calculation
        # This prevents skip_closed_markets from weakening risk baseline
        # and causing position limit breaches after balance top-ups
        my_pos_for_risk = list(my_pos)  # Deep copy for risk baseline

        if closed_token_keys:
            target_pos, removed_target = _filter_closed_positions(target_pos, closed_token_keys)
            my_pos, removed_my = _filter_closed_positions(my_pos, closed_token_keys)
            if removed_target or removed_my:
                logger.info(
                    "[SKIP] closed_positions filtered target=%s my=%s (trading only, risk still uses unfiltered)",
                    removed_target,
                    removed_my,
                )

        should_log_heartbeat = has_new_actions or (
            now_ts - last_heartbeat_ts >= heartbeat_interval_sec
        )
        if should_log_heartbeat:
            logger.info(
                "[POS] target_count=%s my_count=%s target_incomplete=%s my_incomplete=%s | "
                "t_src=%s t_cb=%s t_rsec=%s t_mode=%s t_http=%s t_hit=%s",
                len(target_pos),
                len(my_pos),
                bool(target_info.get("incomplete")),
                bool(my_info.get("incomplete")),
                target_info.get("source"),
                target_info.get("cache_bucket"),
                target_info.get("refresh_sec"),
                target_info.get("cache_bust_mode"),
                target_info.get("http_status"),
                target_info.get("cache_hit_hint"),
            )
            if target_info.get("incomplete"):
                logger.info(
                    "[POS] target positions info limit=%s total=%s max_pages=%s",
                    target_info.get("limit"),
                    target_info.get("total"),
                    target_info.get("max_pages"),
                )
            if log_cache_headers:
                logger.info(
                    "[POS] target cache_headers_first=%s",
                    target_info.get("cache_headers_first"),
                )
                logger.info(
                    "[POS] target cache_headers_last=%s",
                    target_info.get("cache_headers_last"),
                )
            last_heartbeat_ts = now_ts

        if not target_info.get("ok") or target_info.get("incomplete"):
            logger.warning("[SAFE] target positions 不完整，跳过本轮执行")
            save_state(args.state, state)
            time.sleep(_get_poll_interval())
            continue

        if not my_info.get("ok") or my_info.get("incomplete"):
            logger.warning("[SAFE] my positions 不完整，跳过本轮执行")
            save_state(args.state, state)
            time.sleep(_get_poll_interval())
            continue

        boot_sync_mode = str(cfg.get("boot_sync_mode") or "baseline_only").lower()
        fresh_boot = bool(cfg.get("fresh_boot_on_start", False))
        boot_needed = boot_sync_mode == "baseline_only" and (
            (not state.get("bootstrapped"))
            or (
                fresh_boot
                and int(state.get("boot_run_start_ms") or 0)
                != int(state.get("run_start_ms") or 0)
            )
        )
        if boot_needed:
            boot_by_key: Dict[str, float] = {}
            boot_keys: list[str] = []
            token_map = (
                state.get("token_map", {}) if isinstance(state.get("token_map"), dict) else {}
            )
            state["token_map"] = token_map
            for pos in target_pos:
                token_key = str(pos.get("token_key") or "").strip()
                if not token_key:
                    continue
                raw_id = _extract_token_id_from_raw(pos.get("raw") or {})
                if raw_id:
                    token_map.setdefault(token_key, str(raw_id))
                size = float(pos.get("size") or 0.0)
                boot_by_key[token_key] = size
                boot_keys.append(token_key)
            boot_keys = sorted(set(boot_keys))
            state["boot_token_keys"] = boot_keys
            state["target_last_shares_by_token_key"] = boot_by_key

            boot_token_ids: list[str] = []
            for token_key in boot_keys:
                token_id = token_map.get(token_key)
                if token_id:
                    boot_token_ids.append(token_id)
                    state.setdefault("target_last_shares", {})[token_id] = float(
                        boot_by_key.get(token_key) or 0.0
                    )
                    state.setdefault("target_last_seen_ts", {})[token_id] = now_ts
                    state.setdefault("target_missing_streak", {})[token_id] = 0
            state["boot_token_ids"] = sorted(set(boot_token_ids))

            state["target_actions_cursor_ms"] = int(state.get("run_start_ms") or 0)
            state["seen_action_ids"] = []
            state["topic_state"] = {}
            state["probed_token_ids"] = []
            state["boot_run_start_ms"] = int(state.get("run_start_ms") or 0)
            state["bootstrapped"] = True
            logger.info(
                "[BOOT] baseline_only: baseline_keys=%s baseline_ids=%s cursor_ms=%s",
                len(boot_keys),
                len(state["boot_token_ids"]),
                state["target_actions_cursor_ms"],
            )
            save_state(args.state, state)
            time.sleep(_get_poll_interval())
            continue

        token_map = state.get("token_map", {})
        if not isinstance(token_map, dict):
            token_map = {}
            state["token_map"] = token_map

        token_key_by_token_id: Dict[str, str] = {
            str(token_id): str(token_key) for token_key, token_id in token_map.items()
        }

        # Build target shares maps without doing full gamma resolver (avoid freezing on huge accounts).
        # Fast-path: use cached token_map or token_id embedded in pos["raw"].
        target_shares_now_by_token_id: Dict[str, float] = {}
        target_shares_now_by_token_key: Dict[str, float] = {}
        unresolved_target = 0
        resolved_by_cache = 0
        resolved_by_raw = 0
        resolved_by_resolver = 0
        resolver_fail = 0
        resolve_target_budget = max(0, int(max_resolve_target_positions_per_loop or 0))
        for pos in target_pos:
            token_key = str(pos.get("token_key") or "")
            if not token_key:
                continue
            size = float(pos.get("size") or 0.0)
            target_shares_now_by_token_key[token_key] = size

            token_id = token_map.get(token_key)
            if token_id:
                resolved_by_cache += 1
            else:
                token_id = _extract_token_id_from_raw(pos.get("raw") or {})
                if token_id:
                    token_map[token_key] = str(token_id)
                    resolved_by_raw += 1
                elif resolve_target_budget > 0:
                    resolve_target_budget -= 1
                    fail_ts = resolver_fail_cache.get(token_key)
                    if (
                        fail_ts
                        and resolver_fail_cooldown_sec > 0
                        and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
                    ):
                        unresolved_target += 1
                        continue
                    try:
                        token_id = resolve_token_id(token_key, pos, token_map)
                    except Exception as exc:
                        resolver_fail += 1
                        logger.warning("[WARN] resolver 失败(target): %s -> %s", token_key, exc)
                        resolver_fail_cache[token_key] = now_ts
                        unresolved_target += 1
                        continue
                    token_map[token_key] = str(token_id)
                    resolved_by_resolver += 1
                else:
                    unresolved_target += 1
                    continue

            tid = str(token_id)
            target_shares_now_by_token_id[tid] = size
            token_key_by_token_id[tid] = token_key
            cur_price = float(pos.get("cur_price") or 0.0)
            if cur_price > 0:
                state.setdefault("last_mid_price_by_token_id", {})[tid] = cur_price
                state["last_mid_price_update_ts"] = now_ts

        if unresolved_target:
            logger.info(
                "[POSMAP] target idmap cache=%d raw=%d resolver=%d pending=%d total=%d",
                resolved_by_cache,
                resolved_by_raw,
                resolved_by_resolver,
                unresolved_target,
                len(target_pos),
            )

        # My positions are usually small; still prefer fast-path and fall back to resolver if needed.
        my_by_token_id: Dict[str, float] = {}
        for pos in my_pos:
            token_key = str(pos.get("token_key") or "")
            size = float(pos.get("size") or 0.0)

            token_id = pos.get("token_id") or None
            if token_key:
                token_id = token_id or token_map.get(token_key)
            token_id = token_id or _extract_token_id_from_raw(pos.get("raw") or {})
            if not token_id and token_key:
                fail_ts = resolver_fail_cache.get(token_key)
                if (
                    fail_ts
                    and resolver_fail_cooldown_sec > 0
                    and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
                ):
                    continue
                try:
                    token_id = resolve_token_id(token_key, pos, token_map)
                except Exception as exc:
                    logger.warning("[WARN] resolver 失败(自身): %s -> %s", token_key, exc)
                    resolver_fail_cache[token_key] = now_ts
                    continue
            if not token_id:
                continue

            tid = str(token_id)
            if token_key:
                token_map[token_key] = tid
                token_key_by_token_id.setdefault(tid, token_key)
            cur_price = float(pos.get("cur_price") or 0.0)
            if cur_price > 0:
                state.setdefault("last_mid_price_by_token_id", {})[tid] = cur_price
                state["last_mid_price_update_ts"] = now_ts
            my_by_token_id[tid] = size

        # CRITICAL FIX: Build unfiltered position dict for risk calculation
        # This ensures risk baseline includes positions from closed markets that may still be tradeable
        my_by_token_id_for_risk: Dict[str, float] = {}
        for pos in my_pos_for_risk:
            token_key = str(pos.get("token_key") or "")
            size = float(pos.get("size") or 0.0)
            token_id = pos.get("token_id") or None
            if token_key:
                token_id = token_id or token_map.get(token_key)
            token_id = token_id or _extract_token_id_from_raw(pos.get("raw") or {})

            # CRITICAL FIX: Add resolver fallback for risk baseline (same as my_by_token_id)
            # This prevents position limit breaches when API returns positions without token_id
            if not token_id and token_key:
                fail_ts = resolver_fail_cache.get(token_key)
                if (
                    fail_ts
                    and resolver_fail_cooldown_sec > 0
                    and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
                ):
                    logger.warning(
                        "[RISK] skip position due to recent resolver fail: %s (cooldown)",
                        token_key,
                    )
                    continue
                try:
                    token_id = resolve_token_id(token_key, pos, token_map)
                    logger.debug("[RISK] resolved token_id via resolver: %s -> %s", token_key, token_id)
                except Exception as exc:
                    logger.warning("[RISK] resolver fail for position: %s -> %s", token_key, exc)
                    resolver_fail_cache[token_key] = now_ts
                    continue

            if not token_id:
                logger.warning(
                    "[RISK] skip position: missing token_id token_key=%s size=%.2f",
                    token_key,
                    size,
                )
                continue
            tid = str(token_id)
            my_by_token_id_for_risk[tid] = size

        resolve_budget = int(cfg.get("max_resolve_actions_per_loop") or 20)
        missing_ratio_threshold = float(cfg.get("resolve_actions_missing_ratio") or 0.3)
        if actions_list and actions_missing_ratio >= missing_ratio_threshold:
            boosted = int(cfg.get("max_resolve_actions_on_missing") or 60)
            if boosted > resolve_budget:
                logger.warning(
                    "[ACT] missing_ratio=%.3f boosting resolver budget %s->%s",
                    actions_missing_ratio,
                    resolve_budget,
                    boosted,
                )
                resolve_budget = boosted
        for action in actions_list:
            token_id = action.get("token_id")
            token_key = action.get("token_key")
            if token_id:
                token_key_by_token_id.setdefault(str(token_id), str(token_key or ""))
                continue
            if not token_key:
                continue
            token_id = token_map.get(str(token_key)) or _extract_token_id_from_raw(action.get("raw") or {})
            if token_id:
                tid = str(token_id)
                token_map[str(token_key)] = tid
                token_key_by_token_id.setdefault(tid, str(token_key))
                side = str(action.get("side") or "").upper()
                size = float(action.get("size") or 0.0)
                _record_action(tid, side, size)
                continue
            if resolve_budget <= 0:
                continue
            resolve_budget -= 1
            fail_ts = resolver_fail_cache.get(str(token_key))
            if (
                fail_ts
                and resolver_fail_cooldown_sec > 0
                and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
            ):
                continue
            try:
                token_id = resolve_token_id(
                    token_key,
                    {
                        "token_key": token_key,
                        "condition_id": action.get("condition_id"),
                        "outcome_index": action.get("outcome_index"),
                        "slug": None,
                        "raw": action.get("raw") or {},
                    },
                    token_map,
                )
            except Exception as exc:
                logger.warning("[WARN] resolver 失败(actions): %s -> %s", token_key, exc)
                resolver_fail_cache[str(token_key)] = now_ts
                continue
            side = str(action.get("side") or "").upper()
            size = float(action.get("size") or 0.0)
            tid = str(token_id)
            token_map[str(token_key)] = tid
            _record_action(tid, side, size)
            token_key_by_token_id.setdefault(tid, str(token_key))

        resolve_trade_budget = int(cfg.get("max_resolve_trades_per_loop") or 10)
        if unresolved_trade_candidates and resolve_trade_budget > 0:
            logger.warning(
                "[MY_TRADES] unresolved_trades=%s resolve_budget=%s",
                len(unresolved_trade_candidates),
                resolve_trade_budget,
            )
            for trade in unresolved_trade_candidates:
                if resolve_trade_budget <= 0:
                    break
                token_key = str(trade.get("token_key") or "")
                if not token_key:
                    continue
                if token_map.get(token_key):
                    continue
                resolve_trade_budget -= 1
                fail_ts = resolver_fail_cache.get(token_key)
                if (
                    fail_ts
                    and resolver_fail_cooldown_sec > 0
                    and now_ts - int(fail_ts or 0) < resolver_fail_cooldown_sec
                ):
                    continue
                try:
                    token_id = resolve_token_id(token_key, trade, token_map)
                except Exception as exc:
                    logger.warning("[WARN] resolver 失败(trades): %s -> %s", token_key, exc)
                    resolver_fail_cache[token_key] = now_ts
                    continue
                tid = str(token_id)
                token_map[token_key] = tid
                token_key_by_token_id.setdefault(tid, token_key)

        reconcile_set: Set[str] = set(target_shares_now_by_token_id)
        reconcile_set.update(state.get("target_last_shares", {}).keys())
        reconcile_set.update(my_by_token_id)
        reconcile_set.update(state.get("open_orders", {}).keys())
        reconcile_set.update(set(has_buy_by_token.keys()) | set(has_sell_by_token.keys()))
        reconcile_set.update(state.get("topic_state", {}).keys())
        lag_high = lag_ms > actions_lag_threshold_sec * 1000
        actions_unreliable_until = int(state.get("actions_unreliable_until") or 0)
        actions_unreliable = actions_unreliable_until > now_ts
        reduce_reconcile = ((not actions_list) and (not actions_unreliable)) or lag_high
        if reduce_reconcile:
            recent_event_sec = int(cfg.get("reconcile_recent_event_sec") or 600)
            cutoff_ts = now_ts - max(recent_event_sec, 0)
            reduced_set: Set[str] = set(state.get("open_orders", {}).keys())
            reduced_set.update(my_by_token_id)
            reduced_set.update(has_buy_by_token.keys())
            reduced_set.update(has_sell_by_token.keys())
            for token_id, ts in state.get("target_last_event_ts", {}).items():
                try:
                    if int(ts or 0) >= cutoff_ts:
                        reduced_set.add(str(token_id))
                except Exception:
                    continue
            for action in actions_list:
                token_id = action.get("token_id")
                if token_id:
                    reduced_set.add(str(token_id))
            reconcile_set = reduced_set
            reason = "lag_high" if lag_high else "actions_empty"
            logger.info(
                "[SAFE] %s reduce_reconcile_set size=%s recent_sec=%s lag_ms=%s actions=%s actions_unreliable=%s",
                reason,
                len(reconcile_set),
                recent_event_sec,
                lag_ms,
                len(actions_list),
                actions_unreliable,
            )

        ignored = state["ignored_tokens"]
        expired_ignored = [
            token_id
            for token_id, meta in ignored.items()
            if isinstance(meta, dict)
            and meta.get("expires_at")
            and now_ts >= int(meta.get("expires_at") or 0)
        ]
        for token_id in expired_ignored:
            ignored.pop(token_id, None)
        active_ignored = {
            token_id
            for token_id, meta in ignored.items()
            if isinstance(meta, dict)
            and meta.get("expires_at")
            and now_ts < int(meta.get("expires_at") or 0)
        }
        if active_ignored:
            for token_id in sorted(active_ignored):
                meta = ignored.get(token_id)
                if not isinstance(meta, dict):
                    continue
                if meta.get("active_logged"):
                    continue
                logger.info(
                    "[SKIP] active_ignore token_id=%s expires_at=%s",
                    token_id,
                    int(meta.get("expires_at") or 0),
                )
                meta["active_logged"] = True
            reconcile_set = {token_id for token_id in reconcile_set if token_id not in active_ignored}
        status_cache = state["market_status_cache"]
        if skip_closed:
            def _ensure_long_ignore(token_id: str, meta: Optional[Dict[str, Any]]) -> None:
                end_date = None
                if isinstance(meta, dict):
                    end_date = meta.get("end_date") or meta.get("endDate")
                expires_at = _parse_market_end_ts(meta) or now_ts + 24 * 3600
                existing = ignored.get(token_id) if isinstance(ignored.get(token_id), dict) else {}
                should_log = not existing or not existing.get("logged")
                ignored[token_id] = {
                    "ts": now_ts,
                    "reason": "closed_or_not_tradeable",
                    "expires_at": int(expires_at),
                    "end_date": end_date,
                    "logged": bool(existing.get("logged")),
                }
                if should_log:
                    logger.info(
                        "[SKIP] long_ignore token_id=%s expires_at=%s end_date=%s",
                        token_id,
                        int(expires_at),
                        end_date,
                    )
                    ignored[token_id]["logged"] = True

            need_query = []
            for token_id in reconcile_set:
                if token_id in ignored:
                    continue
                cached = status_cache.get(token_id)
                if not cached or now_ts - int(cached.get("ts") or 0) >= refresh_sec:
                    need_query.append(token_id)

            if need_query:
                meta_map = gamma_fetch_markets_by_clob_token_ids(need_query)
                for token_id in need_query:
                    meta = meta_map.get(token_id)
                    tradeable = market_tradeable_state(meta)
                    status_cache[token_id] = {"ts": now_ts, "tradeable": tradeable, "meta": meta}
                    if tradeable is False:
                        _ensure_long_ignore(token_id, meta)

            for token_id in reconcile_set:
                cached = status_cache.get(token_id) or {}
                if cached.get("tradeable") is False:
                    _ensure_long_ignore(token_id, cached.get("meta"))

        orderbooks: Dict[str, Dict[str, Optional[float]]] = {}

        mode = str(cfg.get("order_size_mode") or "fixed_shares").lower()
        min_usd = float(cfg.get("min_order_usd") or 5.0)
        max_usd = float(cfg.get("max_order_usd") or 25.0)
        target_mid_usd = (min_usd + max_usd) / 2.0
        max_position_usd_per_token = float(cfg.get("max_position_usd_per_token") or 0.0)
        max_notional_per_token = float(cfg.get("max_notional_per_token") or 0.0)
        max_notional_total = float(cfg.get("max_notional_total") or 0.0)
        buy_window_sec = int(cfg.get("buy_window_sec") or 0)
        buy_window_max_usd_per_token = float(cfg.get("buy_window_max_usd_per_token") or 0.0)
        buy_window_max_usd_total = float(cfg.get("buy_window_max_usd_total") or 0.0)
        fallback_mid_price = float(cfg.get("missing_mid_fallback_price") or 1.0)
        cooldown_sec = int(cfg.get("cooldown_sec_per_token") or 0)
        shadow_ttl_sec = int(cfg.get("shadow_buy_ttl_sec") or 120)
        missing_timeout_sec = int(cfg.get("missing_timeout_sec") or 0)
        missing_freeze_streak = int(cfg.get("missing_freeze_streak") or 5)
        missing_freeze_min_sec = int(cfg.get("missing_freeze_min_sec") or 600)
        missing_freeze_max_sec = int(cfg.get("missing_freeze_max_sec") or 1800)
        if missing_freeze_min_sec > missing_freeze_max_sec:
            missing_freeze_min_sec, missing_freeze_max_sec = (
                missing_freeze_max_sec,
                missing_freeze_min_sec,
            )
        missing_to_zero_rounds = int(cfg.get("missing_to_zero_rounds") or 0)
        orphan_cancel_rounds = int(cfg.get("orphan_cancel_rounds") or 3)
        orphan_ignore_sec = int(cfg.get("orphan_ignore_sec") or 120)
        debug_token_ids = {str(token_id) for token_id in (cfg.get("debug_token_ids") or [])}
        eps = float(cfg.get("delta_eps") or 1e-9)
        topic_mode = bool(cfg.get("topic_cycle_mode", True))
        entry_settle_sec = int(cfg.get("topic_entry_settle_sec", 60))

        ema = state.get("sizing", {}).get("ema_delta_usd")
        if ema is None or ema <= 0:
            ema = target_mid_usd * 3.0

        k = target_mid_usd / max(ema, 1e-9)
        k = max(0.002, min(1.2, k))

        cfg["_auto_order_k"] = k

        delta_usd_samples = []

        # CRITICAL FIX: Use unfiltered positions for risk calculation
        # This prevents position limit breaches from weakened risk baseline
        (
            planned_total_notional,
            planned_by_token_usd,
            order_info_by_id,
            shadow_buy_usd,
        ) = _calc_planned_notional_with_fallback(
            my_by_token_id_for_risk,  # Use unfiltered positions
            state.get("open_orders", {}),
            state.get("last_mid_price_by_token_id", {}),
            max_position_usd_per_token,
            state,
            now_ts,
            shadow_ttl_sec,
            fallback_mid_price,
            logger,
            include_shadow=False,
        )
        (
            planned_total_notional_shadow,
            planned_by_token_usd_shadow,
            _shadow_order_info_by_id,
            _shadow_buy_usd,
        ) = _calc_planned_notional_with_fallback(
            my_by_token_id_for_risk,  # Use unfiltered positions
            state.get("open_orders", {}),
            state.get("last_mid_price_by_token_id", {}),
            max_position_usd_per_token,
            state,
            now_ts,
            shadow_ttl_sec,
            fallback_mid_price,
            logger,
            include_shadow=True,
        )
        open_buy_orders_usd = sum(float(info.get("usd") or 0.0) for info in order_info_by_id.values())
        recent_buy_total, recent_buy_by_token = _calc_recent_buy_notional(
            state,
            now_ts,
            buy_window_sec,
        )
        top_tokens = sorted(planned_by_token_usd.items(), key=lambda item: item[1], reverse=True)[:5]
        top_tokens_fmt = [
            f"{token_key_by_token_id.get(token_id, token_id)}={usd:.4f}" for token_id, usd in top_tokens
        ]
        logger.info(
            "[RISK_SUMMARY] used_total=%s used_total_shadow=%s open_buy_orders_usd=%s shadow_buy_usd=%s "
            "recent_buy_usd=%s top_tokens=%s",
            planned_total_notional,
            planned_total_notional_shadow,
            open_buy_orders_usd,
            shadow_buy_usd,
            recent_buy_total,
            top_tokens_fmt,
        )

        # CRITICAL: Accumulator is maintained ONLY by local BUY/SELL operations
        # DO NOT reconcile with API data to preserve independence from position API sync issues
        # Accumulator updates happen in ct_exec.py:
        # - Incremented on successful BUY (both taker and maker)
        # - Decremented on successful SELL
        # - Cleared when SELL reduces it below threshold (0.01)

        my_trades_unreliable_until = int(state.get("my_trades_unreliable_until") or 0)
        my_trades_unreliable = my_trades_unreliable_until > now_ts
        if my_trades_unreliable:
            logger.warning(
                "[MY_TRADES] unreliable freeze buys until=%s",
                my_trades_unreliable_until,
            )

        for token_id in reconcile_set:
            if token_id in active_ignored:
                continue
            open_orders = state.get("open_orders", {}).get(token_id, [])
            cached = status_cache.get(token_id) or {}
            token_meta = cached.get("meta") if isinstance(cached, dict) else None
            if isinstance(token_meta, dict):
                token_title = (
                    token_meta.get("title")
                    or token_meta.get("question")
                    or token_meta.get("marketTitle")
                    or token_meta.get("market_title")
                )
            else:
                token_title = None
            cooldown_until = int(state.get("cooldown_until", {}).get(token_id) or 0)
            cooldown_active = cooldown_sec > 0 and now_ts < cooldown_until
            place_fail_until = int(state.get("place_fail_until", {}).get(token_id) or 0)
            place_backoff_active = place_fail_until > 0 and now_ts < place_fail_until
            if cooldown_active:
                logger.info(
                    "[COOLDOWN] token_id=%s until=%s",
                    token_id,
                    cooldown_until,
                )
            if place_backoff_active:
                logger.info(
                    "[PLACE_BACKOFF] token_id=%s until=%s",
                    token_id,
                    place_fail_until,
                )

            missing_freeze = state.setdefault("missing_data_freeze", {})
            freeze_meta = missing_freeze.get(token_id)
            if isinstance(freeze_meta, dict) and freeze_meta.get("expires_at"):
                expires_at = int(freeze_meta.get("expires_at") or 0)
                if expires_at > 0 and now_ts >= expires_at:
                    missing_freeze.pop(token_id, None)
                    logger.info(
                        "[UNFREEZE] token_id=%s reason=%s expired_at=%s",
                        token_id,
                        freeze_meta.get("reason") or "missing_streak",
                        expires_at,
                    )
                    freeze_meta = None
            if (
                isinstance(freeze_meta, dict)
                and freeze_meta.get("expires_at")
                and freeze_meta.get("reason") == "missing_streak"
                and token_id not in my_by_token_id
                and not open_orders
            ):
                logger.info(
                    "[SKIP] token_id=%s reason=missing_streak_freeze until=%s",
                    token_id,
                    freeze_meta.get("expires_at"),
                )
                continue

            if skip_closed:
                if token_id in ignored:
                    if open_orders:
                        logger.info(
                            "[SKIP] ignored token_id=%s open_orders=%s",
                            token_id,
                            len(open_orders),
                        )
                    continue
                tradeable = cached.get("tradeable")

                if tradeable is False:
                    if open_orders:
                        actions = [
                            {"type": "cancel", "order_id": order.get("order_id")}
                            for order in open_orders
                            if order.get("order_id")
                        ]
                        if actions:
                            logger.info(
                                "[CLOSE] token_id=%s cancel_managed_orders=%s",
                                token_id,
                                len(actions),
                            )
                            updated_orders = apply_actions(
                                clob_client,
                                actions,
                                open_orders,
                                now_ts,
                                args.dry_run,
                                cfg=cfg,
                                state=state,
                            )
                            if updated_orders:
                                state.setdefault("open_orders", {})[token_id] = updated_orders
                            else:
                                state.get("open_orders", {}).pop(token_id, None)
                            _prune_order_ts_by_id(state)
                            _refresh_managed_order_ids(state)
                            (
                                planned_total_notional,
                                planned_by_token_usd,
                                order_info_by_id,
                                _shadow_buy_usd,
                            ) = _calc_planned_notional_with_fallback(
                                my_by_token_id,
                                state.get("open_orders", {}),
                                state.get("last_mid_price_by_token_id", {}),
                                max_position_usd_per_token,
                                state,
                                now_ts,
                                shadow_ttl_sec,
                                fallback_mid_price,
                                logger,
                                include_shadow=False,
                            )
                            (
                                planned_total_notional_shadow,
                                planned_by_token_usd_shadow,
                                _shadow_order_info_by_id,
                                _shadow_buy_usd,
                            ) = _calc_planned_notional_with_fallback(
                                my_by_token_id,
                                state.get("open_orders", {}),
                                state.get("last_mid_price_by_token_id", {}),
                                max_position_usd_per_token,
                                state,
                                now_ts,
                                shadow_ttl_sec,
                                fallback_mid_price,
                                logger,
                                include_shadow=True,
                            )
                    ignored[token_id] = {"ts": now_ts, "reason": "closed_or_not_tradeable"}
                    meta = cached.get("meta") or {}
                    slug = meta.get("slug") or ""
                    logger.info("[SKIP] closed/inactive token_id=%s slug=%s", token_id, slug)
                    continue

                if tradeable is None:
                    if bool(cfg.get("block_on_unknown_market_state", False)):
                        logger.warning("[WARN] market 状态未知(阻塞模式): token_id=%s", token_id)
                        continue
                    logger.warning("[WARN] market 状态未知(不阻塞交易): token_id=%s", token_id)

            t_now_present = token_id in target_shares_now_by_token_id
            t_now = target_shares_now_by_token_id.get(token_id) if t_now_present else None
            token_key = token_key_by_token_id.get(token_id, f"token:{token_id}")
            if (not t_now_present) and isinstance(target_shares_now_by_token_key, dict):
                alt = target_shares_now_by_token_key.get(token_key)
                if alt is not None:
                    t_now_present = True
                    t_now = float(alt)
            missing_data = t_now is None
            boot_key_set = set(state.get("boot_token_keys", []))
            is_boot_token = token_key in boot_key_set

            ignore_boot_tokens = bool(cfg.get("ignore_boot_tokens", True))
            boot_scope = str(cfg.get("ignore_boot_tokens_scope") or "probe_only").lower()
            # scope 说明：
            # - "probe_only"（默认）：仅阻止 boot token 的 probe（防开机误买），允许后续增量 BUY 跟单
            # - "all"：旧行为，boot token 的 BUY 也阻止（不推荐）
            probe_blocked_by_boot = (
                ignore_boot_tokens
                and is_boot_token
                and boot_scope in ("probe_only", "probe", "all", "full")
            )
            buy_blocked_by_boot = (
                ignore_boot_tokens and is_boot_token and boot_scope in ("all", "full")
            )
            t_last = state.get("target_last_shares", {}).get(token_id)
            if t_last is None:
                boot_by_key = state.get("target_last_shares_by_token_key", {})
                if isinstance(boot_by_key, dict):
                    base = boot_by_key.get(token_key)
                    if base is not None:
                        state.setdefault("target_last_shares", {})[token_id] = float(base)
                        t_last = float(base)
                        boot_ids = set(state.get("boot_token_ids", []))
                        boot_ids.add(token_id)
                        state["boot_token_ids"] = sorted(boot_ids)
            my_shares = my_by_token_id.get(token_id, 0.0)
            open_orders_count = len(open_orders)
            missing_streak = int(state.get("target_missing_streak", {}).get(token_id) or 0)
            last_seen_ts = int(state.get("target_last_seen_ts", {}).get(token_id) or 0)
            has_buy = bool(has_buy_by_token.get(token_id))
            has_sell = bool(has_sell_by_token.get(token_id))
            buy_sum = float(buy_sum_by_token.get(token_id, 0.0))
            sell_sum = float(sell_sum_by_token.get(token_id, 0.0))
            action_seen = has_buy or has_sell
            topic_state = state.setdefault("topic_state", {})
            st = topic_state.get(token_id) or {"phase": "IDLE"}
            phase = st.get("phase", "IDLE")

            if topic_mode:
                if phase == "IDLE" and has_buy:
                    st = {
                        "phase": "LONG",
                        "first_buy_ts": now_ts,
                        "first_sell_ts": 0,
                        "entry_sized": False,
                        "did_probe": False,
                        "target_peak": float(t_now or 0.0),
                        "entry_buy_accum": 0.0,
                        "desired_shares": 0.0,
                    }
                    topic_state[token_id] = st
                    phase = "LONG"
                    logger.info("[TOPIC] ENTER token_id=%s first_buy_ts=%s", token_id, now_ts)

                if phase == "LONG":
                    if t_now is not None:
                        st["target_peak"] = max(
                            float(st.get("target_peak") or 0.0),
                            float(t_now),
                        )
                    if not st.get("entry_sized"):
                        first_buy_ts = int(st.get("first_buy_ts") or now_ts)
                        if now_ts - first_buy_ts <= entry_settle_sec:
                            st["entry_buy_accum"] = float(
                                st.get("entry_buy_accum") or 0.0
                            ) + float(buy_sum)

                if phase == "LONG" and has_sell:
                    st["phase"] = "EXITING"
                    st["first_sell_ts"] = now_ts
                    topic_state[token_id] = st
                    phase = "EXITING"
                    logger.info("[TOPIC] EXIT token_id=%s first_sell_ts=%s", token_id, now_ts)

                if phase == "EXITING":
                    min_order_shares = float(cfg.get("min_order_shares") or 0.0)
                    dust_eps = float(cfg.get("dust_exit_eps") or 0.0)
                    desired_shares = float(st.get("desired_shares") or 0.0)
                    is_dust = False
                    if desired_shares <= eps and my_shares > eps:
                        if dust_eps > 0 and my_shares <= dust_eps:
                            is_dust = True
                        elif min_order_shares > 0 and my_shares < min_order_shares:
                            is_dust = True
                    if is_dust:
                        state.setdefault("dust_exits", {})[token_id] = {
                            "ts": now_ts,
                            "shares": my_shares,
                        }
                        topic_state.pop(token_id, None)
                        phase = "IDLE"
                        logger.info(
                            "[TOPIC] DUST_RESET token_id=%s remaining=%s",
                            token_id,
                            my_shares,
                        )

                if phase == "EXITING" and my_shares <= eps and open_orders_count == 0:
                    topic_state.pop(token_id, None)
                    phase = "IDLE"
                    logger.info("[TOPIC] RESET token_id=%s", token_id)

            is_exiting = phase == "EXITING"
            topic_active = topic_mode and phase in ("LONG", "EXITING")
            probe_attempted = False
            if (not action_seen) and (not t_now_present) and (not topic_active):
                missing_streak += 1
                state.setdefault("target_missing_streak", {})[token_id] = missing_streak
                missing_timeout = (
                    missing_timeout_sec > 0
                    and last_seen_ts > 0
                    and now_ts - last_seen_ts >= missing_timeout_sec
                )
                missing = t_now is None
                if (
                    missing
                    and missing_freeze_streak > 0
                    and missing_streak >= missing_freeze_streak
                ):
                    missing_freeze = state.setdefault("missing_data_freeze", {})
                    existing_freeze = missing_freeze.get(token_id)
                    has_expiring_freeze = isinstance(existing_freeze, dict) and existing_freeze.get(
                        "expires_at"
                    )
                    if not existing_freeze or has_expiring_freeze:
                        if has_expiring_freeze:
                            expires_at = int(existing_freeze.get("expires_at") or 0)
                            if expires_at > 0 and now_ts < expires_at:
                                pass
                            else:
                                existing_freeze = None
                        if not existing_freeze:
                            freeze_min = max(0, missing_freeze_min_sec)
                            freeze_max = max(freeze_min, missing_freeze_max_sec)
                            if freeze_max == freeze_min:
                                freeze_sec = freeze_min
                            else:
                                freeze_sec = random.randint(freeze_min, freeze_max)
                            until_ts = now_ts + freeze_sec
                            missing_freeze[token_id] = {
                                "ts": now_ts,
                                "expires_at": until_ts,
                                "reason": "missing_streak",
                                "streak": missing_streak,
                            }
                            logger.warning(
                                "[FREEZE] token_id=%s reason=missing_streak streak=%s until=%s",
                                token_id,
                                missing_streak,
                                until_ts,
                            )
                should_log_missing = (
                    missing
                    and (my_shares > 0 or open_orders_count > 0)
                    and token_id not in missing_notice_tokens
                )
                if should_log_missing or (token_id in debug_token_ids):
                    legacy_desired = float(cfg.get("follow_ratio") or 0.0) * (
                        t_now or 0.0
                    )
                    logger.info(
                        "[DBG] token_id=%s missing=%s missing_streak=%s t_now=%s t_last=%s "
                        "my_shares=%s open_orders_count=%s",
                        token_id,
                        missing,
                        missing_streak,
                        t_now,
                        t_last,
                        my_shares,
                        open_orders_count,
                    )
                    logger.info("[DBG] token_id=%s legacy_desired=%s", token_id, legacy_desired)
                    if should_log_missing:
                        missing_notice_tokens.add(token_id)
                if open_orders_count > 0 and missing and (
                    missing_timeout or (missing_streak >= orphan_cancel_rounds)
                ):
                    logger.info(
                        "[ORPHAN] token_id=%s missing_streak=%s open_orders=%s",
                        token_id,
                        missing_streak,
                        open_orders_count,
                    )
                    cancel_actions = [
                        {"type": "cancel", "order_id": order.get("order_id")}
                        for order in open_orders
                        if order.get("order_id")
                    ]
                    if cancel_actions:
                        updated_orders = apply_actions(
                            clob_client,
                            cancel_actions,
                            open_orders,
                            now_ts,
                            args.dry_run,
                            cfg=cfg,
                            state=state,
                        )
                        if updated_orders:
                            state.setdefault("open_orders", {})[token_id] = updated_orders
                        else:
                            state.get("open_orders", {}).pop(token_id, None)
                        _prune_order_ts_by_id(state)
                        _refresh_managed_order_ids(state)
                        (
                            planned_total_notional,
                            planned_by_token_usd,
                            order_info_by_id,
                            _shadow_buy_usd,
                        ) = _calc_planned_notional_with_fallback(
                            my_by_token_id,
                            state.get("open_orders", {}),
                            state.get("last_mid_price_by_token_id", {}),
                            max_position_usd_per_token,
                            state,
                            now_ts,
                            shadow_ttl_sec,
                            fallback_mid_price,
                            logger,
                            include_shadow=False,
                        )
                        (
                            planned_total_notional_shadow,
                            planned_by_token_usd_shadow,
                            _shadow_order_info_by_id,
                            _shadow_buy_usd,
                        ) = _calc_planned_notional_with_fallback(
                            my_by_token_id,
                            state.get("open_orders", {}),
                            state.get("last_mid_price_by_token_id", {}),
                            max_position_usd_per_token,
                            state,
                            now_ts,
                            shadow_ttl_sec,
                            fallback_mid_price,
                            logger,
                            include_shadow=True,
                        )
                        (
                            planned_total_notional_shadow,
                            planned_by_token_usd_shadow,
                            _shadow_order_info_by_id,
                            _shadow_buy_usd,
                        ) = _calc_planned_notional_with_fallback(
                            my_by_token_id,
                            state.get("open_orders", {}),
                            state.get("last_mid_price_by_token_id", {}),
                            max_position_usd_per_token,
                            state,
                            now_ts,
                            shadow_ttl_sec,
                            fallback_mid_price,
                            logger,
                            include_shadow=True,
                        )
                        if orphan_ignore_sec > 0:
                            state.setdefault("ignored_tokens", {})[token_id] = {
                                "ts": now_ts,
                                "reason": "missing_orphan_cancel",
                                "expires_at": now_ts + orphan_ignore_sec,
                            }
                continue

            if action_seen:
                state.setdefault("target_missing_streak", {})[token_id] = 0
                # Even if position snapshot temporarily misses t_now, actions mean "recently seen".
                state.setdefault("target_last_seen_ts", {})[token_id] = now_ts
            elif t_now_present:
                state.setdefault("target_missing_streak", {})[token_id] = 0
                state.setdefault("target_last_seen_ts", {})[token_id] = now_ts

            should_update_last = t_now_present
            if t_last is None and (not action_seen) and (not topic_active):
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                should_probe = (
                    bool(state.get("bootstrapped"))
                    and (not probe_blocked_by_boot)
                    and bool(cfg.get("probe_buy_on_first_seen", True))
                    and t_now is not None
                    and float(t_now) > 0
                    and token_id not in set(state.get("probed_token_ids", []))
                    and my_shares <= 0
                )
                has_buy_open = any(
                    str(order.get("side") or "").upper() == "BUY" for order in open_orders or []
                )
                if should_probe and not has_buy_open:
                    if token_id in orderbooks:
                        ob = orderbooks[token_id]
                    else:
                        ob = get_orderbook(clob_client, token_id)
                        orderbooks[token_id] = ob

                    best_bid = ob.get("best_bid")
                    best_ask = ob.get("best_ask")
                    if best_bid is not None and best_ask is not None and best_bid > best_ask:
                        logger.warning(
                            "[SKIP] invalid book bid>ask token_id=%s best_bid=%s best_ask=%s",
                            token_id,
                            best_bid,
                            best_ask,
                        )
                        orderbooks.pop(token_id, None)
                        ob = get_orderbook(clob_client, token_id)
                        orderbooks[token_id] = ob
                        best_bid = ob.get("best_bid")
                        best_ask = ob.get("best_ask")
                        if (
                            best_bid is not None
                            and best_ask is not None
                            and best_bid > best_ask
                        ):
                            continue
                    ref_price = _mid_price(ob)
                    if ref_price is None or ref_price <= 0:
                        logger.warning(
                            "[WARN] 无效盘口(探针): token_id=%s best_bid=%s best_ask=%s",
                            token_id,
                            best_bid,
                            best_ask,
                        )
                        closed_now = _record_orderbook_empty(
                            state,
                            token_id,
                            logger,
                            cfg,
                            now_ts,
                        )
                        if closed_now:
                            logger.info(
                                "[SKIP] closed_by_orderbook_empty token_id=%s",
                                token_id,
                            )
                        continue

                    _clear_orderbook_empty(state, token_id)
                    state.setdefault("last_mid_price_by_token_id", {})[token_id] = float(
                        ref_price
                    )
                    state["last_mid_price_update_ts"] = now_ts
                    is_lowp = _is_lowp_token(cfg, float(ref_price))
                    cfg_lowp = _lowp_cfg(cfg, is_lowp)
                    probe_usd = float(
                        cfg_lowp.get("probe_order_usd")
                        or cfg_lowp.get("min_order_usd")
                        or 5.0
                    )
                    if probe_usd <= 0:
                        probe_usd = float(cfg_lowp.get("min_order_usd") or 5.0)
                    probe_shares = probe_usd / ref_price

                    cap_shares = float("inf")
                    if max_position_usd_per_token > 0:
                        cap_shares = max_position_usd_per_token / ref_price

                    my_target = min(my_shares + probe_shares, cap_shares)
                    delta = my_target - my_shares
                    if delta <= eps:
                        continue

                    desired_side = "BUY"
                    phase_for_intent = phase if topic_mode else "LONG"
                    intent_key = _intent_key(phase_for_intent, desired_side, my_target)
                    intent_changed, desired_down = _update_intent_state(
                        state, token_id, intent_key, eps, logger
                    )
                    open_orders_for_reconcile = open_orders
                    if open_orders and intent_changed:
                        opposite_orders = [
                            order
                            for order in open_orders
                            if str(order.get("side") or "").upper() != desired_side
                        ]
                        same_side_orders = [
                            order
                            for order in open_orders
                            if str(order.get("side") or "").upper() == desired_side
                        ]
                        cancel_actions = []
                        if opposite_orders:
                            cancel_actions.extend(
                                [
                                    {"type": "cancel", "order_id": order.get("order_id")}
                                    for order in opposite_orders
                                    if order.get("order_id")
                                ]
                            )
                        if desired_down or phase_for_intent == "EXITING":
                            cancel_actions.extend(
                                [
                                    {"type": "cancel", "order_id": order.get("order_id")}
                                    for order in same_side_orders
                                    if order.get("order_id")
                                ]
                            )
                        if cancel_actions:
                            logger.info(
                                "[CANCEL_INTENT] token_id=%s opposite=%s same_side=%s",
                                token_id,
                                len(opposite_orders),
                                len(same_side_orders)
                                if (desired_down or phase_for_intent == "EXITING")
                                else 0,
                            )
                            ignore_cd = bool(cfg.get("exit_ignore_cooldown", True)) and is_exiting
                            cancel_ignore_cd = bool(
                                cfg.get("cancel_intent_ignore_cooldown", True)
                            )
                            if cooldown_active and (not ignore_cd) and (not cancel_ignore_cd):
                                logger.info("[SKIP] token_id=%s reason=cooldown_intent", token_id)
                            else:
                                updated_orders = apply_actions(
                                    clob_client,
                                    cancel_actions,
                                    open_orders,
                                    now_ts,
                                    args.dry_run,
                                    cfg=cfg,
                                    state=state,
                                )
                                if updated_orders:
                                    state.setdefault("open_orders", {})[token_id] = updated_orders
                                    open_orders = updated_orders
                                else:
                                    state.get("open_orders", {}).pop(token_id, None)
                                    open_orders = []
                                _prune_order_ts_by_id(state)
                                _refresh_managed_order_ids(state)
                                (
                                    planned_total_notional,
                                    planned_by_token_usd,
                                    order_info_by_id,
                                    _shadow_buy_usd,
                                ) = _calc_planned_notional_with_fallback(
                                    my_by_token_id,
                                    state.get("open_orders", {}),
                                    state.get("last_mid_price_by_token_id", {}),
                                    max_position_usd_per_token,
                                    state,
                                    now_ts,
                                    shadow_ttl_sec,
                                    fallback_mid_price,
                                    logger,
                                    include_shadow=False,
                                )
                                (
                                    planned_total_notional_shadow,
                                    planned_by_token_usd_shadow,
                                    _shadow_order_info_by_id,
                                    _shadow_buy_usd,
                                ) = _calc_planned_notional_with_fallback(
                                    my_by_token_id,
                                    state.get("open_orders", {}),
                                    state.get("last_mid_price_by_token_id", {}),
                                    max_position_usd_per_token,
                                    state,
                                    now_ts,
                                    shadow_ttl_sec,
                                    fallback_mid_price,
                                    logger,
                                    include_shadow=True,
                                )
                                # NOTE: cancel-intent should NOT extend cooldown.
                                # Cooldown is applied only on successful place actions.
                    open_orders_for_reconcile = [
                        order
                        for order in open_orders
                        if str(order.get("side") or "").upper() == desired_side
                    ]

                    token_key = token_key_by_token_id.get(token_id, f"token:{token_id}")
                    cfg_for_reconcile = cfg_lowp if (is_lowp and desired_side == "BUY") else cfg

                    token_planned = float(planned_by_token_usd_shadow.get(token_id, 0.0))

                    if desired_side == "BUY":
                        max_notional = float(cfg_for_reconcile.get("max_notional_per_token") or 0.0)
                        if max_notional > 0 and token_planned >= max_notional * 0.95:
                            logger.debug(
                                "[SKIP_PREFLIGHT] %s near_limit planned=%s max=%s",
                                token_key,
                                token_planned,
                                max_notional,
                            )
                            continue

                    actions = reconcile_one(
                        token_id,
                        my_target,
                        my_shares,
                        ob,
                        open_orders_for_reconcile,
                        now_ts,
                        cfg_for_reconcile,
                        state,
                        planned_token_notional=token_planned,
                    )
                    if not actions:
                        continue
                    filtered_actions = []
                    blocked_reasons: set[str] = set()
                    has_any_place = any(a.get("type") == "place" for a in actions)
                    pending_cancel_actions = []
                    pending_cancel_usd = 0.0
                    token_planned_before = float(planned_by_token_usd.get(token_id, 0.0))
                    token_planned_before_shadow = float(
                        planned_by_token_usd_shadow.get(token_id, 0.0)
                    )
                    # Track accumulator delta within this batch to prevent batch bypass
                    local_accumulator_delta = 0.0

                    for act in actions:
                        act_type = act.get("type")
                        if act_type == "cancel":
                            order_id = str(act.get("order_id") or "")
                            info = order_info_by_id.get(order_id)
                            if info and info.get("side") == "BUY":
                                usd = float(info.get("usd") or 0.0)
                                pending_cancel_actions.append(act)
                                pending_cancel_usd += usd
                                planned_total_notional -= usd
                                planned_total_notional_shadow -= usd
                                planned_by_token_usd[token_id] = max(
                                    0.0, planned_by_token_usd.get(token_id, 0.0) - usd
                                )
                                planned_by_token_usd_shadow[token_id] = max(
                                    0.0, planned_by_token_usd_shadow.get(token_id, 0.0) - usd
                                )
                            else:
                                filtered_actions.append(act)
                            continue

                        if act_type != "place":
                            filtered_actions.append(act)
                            continue

                        side = str(act.get("side") or "").upper()
                        price = float(act.get("price") or ref_price or 0.0)
                        size = float(act.get("size") or 0.0)
                        if price <= 0 or size <= 0:
                            continue
                        if my_trades_unreliable and side == "BUY":
                            blocked_reasons.add("my_trades_unreliable")
                            continue
                        if side == "BUY" and buy_window_sec > 0:
                            order_notional = abs(size) * price
                            recent_token = float(recent_buy_by_token.get(token_id, 0.0))
                            if (
                                buy_window_max_usd_per_token > 0
                                and recent_token + order_notional > buy_window_max_usd_per_token
                            ):
                                blocked_reasons.add("buy_window_max_usd_per_token")
                                continue
                            if (
                                buy_window_max_usd_total > 0
                                and recent_buy_total + order_notional > buy_window_max_usd_total
                            ):
                                blocked_reasons.add("buy_window_max_usd_total")
                                continue

                        # CRITICAL: Check accumulator first (independent of position API)
                        if side == "BUY":
                            order_notional = abs(size) * price
                            cfg_for_acc = cfg_lowp if is_lowp else cfg
                            planned_token_notional_for_acc = float(planned_by_token_usd.get(token_id, 0.0))
                            acc_ok, acc_reason, acc_available = accumulator_check(
                                token_id,
                                order_notional,
                                state,
                                cfg_for_acc,
                                side=side,
                                local_delta=local_accumulator_delta,
                                planned_token_notional=planned_token_notional_for_acc,
                            )
                            if not acc_ok:
                                # Get accumulator actual values for detailed logging
                                accumulator = state.get("buy_notional_accumulator")
                                acc_current = 0.0
                                if isinstance(accumulator, dict):
                                    token_acc = accumulator.get(token_id)
                                    if isinstance(token_acc, dict):
                                        acc_current = float(token_acc.get("usd", 0.0))
                                max_per_token = float(cfg_for_acc.get("max_notional_per_token") or 0)

                                # Try to shrink order to fit within accumulator limit
                                if acc_available > 0:
                                    min_order_usd = float(cfg_for_acc.get("min_order_usd") or 0.0)
                                    min_order_shares = float(cfg_for_acc.get("min_order_shares") or 0.0)
                                    effective_min_usd = max(min_order_usd, min_order_shares * price)

                                    if acc_available >= effective_min_usd:
                                        # Shrink order to available amount
                                        old_size = size
                                        old_usd = order_notional
                                        size = acc_available / price
                                        act["size"] = size
                                        order_notional = acc_available
                                        logger.warning(
                                            "[ACCUMULATOR_SHRINK] token_id=%s old_usd=%s new_usd=%s acc_current=%s acc_limit=%s planned_token=%s is_lowp=%s reason=%s",
                                            token_id,
                                            old_usd,
                                            acc_available,
                                            acc_current,
                                            max_per_token,
                                            planned_token_notional_for_acc,
                                            is_lowp,
                                            acc_reason,
                                        )
                                        # Continue with shrunken order (don't skip)
                                    else:
                                        # Available amount is below minimum order size
                                        logger.warning(
                                            "[ACCUMULATOR_BLOCK] token_id=%s order_usd=%s acc_current=%s local_delta=%s acc_limit=%s acc_available=%s min_usd=%s planned_token=%s is_lowp=%s reason=%s",
                                            token_id,
                                            order_notional,
                                            acc_current,
                                            local_accumulator_delta,
                                            max_per_token,
                                            acc_available,
                                            effective_min_usd,
                                            planned_token_notional_for_acc,
                                            is_lowp,
                                            acc_reason,
                                        )
                                        blocked_reasons.add(acc_reason or "accumulator_check")
                                        continue
                                else:
                                    # No room available
                                    logger.warning(
                                        "[ACCUMULATOR_BLOCK] token_id=%s order_usd=%s acc_current=%s local_delta=%s acc_limit=%s planned_token=%s is_lowp=%s reason=%s",
                                        token_id,
                                        order_notional,
                                        acc_current,
                                        local_accumulator_delta,
                                        max_per_token,
                                        planned_token_notional_for_acc,
                                        is_lowp,
                                        acc_reason,
                                    )
                                    blocked_reasons.add(acc_reason or "accumulator_check")
                                    continue

                        planned_token_notional = float(planned_by_token_usd.get(token_id, 0.0))
                        planned_token_notional_shadow = float(
                            planned_by_token_usd_shadow.get(token_id, 0.0)
                        )
                        planned_total_notional_risk = max(
                            planned_total_notional, planned_total_notional_shadow
                        )
                        planned_token_notional_risk = max(
                            planned_token_notional, planned_token_notional_shadow
                        )

                        # CRITICAL ALERT: Detect position sync anomaly
                        # If my_shares=0 but shadow shows significant recent orders, position may not be synced
                        if (
                            side == "BUY"
                            and my_shares <= 0.0
                            and planned_token_notional_shadow > 2.0
                            and planned_token_notional_shadow > planned_token_notional + 1.0
                        ):
                            logger.warning(
                                "[ALERT] position_sync_anomaly detected: my_shares=%.2f but "
                                "shadow_notional=%.2f (planned=%.2f) token=%s - position may not be synced, "
                                "risk baseline weakened",
                                my_shares,
                                planned_token_notional_shadow,
                                planned_token_notional,
                                token_id,
                            )

                        cfg_for_action = cfg_lowp if (is_lowp and side == "BUY") else cfg
                        ok, reason = risk_check(
                            token_key,
                            size,
                            my_shares,
                            price,
                            cfg_for_action,
                            token_title=token_title,
                            side=side,
                            planned_total_notional=planned_total_notional_risk,
                            planned_token_notional=planned_token_notional_risk,
                            cumulative_total_usd=None,
                            cumulative_token_usd=None,
                        )
                        if not ok:
                            resized = _shrink_on_risk_limit(
                                act,
                                max_notional_total,
                                planned_total_notional_risk,
                                float(cfg_for_action.get("max_notional_per_token") or 0.0),
                                planned_token_notional_risk,
                                float(cfg_for_action.get("min_order_usd") or 0.0),
                                float(cfg_for_action.get("min_order_shares") or 0.0),
                                token_key,
                                token_id,
                                logger,
                            )
                            if resized is None:
                                if has_any_place and pending_cancel_actions:
                                    planned_total_notional += pending_cancel_usd
                                    planned_by_token_usd[token_id] = token_planned_before
                                    planned_total_notional_shadow += pending_cancel_usd
                                    planned_by_token_usd_shadow[token_id] = (
                                        token_planned_before_shadow
                                    )
                                    pending_cancel_actions = []
                                    pending_cancel_usd = 0.0
                                blocked_reasons.add(reason or "risk_check")
                                continue

                            act, allowed_usd = resized
                            price = float(act.get("price") or 0.0)
                            size = float(act.get("size") or 0.0)

                            # CRITICAL: Re-check accumulator after shrink
                            shrink_notional = abs(size) * price
                            acc_ok_shrink, acc_reason_shrink, _acc_available_shrink = (
                                accumulator_check(
                                    token_id,
                                    shrink_notional,
                                    state,
                                    cfg_for_action,
                                    side=side,
                                    local_delta=local_accumulator_delta,
                                )
                            )
                            if not acc_ok_shrink:
                                logger.warning(
                                    "[ACCUMULATOR_BLOCK_SHRINK] token_id=%s shrink_usd=%s "
                                    "current_delta=%s reason=%s",
                                    token_id,
                                    shrink_notional,
                                    local_accumulator_delta,
                                    acc_reason_shrink,
                                )
                                if has_any_place and pending_cancel_actions:
                                    planned_total_notional += pending_cancel_usd
                                    planned_by_token_usd[token_id] = token_planned_before
                                    planned_total_notional_shadow += pending_cancel_usd
                                    planned_by_token_usd_shadow[token_id] = (
                                        token_planned_before_shadow
                                    )
                                    pending_cancel_actions = []
                                    pending_cancel_usd = 0.0
                                blocked_reasons.add(acc_reason_shrink or "accumulator_check_shrink")
                                continue

                            planned_token_notional = float(planned_by_token_usd.get(token_id, 0.0))
                            planned_token_notional_shadow = float(
                                planned_by_token_usd_shadow.get(token_id, 0.0)
                            )
                            planned_total_notional_risk = max(
                                planned_total_notional, planned_total_notional_shadow
                            )
                            planned_token_notional_risk = max(
                                planned_token_notional, planned_token_notional_shadow
                            )
                            ok2, reason2 = risk_check(
                                token_key,
                                size,
                                my_shares,
                                price,
                                cfg_for_action,
                                token_title=token_title,
                                side=side,
                                planned_total_notional=planned_total_notional_risk,
                                planned_token_notional=planned_token_notional_risk,
                                cumulative_total_usd=None,
                                cumulative_token_usd=None,
                            )
                            if not ok2:
                                if has_any_place and pending_cancel_actions:
                                    planned_total_notional += pending_cancel_usd
                                    planned_by_token_usd[token_id] = token_planned_before
                                    planned_total_notional_shadow += pending_cancel_usd
                                    planned_by_token_usd_shadow[token_id] = (
                                        token_planned_before_shadow
                                    )
                                    pending_cancel_actions = []
                                    pending_cancel_usd = 0.0
                                blocked_reasons.add(reason2 or reason or "risk_check")
                                continue

                        if pending_cancel_actions:
                            filtered_actions.extend(pending_cancel_actions)
                            pending_cancel_actions = []
                            pending_cancel_usd = 0.0

                        filtered_actions.append(act)
                        if side == "BUY":
                            usd = abs(size) * price
                            planned_total_notional += usd
                            planned_total_notional_shadow += usd
                            planned_by_token_usd[token_id] = (
                                planned_by_token_usd.get(token_id, 0.0) + usd
                            )
                            planned_by_token_usd_shadow[token_id] = (
                                planned_by_token_usd_shadow.get(token_id, 0.0) + usd
                            )
                            # CRITICAL: Update local accumulator delta to prevent batch bypass
                            local_accumulator_delta += usd

                    if has_any_place and pending_cancel_actions:
                        planned_total_notional += pending_cancel_usd
                        planned_by_token_usd[token_id] = token_planned_before
                        planned_total_notional_shadow += pending_cancel_usd
                        planned_by_token_usd_shadow[token_id] = token_planned_before_shadow
                        pending_cancel_actions = []
                        pending_cancel_usd = 0.0
                    elif (not has_any_place) and pending_cancel_actions:
                        filtered_actions.extend(pending_cancel_actions)

                    if not filtered_actions:
                        if has_any_place:
                            reason_text = (
                                ",".join(sorted(blocked_reasons))
                                if blocked_reasons
                                else "risk_check"
                            )
                            logger.info("[NOOP] token_id=%s reason=%s", token_id, reason_text)
                        continue
                    actions = filtered_actions
                    logger.info("[ACTION] token_id=%s -> %s", token_id, actions)

                    is_reprice = _is_pure_reprice(actions)
                    missing_freeze = state.setdefault("missing_data_freeze", {})
                    if not missing_data and token_id:
                        state.get("missing_buy_attempts", {}).pop(token_id, None)
                        cap_limit = min(cap_shares, cap_shares_notional)
                        existing_freeze = missing_freeze.get(token_id)
                        active_streak_freeze = (
                            isinstance(existing_freeze, dict)
                            and existing_freeze.get("reason") == "missing_streak"
                            and int(existing_freeze.get("expires_at") or 0) > now_ts
                        )
                        if not active_streak_freeze:
                            if my_shares <= cap_limit + eps:
                                missing_freeze.pop(token_id, None)
                            else:
                                missing_freeze[token_id] = {
                                    "ts": now_ts,
                                    "shares": my_shares,
                                    "cap": cap_limit,
                                    "reason": "position_exceeds_cap",
                                }
                                logger.warning(
                                    "[FREEZE] token_id=%s reason=position_exceeds_cap shares=%s cap=%s",
                                    token_id,
                                    my_shares,
                                    cap_limit,
                                )
                        elif my_shares > cap_limit + eps:
                            existing_freeze["shares"] = my_shares
                            existing_freeze["cap"] = cap_limit
                    if token_id and token_id in missing_freeze and any(
                        act.get("type") == "place"
                        and str(act.get("side") or "").upper() == "BUY"
                        for act in actions
                    ):
                        logger.warning(
                            "[SKIP] token_id=%s reason=missing_data_freeze",
                            token_id,
                        )
                        continue
                    if missing_data and any(
                        act.get("type") == "place"
                        and str(act.get("side") or "").upper() == "BUY"
                        for act in actions
                    ):
                        missing_limit = int(cfg.get("max_missing_buy_attempts") or 0)
                        if missing_limit <= 0:
                            logger.warning(
                                "[SKIP] token_id=%s reason=missing_data_buy_block limit=%s",
                                token_id,
                                missing_limit,
                            )
                            continue
                        missing_counts = state.setdefault("missing_buy_attempts", {})
                        missing_counts[token_id] = int(missing_counts.get(token_id) or 0) + 1
                        if missing_counts[token_id] > missing_limit:
                            logger.warning(
                                "[SKIP] token_id=%s reason=missing_data_buy_limit count=%s "
                                "limit=%s",
                                token_id,
                                missing_counts[token_id],
                                missing_limit,
                            )
                            continue
                    if place_backoff_active and any(
                        act.get("type") == "place" for act in actions
                    ):
                        logger.info(
                            "[SKIP] token_id=%s reason=place_backoff until=%s",
                            token_id,
                            place_fail_until,
                        )
                        continue
                    ignore_cd = bool(cfg.get("exit_ignore_cooldown", True)) and is_exiting
                    if cooldown_active and (not ignore_cd) and (not is_reprice):
                        logger.info("[SKIP] token_id=%s reason=cooldown", token_id)
                        continue

                    updated_orders = apply_actions(
                        clob_client,
                        actions,
                        open_orders,
                        now_ts,
                        args.dry_run,
                        cfg=cfg,
                        state=state,
                        planned_by_token_usd=planned_by_token_usd_shadow,
                    )
                    if updated_orders:
                        state.setdefault("open_orders", {})[token_id] = updated_orders
                    else:
                        state.get("open_orders", {}).pop(token_id, None)
                    _prune_order_ts_by_id(state)
                    _refresh_managed_order_ids(state)
                    (
                        planned_total_notional,
                        planned_by_token_usd,
                        order_info_by_id,
                        _shadow_buy_usd,
                    ) = _calc_planned_notional_with_fallback(
                        my_by_token_id,
                        state.get("open_orders", {}),
                        state.get("last_mid_price_by_token_id", {}),
                        max_position_usd_per_token,
                        state,
                        now_ts,
                        shadow_ttl_sec,
                        fallback_mid_price,
                        logger,
                        include_shadow=False,
                    )
                    (
                        planned_total_notional_shadow,
                        planned_by_token_usd_shadow,
                        _shadow_order_info_by_id,
                        _shadow_buy_usd,
                    ) = _calc_planned_notional_with_fallback(
                        my_by_token_id,
                        state.get("open_orders", {}),
                        state.get("last_mid_price_by_token_id", {}),
                        max_position_usd_per_token,
                        state,
                        now_ts,
                        shadow_ttl_sec,
                        fallback_mid_price,
                        logger,
                        include_shadow=True,
                    )

                    has_any_place_final = any(
                        act.get("type") == "place" for act in actions
                    )
                    if (
                        cooldown_sec > 0
                        and actions
                        and has_any_place_final
                        and (not ignore_cd)
                        and (not is_reprice)
                    ):
                        state.setdefault("cooldown_until", {})[token_id] = (
                            now_ts + cooldown_sec
                        )
                    if is_reprice:
                        state.setdefault("last_reprice_ts_by_token", {})[token_id] = now_ts

                    probed = set(state.get("probed_token_ids", []))
                    probed.add(token_id)
                    state["probed_token_ids"] = sorted(probed)
                continue

            if t_now is None and not action_seen and not topic_active:
                continue

            if t_now is None:
                action_delta = buy_sum - sell_sum
                if action_seen and abs(action_delta) > eps:
                    d_target = action_delta
                else:
                    if not topic_active:
                        continue
                    d_target = 0.0
            elif t_last is None:
                d_target = float(t_now)
            else:
                d_target = float(t_now) - float(t_last)
            topic_active = topic_mode and phase in ("LONG", "EXITING")
            actions_unreliable_until = int(state.get("actions_unreliable_until") or 0)
            actions_unreliable = actions_unreliable_until > now_ts
            if has_sell and d_target >= -eps:
                d_target = -max(sell_sum, eps)
                logger.info(
                    "[SIGNAL] SELL forced_by_action token_id=%s d_target=%s sell_sum=%s",
                    token_id,
                    d_target,
                    sell_sum,
                )
            if d_target < -eps:
                if has_sell:
                    state.setdefault("sell_confirm", {}).pop(token_id, None)
                else:
                    sell_confirm = state.setdefault("sell_confirm", {})
                    token_confirm = sell_confirm.get(token_id) or {"count": 0, "first_ts": now_ts}
                    if actions_unreliable:
                        token_confirm["first_ts"] = now_ts
                        sell_confirm[token_id] = token_confirm
                        state["actions_replay_from_ms"] = max(
                            0, now_ms - actions_replay_window_sec * 1000
                        )
                        logger.info(
                            "[HOLD] token_id=%s reason=actions_unreliable d_target=%s confirm=%s/%s replay_from_ms=%s",
                            token_id,
                            d_target,
                            token_confirm.get("count"),
                            sell_confirm_max,
                            state.get("actions_replay_from_ms"),
                        )
                        d_target = 0.0
                    else:
                        if now_ts - int(token_confirm.get("first_ts") or now_ts) > sell_confirm_window_sec:
                            token_confirm = {"count": 0, "first_ts": now_ts}
                        token_confirm["count"] = int(token_confirm.get("count") or 0) + 1
                        token_confirm["first_ts"] = int(token_confirm.get("first_ts") or now_ts)
                        sell_confirm[token_id] = token_confirm
                        if token_confirm["count"] < sell_confirm_max:
                            state["actions_replay_from_ms"] = max(
                                0, now_ms - actions_replay_window_sec * 1000
                            )
                            logger.info(
                                "[HOLD] token_id=%s reason=no_sell_action d_target=%s confirm=%s/%s replay_from_ms=%s",
                                token_id,
                                d_target,
                                token_confirm["count"],
                                sell_confirm_max,
                                state.get("actions_replay_from_ms"),
                            )
                            d_target = 0.0
                        else:
                            drop_shares = max(0.0, -float(d_target))
                            base_shares = max(0.0, float(t_last or 0.0))
                            drop_threshold = 0.0
                            if sell_confirm_force_ratio > 0 and base_shares > 0:
                                drop_threshold = max(
                                    drop_threshold, base_shares * sell_confirm_force_ratio
                                )
                            if sell_confirm_force_shares > 0:
                                drop_threshold = max(
                                    drop_threshold, sell_confirm_force_shares
                                )
                            significant_drop = drop_threshold > 0 and drop_shares >= drop_threshold
                            if significant_drop:
                                logger.info(
                                    "[FORCE] token_id=%s reason=sell_confirm_drop d_target=%s drop=%s threshold=%s ratio=%s base=%s",
                                    token_id,
                                    d_target,
                                    drop_shares,
                                    drop_threshold,
                                    sell_confirm_force_ratio,
                                    base_shares,
                                )
                                sell_confirm.pop(token_id, None)
                            else:
                                logger.info(
                                    "[HOLD] token_id=%s reason=no_sell_after_confirm d_target=%s confirm=%s/%s drop=%s threshold=%s",
                                    token_id,
                                    d_target,
                                    token_confirm["count"],
                                    sell_confirm_max,
                                    drop_shares,
                                    drop_threshold,
                                )
                                token_confirm["count"] = sell_confirm_max
                                sell_confirm[token_id] = token_confirm
                                d_target = 0.0
            else:
                state.setdefault("sell_confirm", {}).pop(token_id, None)
            if abs(d_target) <= eps and not topic_active:
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue

            if token_id in orderbooks:
                ob = orderbooks[token_id]
            else:
                ob = get_orderbook(clob_client, token_id)
                orderbooks[token_id] = ob

            best_bid = ob.get("best_bid")
            best_ask = ob.get("best_ask")
            if best_bid is not None and best_ask is not None and best_bid > best_ask:
                logger.warning(
                    "[SKIP] invalid book bid>ask token_id=%s best_bid=%s best_ask=%s",
                    token_id,
                    best_bid,
                    best_ask,
                )
                orderbooks.pop(token_id, None)
                ob = get_orderbook(clob_client, token_id)
                orderbooks[token_id] = ob
                best_bid = ob.get("best_bid")
                best_ask = ob.get("best_ask")
                if best_bid is not None and best_ask is not None and best_bid > best_ask:
                    continue
            ref_price = _mid_price(ob)
            if ref_price is None or ref_price <= 0:
                logger.warning(
                    "[WARN] 无效盘口: token_id=%s best_bid=%s best_ask=%s",
                    token_id,
                    best_bid,
                    best_ask,
                )
                closed_now = _record_orderbook_empty(
                    state,
                    token_id,
                    logger,
                    cfg,
                    now_ts,
                )
                if closed_now:
                    logger.info(
                        "[SKIP] closed_by_orderbook_empty token_id=%s",
                        token_id,
                    )
                logger.info("[NOOP] token_id=%s reason=orderbook_empty", token_id)
                continue
            _clear_orderbook_empty(state, token_id)
            state.setdefault("last_mid_price_by_token_id", {})[token_id] = float(ref_price)
            state["last_mid_price_update_ts"] = now_ts
            is_lowp = _is_lowp_token(cfg, float(ref_price))
            cfg_lowp = _lowp_cfg(cfg, is_lowp)
            ratio_base = float(cfg.get("follow_ratio") or 0.0)
            ratio_buy = _lowp_buy_ratio(cfg, is_lowp)
            if is_lowp and (t_now is not None) and (t_last is not None):
                if float(t_now) - float(t_last) > 0:
                    logger.info(
                        "[LOWP] token_id=%s ref_price=%.4f ratio=%.4f->%.4f "
                        "cap_token=%.2f->%.2f min/max_usd=%s/%s",
                        token_id,
                        float(ref_price),
                        ratio_base,
                        ratio_buy,
                        float(cfg.get("max_notional_per_token") or 0.0),
                        float(cfg_lowp.get("max_notional_per_token") or 0.0),
                        cfg_lowp.get("min_order_usd"),
                        cfg_lowp.get("max_order_usd"),
                    )

            cap_shares = float("inf")
            if max_position_usd_per_token > 0:
                cap_shares = max_position_usd_per_token / ref_price

            max_notional_per_token = float(
                cfg_lowp.get("max_notional_per_token") or cfg.get("max_notional_per_token") or 0.0
            )
            cap_shares_notional = (
                (max_notional_per_token / ref_price) if max_notional_per_token > 0 else float("inf")
            )

            use_ratio = ratio_buy if d_target > 0 else ratio_base
            d_my = use_ratio * d_target
            if d_target > 0:
                logger.info(
                    "[SIGNAL] BUY token_id=%s d_target=%s d_my=%s my_shares=%s",
                    token_id,
                    d_target,
                    d_my,
                    my_shares,
                )
            elif d_target < 0:
                logger.info(
                    "[SIGNAL] SELL token_id=%s d_target=%s d_my=%s my_shares=%s",
                    token_id,
                    d_target,
                    d_my,
                    my_shares,
                )
            my_target = my_shares + d_my
            if my_target < 0:
                my_target = 0.0
            if d_target > 0:
                my_target = min(my_target, cap_shares, cap_shares_notional)
            else:
                if my_target > cap_shares:
                    my_target = cap_shares

            if topic_active:
                probe_usd = float(
                    cfg_lowp.get("probe_order_usd")
                    or cfg_lowp.get("min_order_usd")
                    or 5.0
                )
                probe_shares = probe_usd / ref_price

                if phase == "LONG":
                    if not st.get("did_probe") and my_shares <= eps:
                        my_target = min(cap_shares, cap_shares_notional, my_shares + probe_shares)
                        probe_attempted = True
                        logger.info("[TOPIC] PROBE token_id=%s target=%s", token_id, my_target)

                    if not st.get("entry_sized"):
                        first_buy_ts = int(st.get("first_buy_ts") or now_ts)
                        if now_ts - first_buy_ts >= entry_settle_sec:
                            base = float(t_now) if t_now is not None else float(
                                st.get("target_peak") or 0.0
                            )
                            ratio = ratio_buy
                            desired = 0.0
                            if base > 0 and ratio > 0:
                                desired = min(cap_shares, cap_shares_notional, ratio * base)
                            desired = max(
                                desired,
                                min(cap_shares, cap_shares_notional, my_shares + probe_shares),
                            )
                            st["desired_shares"] = float(desired)
                            st["entry_sized"] = True
                            topic_state[token_id] = st
                            logger.info(
                                "[TOPIC] SIZE token_id=%s desired=%s base=%s",
                                token_id,
                                desired,
                                base,
                            )

                    base = float(t_now) if t_now is not None else float(
                        st.get("target_peak") or 0.0
                    )
                    desired_locked = float(st.get("desired_shares") or 0.0)
                    desired_target = desired_locked
                    if base > 0 and ratio_buy > 0:
                        desired_target = min(cap_shares, cap_shares_notional, ratio_buy * base)
                    if desired_target > 0:
                        st["desired_shares"] = float(desired_target)
                        topic_state[token_id] = st
                        my_target = max(my_shares, min(cap_shares, cap_shares_notional, desired_target))

                elif phase == "EXITING":
                    my_target = 0.0

            # Guard: if target snapshot temporarily misses this token, don't drop desired to 0
            # while we still have outstanding orders (prevents churn & "stuck probe" at stale price).
            if phase == "LONG" and (t_now is None) and (not action_seen) and open_orders_count > 0:
                hold_sec = int(cfg.get("missing_hold_sec") or entry_settle_sec or 60)
                last_seen = int(state.get("target_last_seen_ts", {}).get(token_id) or 0)
                if last_seen > 0 and (now_ts - last_seen) <= hold_sec:
                    prev_intent_tmp = state.get("intent_keys", {}).get(token_id)
                    prev_desired = (
                        float(prev_intent_tmp.get("desired_shares") or 0.0)
                        if isinstance(prev_intent_tmp, dict)
                        else 0.0
                    )
                    if prev_desired > my_target + eps:
                        logger.info(
                            "[HOLD] token_id=%s reason=missing_target prev_desired=%s "
                            "my_target=%s last_seen=%s hold_sec=%s",
                            token_id,
                            prev_desired,
                            my_target,
                            last_seen,
                            hold_sec,
                        )
                        my_target = min(cap_shares, cap_shares_notional, prev_desired)
            delta = my_target - my_shares
            prev_intent = state.get("intent_keys", {}).get(token_id)
            if delta > eps:
                desired_side = "BUY"
            elif delta < -eps:
                desired_side = "SELL"
            elif isinstance(prev_intent, dict) and prev_intent.get("desired_side"):
                desired_side = str(prev_intent.get("desired_side")).upper()
            else:
                desired_side = "BUY"
            phase_for_intent = phase if topic_mode else ("LONG" if desired_side == "BUY" else "EXITING")
            intent_key = _intent_key(phase_for_intent, desired_side, my_target)
            intent_changed, desired_down = _update_intent_state(
                state, token_id, intent_key, eps, logger
            )
            if open_orders and intent_changed:
                opposite_orders = [
                    order
                    for order in open_orders
                    if str(order.get("side") or "").upper() != desired_side
                ]
                same_side_orders = [
                    order
                    for order in open_orders
                    if str(order.get("side") or "").upper() == desired_side
                ]
                cancel_actions = []
                if opposite_orders:
                    cancel_actions.extend(
                        [
                            {"type": "cancel", "order_id": order.get("order_id")}
                            for order in opposite_orders
                            if order.get("order_id")
                        ]
                    )
                if desired_down or phase_for_intent == "EXITING":
                    cancel_actions.extend(
                        [
                            {"type": "cancel", "order_id": order.get("order_id")}
                            for order in same_side_orders
                            if order.get("order_id")
                        ]
                    )
                if cancel_actions:
                    logger.info(
                        "[CANCEL_INTENT] token_id=%s opposite=%s same_side=%s",
                        token_id,
                        len(opposite_orders),
                        len(same_side_orders)
                        if (desired_down or phase_for_intent == "EXITING")
                        else 0,
                    )
                    ignore_cd = bool(cfg.get("exit_ignore_cooldown", True)) and is_exiting
                    cancel_ignore_cd = bool(
                        cfg.get("cancel_intent_ignore_cooldown", True)
                    )
                    if cooldown_active and (not ignore_cd) and (not cancel_ignore_cd):
                        logger.info("[SKIP] token_id=%s reason=cooldown_intent", token_id)
                    else:
                        updated_orders = apply_actions(
                            clob_client,
                            cancel_actions,
                            open_orders,
                            now_ts,
                            args.dry_run,
                            cfg=cfg,
                            state=state,
                        )
                        if updated_orders:
                            state.setdefault("open_orders", {})[token_id] = updated_orders
                            open_orders = updated_orders
                        else:
                            state.get("open_orders", {}).pop(token_id, None)
                            open_orders = []
                        _prune_order_ts_by_id(state)
                        _refresh_managed_order_ids(state)
                        (
                            planned_total_notional,
                            planned_by_token_usd,
                            order_info_by_id,
                            _shadow_buy_usd,
                        ) = _calc_planned_notional_with_fallback(
                            my_by_token_id,
                            state.get("open_orders", {}),
                            state.get("last_mid_price_by_token_id", {}),
                            max_position_usd_per_token,
                            state,
                            now_ts,
                            shadow_ttl_sec,
                            fallback_mid_price,
                            logger,
                            include_shadow=False,
                        )
                        # NOTE: cancel-intent should NOT extend cooldown.
                        # Cooldown is applied only on successful place actions.
            if abs(delta) <= eps:
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue
            open_orders_for_reconcile = [
                order
                for order in open_orders
                if str(order.get("side") or "").upper() == desired_side
            ]
            deadband_shares = float(cfg.get("deadband_shares") or 0.0)
            if abs(delta) <= deadband_shares and not open_orders_for_reconcile:
                logger.info(
                    "[NOOP] token_id=%s reason=deadband delta=%s deadband=%s",
                    token_id,
                    delta,
                    deadband_shares,
                )
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue

            state.setdefault("target_last_event_ts", {})[token_id] = now_ts

            if mode == "auto_usd":
                delta_shares = abs(my_target - my_shares)
                delta_usd_samples.append(delta_shares * ref_price)

            token_key = token_key_by_token_id.get(token_id, f"token:{token_id}")
            cfg_for_reconcile = cfg_lowp if (is_lowp and desired_side == "BUY") else cfg

            token_planned = float(planned_by_token_usd_shadow.get(token_id, 0.0))

            if desired_side == "BUY":
                max_notional = float(cfg_for_reconcile.get("max_notional_per_token") or 0.0)
                if max_notional > 0 and token_planned >= max_notional * 0.95:
                    logger.debug(
                        "[SKIP_PREFLIGHT] %s near_limit planned=%s max=%s",
                        token_key,
                        token_planned,
                        max_notional,
                    )
                    _maybe_update_target_last(state, token_id, t_now, should_update_last)
                    continue

            actions = reconcile_one(
                token_id,
                my_target,
                my_shares,
                ob,
                open_orders_for_reconcile,
                now_ts,
                cfg_for_reconcile,
                state,
                planned_token_notional=token_planned,
            )
            if not actions:
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue
            filtered_actions = []
            blocked_reasons: set[str] = set()
            has_any_place = any(a.get("type") == "place" for a in actions)
            pending_cancel_actions = []
            pending_cancel_usd = 0.0
            token_planned_before = float(planned_by_token_usd.get(token_id, 0.0))
            # Track accumulator delta within this batch to prevent batch bypass
            local_accumulator_delta = 0.0

            for act in actions:
                act_type = act.get("type")
                if act_type == "cancel":
                    order_id = str(act.get("order_id") or "")
                    info = order_info_by_id.get(order_id)
                    if info and info.get("side") == "BUY":
                        usd = float(info.get("usd") or 0.0)
                        pending_cancel_actions.append(act)
                        pending_cancel_usd += usd
                        planned_total_notional -= usd
                        planned_by_token_usd[token_id] = max(
                            0.0, planned_by_token_usd.get(token_id, 0.0) - usd
                        )
                    else:
                        filtered_actions.append(act)
                    continue

                if act_type != "place":
                    filtered_actions.append(act)
                    continue

                side = str(act.get("side") or "").upper()
                price = float(act.get("price") or ref_price or 0.0)
                size = float(act.get("size") or 0.0)
                if price <= 0 or size <= 0:
                    continue
                if my_trades_unreliable and side == "BUY":
                    blocked_reasons.add("my_trades_unreliable")
                    continue
                if side == "BUY" and buy_window_sec > 0:
                    order_notional = abs(size) * price
                    recent_token = float(recent_buy_by_token.get(token_id, 0.0))
                    if (
                        buy_window_max_usd_per_token > 0
                        and recent_token + order_notional > buy_window_max_usd_per_token
                    ):
                        blocked_reasons.add("buy_window_max_usd_per_token")
                        continue
                    if (
                        buy_window_max_usd_total > 0
                        and recent_buy_total + order_notional > buy_window_max_usd_total
                    ):
                        blocked_reasons.add("buy_window_max_usd_total")
                        continue

                # CRITICAL: Check accumulator first (independent of position API)
                if side == "BUY":
                    order_notional = abs(size) * price
                    cfg_for_acc = cfg_lowp if is_lowp else cfg
                    planned_token_notional_for_acc = float(planned_by_token_usd.get(token_id, 0.0))
                    acc_ok, acc_reason, acc_available = accumulator_check(
                        token_id,
                        order_notional,
                        state,
                        cfg_for_acc,
                        side=side,
                        local_delta=local_accumulator_delta,
                        planned_token_notional=planned_token_notional_for_acc,
                    )
                    if not acc_ok:
                        # Get accumulator actual values for detailed logging
                        accumulator = state.get("buy_notional_accumulator")
                        acc_current = 0.0
                        if isinstance(accumulator, dict):
                            token_acc = accumulator.get(token_id)
                            if isinstance(token_acc, dict):
                                acc_current = float(token_acc.get("usd", 0.0))
                        max_per_token = float(cfg_for_acc.get("max_notional_per_token") or 0)

                        # Try to shrink order to fit within accumulator limit
                        if acc_available > 0:
                            min_order_usd = float(cfg_for_acc.get("min_order_usd") or 0.0)
                            min_order_shares = float(cfg_for_acc.get("min_order_shares") or 0.0)
                            effective_min_usd = max(min_order_usd, min_order_shares * price)

                            if acc_available >= effective_min_usd:
                                # Shrink order to available amount
                                old_size = size
                                old_usd = order_notional
                                size = acc_available / price
                                act["size"] = size
                                order_notional = acc_available
                                logger.warning(
                                    "[ACCUMULATOR_SHRINK] token_id=%s old_usd=%s new_usd=%s acc_current=%s acc_limit=%s planned_token=%s is_lowp=%s reason=%s",
                                    token_id,
                                    old_usd,
                                    acc_available,
                                    acc_current,
                                    max_per_token,
                                    planned_token_notional_for_acc,
                                    is_lowp,
                                    acc_reason,
                                )
                                # Continue with shrunken order (don't skip)
                            else:
                                # Available amount is below minimum order size
                                logger.warning(
                                    "[ACCUMULATOR_BLOCK] token_id=%s order_usd=%s acc_current=%s local_delta=%s acc_limit=%s acc_available=%s min_usd=%s planned_token=%s is_lowp=%s reason=%s",
                                    token_id,
                                    order_notional,
                                    acc_current,
                                    local_accumulator_delta,
                                    max_per_token,
                                    acc_available,
                                    effective_min_usd,
                                    planned_token_notional_for_acc,
                                    is_lowp,
                                    acc_reason,
                                )
                                blocked_reasons.add(acc_reason or "accumulator_check")
                                continue
                        else:
                            # No room available
                            logger.warning(
                                "[ACCUMULATOR_BLOCK] token_id=%s order_usd=%s acc_current=%s local_delta=%s acc_limit=%s planned_token=%s is_lowp=%s reason=%s",
                                token_id,
                                order_notional,
                                acc_current,
                                local_accumulator_delta,
                                max_per_token,
                                planned_token_notional_for_acc,
                                is_lowp,
                                acc_reason,
                            )
                            blocked_reasons.add(acc_reason or "accumulator_check")
                            continue

                planned_token_notional = max(
                    float(planned_by_token_usd.get(token_id, 0.0)),
                    float(planned_by_token_usd_shadow.get(token_id, 0.0)),
                )
                planned_total_notional_risk = max(
                    planned_total_notional, planned_total_notional_shadow
                )
                cfg_for_action = cfg_lowp if (is_lowp and side == "BUY") else cfg
                ok, reason = risk_check(
                    token_key,
                    size,
                    my_shares,
                    price,
                    cfg_for_action,
                    token_title=token_title,
                    side=side,
                    planned_total_notional=planned_total_notional_risk,
                    planned_token_notional=planned_token_notional,
                    cumulative_total_usd=None,
                    cumulative_token_usd=None,
                )
                if not ok:
                    resized = _shrink_on_risk_limit(
                        act,
                        max_notional_total,
                        planned_total_notional_risk,
                        float(cfg_for_action.get("max_notional_per_token") or 0.0),
                        planned_token_notional,
                        float(cfg_for_action.get("min_order_usd") or 0.0),
                        float(cfg_for_action.get("min_order_shares") or 0.0),
                        token_key,
                        token_id,
                        logger,
                    )
                    if resized is None:
                        if has_any_place and pending_cancel_actions:
                            planned_total_notional += pending_cancel_usd
                            planned_by_token_usd[token_id] = token_planned_before
                            pending_cancel_actions = []
                            pending_cancel_usd = 0.0
                        blocked_reasons.add(reason or "risk_check")
                        continue

                    act, allowed_usd = resized
                    price = float(act.get("price") or 0.0)
                    size = float(act.get("size") or 0.0)

                    # CRITICAL: Re-check accumulator after shrink
                    shrink_notional = abs(size) * price
                    acc_ok_shrink, acc_reason_shrink, _acc_available_shrink = (
                        accumulator_check(
                            token_id,
                            shrink_notional,
                            state,
                            cfg_for_action,
                            side=side,
                            local_delta=local_accumulator_delta,
                        )
                    )
                    if not acc_ok_shrink:
                        logger.warning(
                            "[ACCUMULATOR_BLOCK_SHRINK] token_id=%s shrink_usd=%s "
                            "current_delta=%s reason=%s",
                            token_id,
                            shrink_notional,
                            local_accumulator_delta,
                            acc_reason_shrink,
                        )
                        if has_any_place and pending_cancel_actions:
                            planned_total_notional += pending_cancel_usd
                            planned_by_token_usd[token_id] = token_planned_before
                            pending_cancel_actions = []
                            pending_cancel_usd = 0.0
                        blocked_reasons.add(acc_reason_shrink or "accumulator_check_shrink")
                        continue

                    planned_token_notional = max(
                        float(planned_by_token_usd.get(token_id, 0.0)),
                        float(planned_by_token_usd_shadow.get(token_id, 0.0)),
                    )
                    ok2, reason2 = risk_check(
                        token_key,
                        size,
                        my_shares,
                        price,
                        cfg_for_action,
                        token_title=token_title,
                        side=side,
                        planned_total_notional=planned_total_notional_risk,
                        planned_token_notional=planned_token_notional,
                        cumulative_total_usd=None,
                        cumulative_token_usd=None,
                    )
                    if not ok2:
                        if has_any_place and pending_cancel_actions:
                            planned_total_notional += pending_cancel_usd
                            planned_by_token_usd[token_id] = token_planned_before
                            pending_cancel_actions = []
                            pending_cancel_usd = 0.0
                        blocked_reasons.add(reason2 or reason or "risk_check")
                        continue

                if pending_cancel_actions:
                    filtered_actions.extend(pending_cancel_actions)
                    pending_cancel_actions = []
                    pending_cancel_usd = 0.0

                filtered_actions.append(act)
                if side == "BUY":
                    usd = abs(size) * price
                    planned_total_notional += usd
                    planned_by_token_usd[token_id] = planned_by_token_usd.get(token_id, 0.0) + usd
                    # CRITICAL: Update local accumulator delta to prevent batch bypass
                    local_accumulator_delta += usd

            if has_any_place and pending_cancel_actions:
                planned_total_notional += pending_cancel_usd
                planned_by_token_usd[token_id] = token_planned_before
                pending_cancel_actions = []
                pending_cancel_usd = 0.0
            elif (not has_any_place) and pending_cancel_actions:
                filtered_actions.extend(pending_cancel_actions)

            if not filtered_actions:
                if has_any_place:
                    reason_text = (
                        ",".join(sorted(blocked_reasons)) if blocked_reasons else "risk_check"
                    )
                    logger.info("[NOOP] token_id=%s reason=%s", token_id, reason_text)
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue
            actions = filtered_actions
            logger.info("[ACTION] token_id=%s -> %s", token_id, actions)

            is_reprice = _is_pure_reprice(actions)
            if place_backoff_active and any(act.get("type") == "place" for act in actions):
                logger.info(
                    "[SKIP] token_id=%s reason=place_backoff until=%s",
                    token_id,
                    place_fail_until,
                )
                _maybe_update_target_last(state, token_id, t_now, should_update_last)
                continue
            ignore_cd = bool(cfg.get("exit_ignore_cooldown", True)) and is_exiting
            if cooldown_active and (not ignore_cd) and (not is_reprice):
                logger.info("[SKIP] token_id=%s reason=cooldown", token_id)
                continue

            did_place_buy = any(
                act.get("type") == "place" and str(act.get("side") or "").upper() == "BUY"
                for act in filtered_actions
            )
            if probe_attempted and did_place_buy and st.get("phase") == "LONG":
                st["did_probe"] = True
                topic_state[token_id] = st

            updated_orders = apply_actions(
                clob_client,
                actions,
                open_orders,
                now_ts,
                args.dry_run,
                cfg=cfg,
                state=state,
                planned_by_token_usd=planned_by_token_usd_shadow,
            )
            if updated_orders:
                state.setdefault("open_orders", {})[token_id] = updated_orders
            else:
                state.get("open_orders", {}).pop(token_id, None)
            _prune_order_ts_by_id(state)
            _refresh_managed_order_ids(state)
            (
                planned_total_notional,
                planned_by_token_usd,
                order_info_by_id,
                _shadow_buy_usd,
            ) = _calc_planned_notional_with_fallback(
                my_by_token_id_for_risk,
                state.get("open_orders", {}),
                state.get("last_mid_price_by_token_id", {}),
                max_position_usd_per_token,
                state,
                now_ts,
                shadow_ttl_sec,
                fallback_mid_price,
                logger,
                include_shadow=False,
            )
            (
                planned_total_notional_shadow,
                planned_by_token_usd_shadow,
                _shadow_order_info_by_id,
                _shadow_buy_usd,
            ) = _calc_planned_notional_with_fallback(
                my_by_token_id_for_risk,
                state.get("open_orders", {}),
                state.get("last_mid_price_by_token_id", {}),
                max_position_usd_per_token,
                state,
                now_ts,
                shadow_ttl_sec,
                fallback_mid_price,
                logger,
                include_shadow=True,
            )

            has_any_place_final = any(act.get("type") == "place" for act in actions)
            if (
                cooldown_sec > 0
                and actions
                and has_any_place_final
                and (not ignore_cd)
                and (not is_reprice)
            ):
                state.setdefault("cooldown_until", {})[token_id] = now_ts + cooldown_sec
            if is_reprice:
                state.setdefault("last_reprice_ts_by_token", {})[token_id] = now_ts

            _maybe_update_target_last(state, token_id, t_now, should_update_last)

        if mode == "auto_usd" and delta_usd_samples:
            delta_usd_samples.sort()
            mid = delta_usd_samples[len(delta_usd_samples) // 2]
            alpha = 0.2
            new_ema = (1 - alpha) * ema + alpha * mid
            state.setdefault("sizing", {})["ema_delta_usd"] = new_ema
            state["sizing"]["last_k"] = cfg.get("_auto_order_k")

        state["last_sync_ts"] = now_ts
        save_state(args.state, state)
        time.sleep(_get_poll_interval())


if __name__ == "__main__":
    main()
