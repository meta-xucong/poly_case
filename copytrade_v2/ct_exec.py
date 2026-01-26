from __future__ import annotations

import inspect
import logging
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ct_utils import round_to_tick, safe_float


logger = logging.getLogger(__name__)


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


def _best_from_levels(levels: Iterable[Any], pick_max: bool) -> Optional[float]:
    prices: List[float] = []
    for level in levels:
        if isinstance(level, Mapping):
            candidate = safe_float(level.get("price"))
            if candidate is not None:
                prices.append(candidate)
        elif isinstance(level, (list, tuple)) and level:
            candidate = safe_float(level[0])
            if candidate is not None:
                prices.append(candidate)
    if not prices:
        return None
    return max(prices) if pick_max else min(prices)


def _normalize_orderbook_payload(book: Any) -> Optional[Mapping[str, Any]]:
    if isinstance(book, Mapping):
        return book
    if hasattr(book, "dict"):
        payload = book.dict()
        if isinstance(payload, Mapping):
            return payload
    if hasattr(book, "__dict__"):
        payload = dict(book.__dict__)
        if isinstance(payload, Mapping):
            return payload
    return None


def get_orderbook(client: Any, token_id: str) -> Dict[str, Optional[float]]:
    tid = str(token_id)

    best_ask: Optional[float] = None
    best_bid: Optional[float] = None
    price_bid_raw: Any = None
    price_ask_raw: Any = None

    try:
        price_bid_raw = client.get_price(tid, side="BUY")
        if isinstance(price_bid_raw, dict):
            best_bid = safe_float(price_bid_raw.get("price"))
        else:
            best_bid = safe_float(price_bid_raw)
    except Exception:
        pass
    try:
        price_ask_raw = client.get_price(tid, side="SELL")
        if isinstance(price_ask_raw, dict):
            best_ask = safe_float(price_ask_raw.get("price"))
        else:
            best_ask = safe_float(price_ask_raw)
    except Exception:
        pass

    if best_ask is not None or best_bid is not None:
        if best_ask is not None and best_ask <= 0:
            best_ask = None
        if best_bid is not None and best_bid <= 0:
            best_bid = None
        if best_ask is not None and best_bid is not None:
            if best_bid <= best_ask:
                logger.debug(
                    "[ORDERBOOK_PRICE] token_id=%s price_bid=%s price_ask=%s "
                    "best_bid=%s best_ask=%s",
                    tid,
                    price_bid_raw,
                    price_ask_raw,
                    best_bid,
                    best_ask,
                )
                return {"best_bid": best_bid, "best_ask": best_ask}
            best_ask = None
            best_bid = None

    try:
        book = client.get_order_book(tid)
        payload: Any = book
        if hasattr(book, "dict"):
            payload = book.dict()
        elif isinstance(book, dict):
            payload = book

        bids = payload.get("bids", []) if isinstance(payload, dict) else getattr(book, "bids", [])
        asks = payload.get("asks", []) if isinstance(payload, dict) else getattr(book, "asks", [])

        def _best(levels: Any, pick_max: bool) -> Optional[float]:
            prices: list[float] = []
            if isinstance(levels, list):
                for level in levels:
                    if isinstance(level, dict):
                        price = safe_float(level.get("price"))
                    elif isinstance(level, (list, tuple)) and level:
                        price = safe_float(level[0])
                    else:
                        price = None
                    if price is not None:
                        prices.append(float(price))
            if not prices:
                return None
            return max(prices) if pick_max else min(prices)

        book_bid = _best(bids, pick_max=True)
        book_ask = _best(asks, pick_max=False)
        if book_ask is not None and book_ask <= 0:
            book_ask = None
        if book_bid is not None and book_bid <= 0:
            book_bid = None

        if best_bid is None:
            best_bid = book_bid
        if best_ask is None:
            best_ask = book_ask

        # NOTE: Allow single-sided books (one side temporarily missing) instead of treating as empty.
        # This avoids "orderbook_empty" NOOPs that can freeze existing orders at stale prices.
        if best_bid is not None and best_ask is not None and best_bid > best_ask:
            return {"best_bid": None, "best_ask": None}
        logger.debug(
            "[ORDERBOOK] token_id=%s price_bid=%s price_ask=%s best_bid=%s best_ask=%s "
            "book_bid=%s book_ask=%s",
            tid,
            price_bid_raw,
            price_ask_raw,
            best_bid,
            best_ask,
            book_bid,
            book_ask,
        )
        return {"best_bid": best_bid, "best_ask": best_ask}
    except Exception:
        return {"best_bid": None, "best_ask": None}


def reconcile_one(
    token_id: str,
    desired_shares: float,
    my_shares: float,
    orderbook: Dict[str, Optional[float]],
    open_orders: List[Dict[str, Any]],
    now_ts: int,
    cfg: Dict[str, Any],
    state: Dict[str, Any],
    planned_token_notional: float = 0.0,
) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    deadband = float(cfg.get("deadband_shares") or 0)
    delta = desired_shares - my_shares
    if abs(delta) <= deadband and not open_orders:
        return actions

    abs_delta = abs(delta)
    phase = (state.get("topic_state", {}).get(token_id) or {}).get("phase")
    is_exiting = phase == "EXITING"

    mode = str(cfg.get("order_size_mode") or "fixed_shares").lower()
    size: float = 0.0
    target_order_usd: Optional[float] = None

    if mode == "auto_usd":
        ref_price = _mid_price(orderbook)
        if ref_price is None or ref_price <= 0:
            return actions

        min_usd = float(cfg.get("min_order_usd") or 5.0)
        min_shares = float(cfg.get("min_order_shares") or 0.0)
        max_usd = float(cfg.get("max_order_usd") or 25.0)
        if max_usd < min_usd:
            max_usd = min_usd

        k = float(cfg.get("_auto_order_k") or 0.3)

        delta_usd = abs_delta * ref_price
        order_usd = delta_usd * k
        if order_usd < min_usd:
            order_usd = min_usd
        if min_shares > 0:
            order_usd = max(order_usd, min_shares * ref_price)
        if order_usd > max_usd:
            order_usd = max_usd

        target_order_usd = order_usd
        size = order_usd / ref_price
    else:
        slice_min = float(cfg.get("slice_min") or 0)
        slice_max = float(cfg.get("slice_max") or abs_delta)
        if slice_max <= 0:
            slice_max = abs_delta

        size = min(abs_delta, slice_max)
        if slice_min > 0 and abs_delta > slice_min and size < slice_min:
            size = slice_min

    side = "BUY" if delta > 0 else "SELL"
    price: Optional[float] = None
    best_bid = orderbook.get("best_bid")
    best_ask = orderbook.get("best_ask")
    tick_size = float(cfg.get("tick_size") or 0)
    taker_spread_thr = float(cfg.get("taker_spread_threshold") or 0.01)
    taker_enabled = bool(cfg.get("taker_enabled", True))

    spread: Optional[float] = None
    if best_bid is not None and best_ask is not None:
        try:
            spread = float(best_ask) - float(best_bid)
        except Exception:
            spread = None

    use_taker = bool(
        taker_enabled
        and spread is not None
        and spread <= (taker_spread_thr + 1e-12)
    )
    logger.debug(
        "[TAKER_CHECK] token_id=%s side=%s best_bid=%s best_ask=%s spread=%s thr=%s "
        "taker_enabled=%s use_taker=%s open_orders=%s",
        token_id,
        side,
        best_bid,
        best_ask,
        spread,
        taker_spread_thr,
        taker_enabled,
        use_taker,
        len(open_orders),
    )

    if use_taker:
        if side == "BUY":
            if best_ask is None:
                use_taker = False
            else:
                price = round_to_tick(float(best_ask), tick_size, direction="up")
        else:
            if best_bid is None:
                use_taker = False
            else:
                price = round_to_tick(float(best_bid), tick_size, direction="down")

    if not use_taker:
        if side == "BUY":
            if best_bid is not None:
                price = best_bid
            elif best_ask is not None:
                price = best_ask - tick_size
            if price is not None:
                price = round_to_tick(price, tick_size, direction="down")
        else:
            if best_ask is not None:
                price = best_ask
            elif best_bid is not None:
                price = best_bid + tick_size
            if price is not None:
                price = round_to_tick(price, tick_size, direction="up")

        maker_only = bool(cfg.get("maker_only"))
        if maker_only and tick_size and tick_size > 0:
            if side == "BUY" and best_ask is not None and price is not None and price >= best_ask:
                price = round_to_tick(best_ask - tick_size, tick_size, direction="down")
            if side == "SELL" and best_bid is not None and price is not None and price <= best_bid:
                price = round_to_tick(best_bid + tick_size, tick_size, direction="up")

            if price is None or price <= 0:
                return actions

    if price is None or price <= 0:
        return actions

    min_price = float(cfg.get("min_price") or 0.01)
    if min_price > 0 and price < min_price:
        price = min_price
        if tick_size > 0:
            price = round_to_tick(price, tick_size, direction="up")
    if min_price > 0 and price < min_price:
        price = min_price

    if mode == "auto_usd" and target_order_usd is not None:
        size = target_order_usd / price

    if is_exiting and side == "SELL" and bool(cfg.get("exit_full_sell", True)):
        size = abs_delta

    max_shares_cap = float(cfg.get("max_order_shares_cap") or 5000.0)
    if size > max_shares_cap:
        size = max_shares_cap

    allow_short = bool(cfg.get("allow_short"))
    if side == "SELL" and not allow_short:
        size = min(size, my_shares)

    min_shares = float(cfg.get("min_order_shares") or 0.0)
    api_min_shares = 0.0
    if state is not None:
        status_cache = state.get("market_status_cache")
        if isinstance(status_cache, dict):
            cached = status_cache.get(token_id) or {}
            if isinstance(cached, dict):
                meta = cached.get("meta") or {}
                if isinstance(meta, dict):
                    api_min_shares = safe_float(meta.get("orderMinSize")) or 0.0
    effective_min_shares = max(min_shares, api_min_shares)
    cap_shares = None
    cap_shares_remaining = None
    if price > 0 and side == "BUY":
        max_position_usd_per_token = float(cfg.get("max_position_usd_per_token") or 0.0)
        max_notional_per_token = float(cfg.get("max_notional_per_token") or 0.0)
        caps = []
        if max_position_usd_per_token > 0:
            caps.append(max_position_usd_per_token / price)
        if max_notional_per_token > 0:
            caps.append(max_notional_per_token / price)
        if caps:
            cap_shares = min(caps)
            max_notional = cap_shares * price
            remaining_notional = max_notional - planned_token_notional
            cap_shares_remaining = remaining_notional / price if price > 0 else 0

    if effective_min_shares > 0 and size < effective_min_shares and side == "BUY":
        bumped_size = effective_min_shares
        if cap_shares_remaining is not None:
            if cap_shares_remaining <= 0:
                logger.debug(
                    "[MIN_BUMP_SKIP] token_id=%s no_remaining cap_shares_remaining=%s "
                    "planned_notional=%s my_shares=%s price=%s",
                    token_id,
                    cap_shares_remaining,
                    planned_token_notional,
                    my_shares,
                    price,
                )
                return actions
            bumped_size = min(bumped_size, cap_shares_remaining)
        if bumped_size + 1e-12 < effective_min_shares:
            logger.debug(
                "[MIN_BUMP_SKIP] token_id=%s bumped_below_min bumped=%s min=%s",
                token_id,
                bumped_size,
                effective_min_shares,
            )
            return actions
        logger.info(
            "[MIN_BUMP] token_id=%s old_size=%s bumped_size=%s remaining=%s",
            token_id,
            size,
            bumped_size,
            cap_shares_remaining,
        )
        size = bumped_size
    if open_orders:
        total_open = 0.0
        for order in open_orders:
            try:
                total_open += float(order.get("size") or order.get("original_size") or 0.0)
            except Exception:
                continue
        if effective_min_shares > 0 and size < effective_min_shares:
            size = max(size, effective_min_shares, total_open)
    if size > max_shares_cap:
        size = max_shares_cap
    if side == "SELL" and not allow_short:
        size = min(size, my_shares)
    if effective_min_shares > 0 and size < effective_min_shares:
        if is_exiting and side == "SELL":
            logger.info(
                "[DUST_EXIT] token_id=%s remaining=%s < min_order=%s; treat as exited",
                token_id,
                my_shares,
                effective_min_shares,
            )
            state.setdefault("dust_exits", {})[token_id] = {
                "ts": now_ts,
                "shares": my_shares,
            }
            topic_state = state.get("topic_state")
            if isinstance(topic_state, dict):
                topic_state.pop(token_id, None)
        return actions

    if size <= 0:
        return actions

    if open_orders:
        if use_taker:
            actions = []
            for order in open_orders:
                order_id = order.get("order_id") or order.get("id")
                if order_id:
                    actions.append(
                        {
                            "type": "cancel",
                            "order_id": order_id,
                            "token_id": token_id,
                            "ts": now_ts,
                        }
                    )
            if size > 0:
                actions.append(
                    {
                        "type": "place",
                        "token_id": token_id,
                        "side": side,
                        "price": price,
                        "size": size,
                        "ts": now_ts,
                        "_taker": True,
                        "_taker_spread": spread,
                        "_taker_thr": taker_spread_thr,
                    }
                )
            logger.info(
                "[SWITCH_TO_TAKER] token_id=%s side=%s spread=%s thr=%s",
                token_id,
                side,
                spread,
                taker_spread_thr,
            )
            return actions
        if is_exiting and side == "SELL" and bool(cfg.get("exit_full_sell", True)):
            eps = 1e-9
            total_open = 0.0
            for order in open_orders:
                try:
                    total_open += float(order.get("size") or order.get("original_size") or 0.0)
                except Exception:
                    continue
            if len(open_orders) != 1 or total_open < (abs_delta - eps):
                actions = []
                for order in open_orders:
                    order_id = order.get("order_id") or order.get("id")
                    if order_id:
                        actions.append(
                            {
                                "type": "cancel",
                                "order_id": order_id,
                                "token_id": token_id,
                                "ts": now_ts,
                            }
                        )
                actions.append(
                    {
                        "type": "place",
                        "token_id": token_id,
                        "side": "SELL",
                        "price": price,
                        "size": abs_delta,
                        "ts": now_ts,
                        "_exit_consolidate": True,
                    }
                )
                state.setdefault("last_reprice_ts_by_token", {})[token_id] = now_ts
                logger.info(
                    "[EXIT_CONSOLIDATE] token_id=%s remaining=%s open_orders=%s open_total=%s",
                    token_id,
                    abs_delta,
                    len(open_orders),
                    total_open,
                )
                return actions
        enable_reprice = bool(cfg.get("enable_reprice", False))
        if not enable_reprice:
            return actions
        active_order: Optional[Dict[str, Any]] = None
        if side == "BUY":
            active_order = max(open_orders, key=lambda order: float(order.get("price") or 0))
        else:
            active_order = min(open_orders, key=lambda order: float(order.get("price") or 0))
        if active_order:
            active_price = safe_float(active_order.get("price"))
            last_reprice_ts = int(
                state.setdefault("last_reprice_ts_by_token", {}).get(token_id) or 0
            )
            reprice_ticks = int(cfg.get("reprice_ticks") or cfg.get("reprice_min_ticks") or 1)
            cooldown_sec = int(cfg.get("reprice_cooldown_sec") or 0)
            cooldown_ok = cooldown_sec <= 0 or (now_ts - last_reprice_ts) >= cooldown_sec
            if active_price is not None and tick_size > 0 and cooldown_ok:
                ideal_price = price
                if ideal_price is not None and abs(ideal_price - active_price) < tick_size / 2:
                    return actions
                moved_ticks = None
                if ideal_price is not None:
                    moved_ticks = abs(ideal_price - active_price) / tick_size
                trigger = moved_ticks is not None and moved_ticks >= (reprice_ticks - 1e-9)
                if trigger:
                    logger.info(
                        "[REPRICE] token_id=%s side=%s active_price=%s ideal_price=%s "
                        "best_bid=%s best_ask=%s reprice_ticks=%s cooldown_sec=%s since_last=%s",
                        token_id,
                        side,
                        active_price,
                        ideal_price,
                        best_bid,
                        best_ask,
                        reprice_ticks,
                        cooldown_sec,
                        now_ts - last_reprice_ts,
                    )
                    for order in open_orders:
                        order_id = order.get("order_id") or order.get("id")
                        if order_id:
                            actions.append({"type": "cancel", "order_id": order_id})
                    actions.append(
                        {
                            "type": "place",
                            "token_id": token_id,
                            "side": side,
                            "price": price,
                            "size": size,
                            "ts": now_ts,
                            "_reprice": True,
                        }
                    )
                    state.setdefault("last_reprice_ts_by_token", {})[token_id] = now_ts
                    return actions
        return actions

    actions.append(
        {
            "type": "place",
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "ts": now_ts,
            **(
                {
                    "_taker": True,
                    "_taker_spread": spread,
                    "_taker_thr": taker_spread_thr,
                }
                if use_taker
                else {}
            ),
        }
    )
    return actions


def _extract_order_id(response: object) -> Optional[str]:
    candidates = (
        "order_id",
        "orderId",
        "orderID",
        "id",
        "orderHash",
        "order_hash",
        "hash",
    )

    visited: set[int] = set()

    def walk(obj: object) -> Optional[str]:
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj_id = id(obj)
            if obj_id in visited:
                return None
            visited.add(obj_id)
            for key in candidates:
                if key in obj and obj[key] is not None:
                    return str(obj[key])
            for value in obj.values():
                nested = walk(value)
                if nested:
                    return nested
        if isinstance(obj, (list, tuple)):
            for item in obj:
                nested = walk(item)
                if nested:
                    return nested
        return None

    return walk(response)


def cancel_order(client: Any, order_id: str) -> Optional[object]:
    if not order_id:
        return None
    if callable(getattr(client, "cancel", None)):
        return client.cancel(order_id=order_id)
    if callable(getattr(client, "cancel_order", None)):
        return client.cancel_order(order_id)
    if callable(getattr(client, "cancel_orders", None)):
        return client.cancel_orders([order_id])

    private = getattr(client, "private", None)
    if private is not None:
        if callable(getattr(private, "cancel", None)):
            return private.cancel(order_id=order_id)
        if callable(getattr(private, "cancel_order", None)):
            return private.cancel_order(order_id)
        if callable(getattr(private, "cancel_orders", None)):
            return private.cancel_orders([order_id])

    return None


def _is_insufficient_balance(value: object) -> bool:
    def _text_has_shortage(text: str) -> bool:
        lowered = text.lower()
        shortage_keywords = ("insufficient", "not enough")
        balance_keywords = ("balance", "fund", "allowance")
        return any(key in lowered for key in shortage_keywords) and any(
            key in lowered for key in balance_keywords
        )

    if hasattr(value, "error_message"):
        try:
            if _is_insufficient_balance(getattr(value, "error_message")):
                return True
        except Exception:
            pass
    if hasattr(value, "response"):
        try:
            if _is_insufficient_balance(getattr(value, "response")):
                return True
        except Exception:
            pass
    if hasattr(value, "args"):
        try:
            for arg in getattr(value, "args", ()):
                if _is_insufficient_balance(arg):
                    return True
        except Exception:
            pass

    if isinstance(value, dict):
        for key in ("error", "message", "detail", "reason", "status"):
            if key in value and _is_insufficient_balance(value[key]):
                return True
    try:
        return _text_has_shortage(str(value))
    except Exception:
        return False


def place_order(
    client: Any,
    token_id: str,
    side: str,
    price: float,
    size: float,
    allow_partial: bool = True,
) -> Dict[str, Any]:
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL

    side_const = BUY if side.upper() == "BUY" else SELL
    order_kwargs: Dict[str, Any] = {
        "token_id": str(token_id),
        "side": side_const,
        "price": float(price),
        "size": float(size),
    }
    allow_partial_key: Optional[str] = None
    try:
        params = inspect.signature(OrderArgs).parameters
    except (TypeError, ValueError):
        params = {}
    if "allow_partial" in params:
        allow_partial_key = "allow_partial"
    elif "allowPartial" in params:
        allow_partial_key = "allowPartial"
    if allow_partial_key:
        order_kwargs[allow_partial_key] = allow_partial

    order_args = OrderArgs(**order_kwargs)
    signed = client.create_order(order_args)
    if allow_partial and allow_partial_key is None:
        try:
            response = client.post_order(signed, OrderType.GTC, allow_partial=True)
        except TypeError:
            response = client.post_order(signed, OrderType.GTC)
    else:
        response = client.post_order(signed, OrderType.GTC)
    order_id = _extract_order_id(response)
    result: Dict[str, Any] = {"response": response}
    if order_id:
        result["order_id"] = order_id
    return result


def place_market_order(
    client: Any,
    token_id: str,
    side: str,
    amount: float,
    price: Optional[float] = None,
    order_type: str = "FAK",
) -> Dict[str, Any]:
    """
    Taker path via MarketOrderArgs.
    - BUY: amount is USD
    - SELL: amount is shares
    """
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL

    side_const = BUY if side.upper() == "BUY" else SELL

    kwargs: Dict[str, Any] = {"token_id": str(token_id), "side": side_const}
    try:
        params = inspect.signature(MarketOrderArgs).parameters
    except Exception:
        params = {}

    if "amount" in params:
        kwargs["amount"] = float(amount)
    elif "size" in params:
        kwargs["size"] = float(amount)
    else:
        kwargs["amount"] = float(amount)

    if price is not None and float(price) > 0:
        if "price" in params:
            kwargs["price"] = float(price)

    order_args = MarketOrderArgs(**kwargs)

    if hasattr(client, "create_market_order"):
        signed = client.create_market_order(order_args)
    else:
        signed = client.create_order(order_args)

    ot = getattr(OrderType, str(order_type).upper(), None)
    if ot is None:
        ot = getattr(OrderType, "FAK", None) or getattr(OrderType, "FOK")

    response = client.post_order(signed, ot)
    order_id = _extract_order_id(response)
    result: Dict[str, Any] = {"response": response}
    if order_id:
        result["order_id"] = order_id
    return result


def apply_actions(
    client: Any,
    actions: List[Dict[str, Any]],
    open_orders: List[Dict[str, Any]],
    now_ts: int,
    dry_run: bool,
    cfg: Optional[Dict[str, Any]] = None,
    state: Optional[Dict[str, Any]] = None,
    planned_by_token_usd: Optional[Dict[str, float]] = None,
) -> List[Dict[str, Any]]:
    updated = [dict(order) for order in open_orders]

    def _short_msg(value: object, limit: int = 160) -> str:
        text = str(value)
        if len(text) <= limit:
            return text
        return f"{text[: limit - 3]}..."

    def _remove_recent_buy(
        orders: object,
        token_id: str,
        usd: float,
        eps: float = 1e-6,
    ) -> bool:
        if not isinstance(orders, list):
            return False
        for idx in range(len(orders) - 1, -1, -1):
            item = orders[idx]
            if not isinstance(item, dict):
                continue
            if str(item.get("token_id") or "") != token_id:
                continue
            item_usd = float(item.get("usd") or 0.0)
            if abs(item_usd - usd) <= max(eps, eps * max(1.0, usd)):
                orders.pop(idx)
                return True
        return False

    def _rollback_buy_tracking(token_id: str, usd: float) -> None:
        if state is None or usd <= 0:
            return
        accumulator = state.get("buy_notional_accumulator")
        if isinstance(accumulator, dict) and token_id in accumulator:
            acc_data = accumulator.get(token_id)
            if isinstance(acc_data, dict):
                old_usd = float(acc_data.get("usd", 0.0))
                new_usd = max(0.0, old_usd - usd)
                if new_usd <= 0.01:
                    accumulator.pop(token_id, None)
                else:
                    acc_data["usd"] = new_usd
                    acc_data["last_ts"] = now_ts
                logger.info(
                    "[ACCUMULATOR_ROLLBACK] token_id=%s old=%s cancel=%s new=%s",
                    token_id,
                    old_usd,
                    usd,
                    new_usd,
                )
        recent_orders = state.get("recent_buy_orders")
        if _remove_recent_buy(recent_orders, token_id, usd):
            state["recent_buy_orders"] = recent_orders
            logger.info(
                "[RECENT_BUY_ROLLBACK] token_id=%s usd=%s",
                token_id,
                usd,
            )
        taker_orders = state.get("taker_buy_orders")
        if _remove_recent_buy(taker_orders, token_id, usd):
            state["taker_buy_orders"] = taker_orders
            logger.info(
                "[TAKER_BUY_ROLLBACK] token_id=%s usd=%s",
                token_id,
                usd,
            )
        shadow_orders = state.get("shadow_buy_orders")
        if _remove_recent_buy(shadow_orders, token_id, usd):
            state["shadow_buy_orders"] = shadow_orders
            logger.info(
                "[SHADOW_BUY_ROLLBACK] token_id=%s usd=%s",
                token_id,
                usd,
            )

    def _bump_backoff(token_id: str, kind: str, msg: str) -> None:
        if state is None or cfg is None:
            return
        key = f"{kind}:{token_id}"
        fail_counts = state.setdefault("fail_counts", {})
        fail_counts[key] = int(fail_counts.get(key) or 0) + 1
        base = float(cfg.get("place_fail_backoff_base_sec") or 2)
        cap = float(cfg.get("place_fail_backoff_cap_sec") or 60)
        wait = min(cap, base * (2 ** (fail_counts[key] - 1)))
        until = now_ts + wait
        state.setdefault("place_fail_until", {})[token_id] = int(until)
        lastlog = state.setdefault("place_fail_lastlog", {}).get(key, 0)
        if now_ts - int(lastlog or 0) >= 5:
            logger.warning(
                "[BACKOFF] token_id=%s kind=%s fail_count=%s wait=%.1fs msg=%s",
                token_id,
                kind,
                fail_counts[key],
                wait,
                _short_msg(msg),
            )
            state["place_fail_lastlog"][key] = now_ts
    for action in actions:
        if action.get("type") == "cancel":
            order_id = action.get("order_id")
            if not order_id:
                continue
            token_id = ""
            side_u = ""
            price = 0.0
            size = 0.0
            for order in updated:
                if str(order.get("order_id")) == str(order_id):
                    token_id = str(order.get("token_id") or "")
                    side_u = str(order.get("side") or "").upper()
                    try:
                        price = float(order.get("price") or 0.0)
                    except Exception:
                        price = 0.0
                    try:
                        size = float(order.get("size") or order.get("original_size") or 0.0)
                    except Exception:
                        size = 0.0
                    break
            if dry_run:
                updated = [o for o in updated if str(o.get("order_id")) != str(order_id)]
                if token_id and side_u == "BUY" and price > 0 and size > 0:
                    _rollback_buy_tracking(token_id, abs(size) * price)
                continue
            try:
                cancel_order(client, str(order_id))
                updated = [o for o in updated if str(o.get("order_id")) != str(order_id)]
                if token_id and side_u == "BUY" and price > 0 and size > 0:
                    _rollback_buy_tracking(token_id, abs(size) * price)
            except Exception as exc:
                logger.warning("cancel_order failed order_id=%s: %s", order_id, exc)
            continue

    for action in actions:
        if action.get("type") != "place":
            continue
        is_taker = bool(action.get("_taker"))
        if dry_run:
            if not is_taker:
                updated.append(
                    {
                        "order_id": "dry_run",
                        "side": action.get("side"),
                        "price": action.get("price"),
                        "size": action.get("size"),
                        "ts": now_ts,
                    }
                )
            else:
                logger.info(
                    "[DRY_RUN_TAKER] token_id=%s side=%s price=%s size=%s",
                    action.get("token_id"),
                    action.get("side"),
                    action.get("price"),
                    action.get("size"),
                )
            continue
        if (not is_taker) and cfg is not None and bool(cfg.get("dedupe_place", True)):
            token_id = str(action.get("token_id") or "")
            want_side = str(action.get("side") or "")
            try:
                want_price = float(action.get("price") or 0.0)
            except Exception:
                want_price = 0.0
            try:
                want_size = float(action.get("size") or 0.0)
            except Exception:
                want_size = 0.0

            eps_p = float(cfg.get("dedupe_place_price_eps") or 1e-6)
            eps_rel = float(cfg.get("dedupe_place_size_rel_eps") or 1e-6)

            dup = False
            for order in updated:
                try:
                    if str(order.get("side") or "") != want_side:
                        continue
                    order_price = float(order.get("price") or 0.0)
                    if abs(order_price - want_price) > eps_p:
                        continue
                    order_size = float(order.get("size") or 0.0)
                    if want_size > 0 and abs(order_size - want_size) > max(
                        1e-9, eps_rel * want_size
                    ):
                        continue
                    logger.info(
                        "[DEDUP] token_id=%s skip duplicate place (order_id=%s)",
                        token_id,
                        order.get("order_id"),
                    )
                    dup = True
                    break
                except Exception:
                    continue

            if dup:
                continue
        allow_partial = True
        if cfg is not None:
            allow_partial = bool(cfg.get("allow_partial", True))
        size_for_record = float(action.get("size") or 0.0)
        side_u = str(action.get("side") or "").upper()
        price = float(action.get("price") or 0.0)
        size = float(action.get("size") or 0.0)
        try:
            if is_taker:
                if side_u == "BUY" and cfg is not None and planned_by_token_usd is not None:
                    max_per_token = float(cfg.get("max_notional_per_token") or 0.0)
                    if max_per_token > 0:
                        token_id_check = str(action.get("token_id"))
                        planned = float(planned_by_token_usd.get(token_id_check, 0.0))
                        order_usd = abs(size) * price
                        if planned + order_usd > max_per_token:
                            logger.warning(
                                "[TAKER_BLOCKED] token_id=%s would_exceed planned=%s order=%s max=%s",
                                token_id_check,
                                planned,
                                order_usd,
                                max_per_token,
                            )
                            if state is not None:
                                state.setdefault("taker_blocked_count", 0)
                                state["taker_blocked_count"] = state["taker_blocked_count"] + 1
                            continue

                if side_u == "BUY":
                    amount = abs(size) * price
                else:
                    amount = abs(size)
                taker_order_type = "FAK"
                if cfg is not None:
                    if cfg.get("taker_order_type"):
                        taker_order_type = str(cfg.get("taker_order_type")).upper()
                    else:
                        taker_order_type = "FAK" if allow_partial else "FOK"
                response = place_market_order(
                    client,
                    token_id=str(action.get("token_id")),
                    side=side_u,
                    amount=amount,
                    price=price,
                    order_type=taker_order_type,
                )
            else:
                response = place_order(
                    client,
                    token_id=str(action.get("token_id")),
                    side=side_u,
                    price=price,
                    size=size_for_record,
                    allow_partial=allow_partial,
                )
        except Exception as exc:
            logger.warning("place_order failed token_id=%s: %s", action.get("token_id"), exc)
            if not cfg or not cfg.get("retry_on_insufficient_balance"):
                continue
            if side_u != "BUY":
                if _is_insufficient_balance(exc) and state is not None:
                    token_id = str(action.get("token_id") or "")
                    refresh_tokens = state.setdefault("force_refresh_tokens", [])
                    if token_id and token_id not in refresh_tokens:
                        refresh_tokens.append(token_id)
                    if price > 0 and size > 0:
                        min_order_usd = float(cfg.get("min_order_usd") or 0.0)
                        min_order_shares = float(cfg.get("min_order_shares") or 0.0)
                        shrink_factor = float(cfg.get("retry_shrink_factor") or 0.5)
                        old_usd = abs(size) * price
                        new_usd = max(min_order_usd, old_usd * shrink_factor)
                        if min_order_shares > 0:
                            new_usd = max(new_usd, min_order_shares * price)
                        if new_usd < old_usd * (1 - 1e-9):
                            new_size = new_usd / price
                            if min_order_shares <= 0 or new_size + 1e-12 >= min_order_shares:
                                try:
                                    if is_taker:
                                        response = place_market_order(
                                            client,
                                            token_id=str(action.get("token_id")),
                                            side=side_u,
                                            amount=abs(new_size),
                                            price=price,
                                            order_type="FAK" if allow_partial else "FOK",
                                        )
                                    else:
                                        response = place_order(
                                            client,
                                            token_id=str(action.get("token_id")),
                                            side=side_u,
                                            price=price,
                                            size=new_size,
                                            allow_partial=allow_partial,
                                        )
                                except Exception as retry_exc:
                                    logger.warning(
                                        "[RETRY_SELL_FAIL] token_id=%s old_usd=%s new_usd=%s: %s",
                                        action.get("token_id"),
                                        old_usd,
                                        new_usd,
                                        retry_exc,
                                    )
                                    _bump_backoff(token_id, "sell_insufficient", str(retry_exc))
                                    continue
                                size_for_record = new_size
                                logger.info(
                                    "[RETRY_SELL_OK] token_id=%s old_usd=%s new_usd=%s",
                                    action.get("token_id"),
                                    old_usd,
                                    new_usd,
                                )
                                order_id = response.get("order_id")
                                if order_id and not is_taker:
                                    updated.append(
                                        {
                                            "order_id": order_id,
                                            "side": action.get("side"),
                                            "price": action.get("price"),
                                            "size": size_for_record,
                                            "ts": now_ts,
                                        }
                                    )
                                    if state is not None:
                                        token_id = str(action.get("token_id") or "")
                                        place_fail_until = state.get("place_fail_until")
                                        if isinstance(place_fail_until, dict):
                                            place_fail_until.pop(token_id, None)
                                        logger.info(
                                            "[BACKOFF_CLEAR] token_id=%s place succeeded -> clear "
                                            "place_fail_until",
                                            token_id,
                                        )
                                continue
                    _bump_backoff(token_id, "sell_insufficient", str(exc))
                continue
            if not _is_insufficient_balance(exc):
                continue
            if price <= 0 or size <= 0:
                continue
            min_order_usd = float(cfg.get("min_order_usd") or 0.0)
            min_order_shares = float(cfg.get("min_order_shares") or 0.0)
            shrink_factor = float(cfg.get("retry_shrink_factor") or 0.5)
            old_usd = abs(size) * price
            new_usd = max(min_order_usd, old_usd * shrink_factor)
            if min_order_shares > 0:
                new_usd = max(new_usd, min_order_shares * price)
            if new_usd >= old_usd * (1 - 1e-9):
                continue
            new_size = new_usd / price
            if min_order_shares > 0 and new_size + 1e-12 < min_order_shares:
                continue
            try:
                if is_taker:
                    response = place_market_order(
                        client,
                        token_id=str(action.get("token_id")),
                        side=side_u,
                        amount=abs(new_size) * price,
                        price=price,
                        order_type="FAK" if allow_partial else "FOK",
                    )
                else:
                    response = place_order(
                        client,
                        token_id=str(action.get("token_id")),
                        side=side_u,
                        price=price,
                        size=new_size,
                        allow_partial=allow_partial,
                    )
            except Exception as retry_exc:
                logger.warning(
                    "[RETRY_BALANCE_FAIL] token_id=%s old_usd=%s new_usd=%s: %s",
                    action.get("token_id"),
                    old_usd,
                    new_usd,
                    retry_exc,
                )
                continue
            size_for_record = new_size
            logger.info(
                "[RETRY_BALANCE_OK] token_id=%s old_usd=%s new_usd=%s",
                action.get("token_id"),
                old_usd,
                new_usd,
            )
        order_id = response.get("order_id")
        # CRITICAL: Update accumulator for SELL orders (reduce on sell)
        if state is not None and side_u == "SELL" and price > 0 and size_for_record > 0:
            token_id = str(action.get("token_id") or "")
            sell_usd = abs(size_for_record) * price
            if token_id and sell_usd > 0:
                accumulator = state.get("buy_notional_accumulator")
                if isinstance(accumulator, dict) and token_id in accumulator:
                    acc_data = accumulator[token_id]
                    if isinstance(acc_data, dict):
                        old_usd = float(acc_data.get("usd", 0.0))
                        new_usd = max(0.0, old_usd - sell_usd)
                        if new_usd <= 0.01:
                            # Position effectively cleared
                            accumulator.pop(token_id, None)
                            logger.info(
                                "[ACCUMULATOR_CLEAR_SELL] token_id=%s old=%s sell=%s reason=cleared",
                                token_id,
                                old_usd,
                                sell_usd,
                            )
                        else:
                            acc_data["usd"] = new_usd
                            acc_data["last_ts"] = now_ts
                            logger.info(
                                "[ACCUMULATOR_REDUCE_SELL] token_id=%s old=%s sell=%s new=%s",
                                token_id,
                                old_usd,
                                sell_usd,
                                new_usd,
                            )

        if (
            state is not None
            and side_u == "BUY"
            and price > 0
            and size_for_record > 0
        ):
            token_id = str(action.get("token_id") or "")
            usd = abs(size_for_record) * price
            if token_id and usd > 0:
                recent_orders = state.setdefault("recent_buy_orders", [])
                if not isinstance(recent_orders, list):
                    state["recent_buy_orders"] = []
                    recent_orders = state["recent_buy_orders"]
                recent_orders.append(
                    {
                        "token_id": token_id,
                        "usd": float(usd),
                        "ts": int(now_ts),
                    }
                )
                if is_taker:
                    taker_orders = state.setdefault("taker_buy_orders", [])
                    if not isinstance(taker_orders, list):
                        state["taker_buy_orders"] = []
                        taker_orders = state["taker_buy_orders"]
                    taker_orders.append(
                        {
                            "token_id": token_id,
                            "usd": float(usd),
                            "ts": int(now_ts),
                        }
                    )
                # CRITICAL: Update buy notional accumulator (first line of defense)
                accumulator = state.setdefault("buy_notional_accumulator", {})
                if not isinstance(accumulator, dict):
                    state["buy_notional_accumulator"] = {}
                    accumulator = state["buy_notional_accumulator"]
                if token_id not in accumulator:
                    accumulator[token_id] = {"usd": 0.0, "last_ts": now_ts}
                accumulator[token_id]["usd"] = float(accumulator[token_id].get("usd", 0.0)) + float(usd)
                accumulator[token_id]["last_ts"] = int(now_ts)
                logger.info(
                    "[ACCUMULATOR_UPDATE] token_id=%s added_usd=%s total_usd=%s is_taker=%s",
                    token_id,
                    usd,
                    accumulator[token_id]["usd"],
                    is_taker,
                )
        if order_id:
            if state is not None:
                token_id = str(action.get("token_id") or "")
                for key_prefix in ("sell_insufficient", "sell_insufficient_shrink"):
                    state.get("fail_counts", {}).pop(f"{key_prefix}:{token_id}", None)
                    state.get("place_fail_lastlog", {}).pop(f"{key_prefix}:{token_id}", None)
                place_fail_until = state.get("place_fail_until")
                if isinstance(place_fail_until, dict):
                    place_fail_until.pop(token_id, None)
                logger.info(
                    "[BACKOFF_CLEAR] token_id=%s place succeeded -> clear place_fail_until",
                    token_id,
                )
            if not is_taker:
                updated.append(
                    {
                        "order_id": order_id,
                        "side": action.get("side"),
                        "price": action.get("price"),
                        "size": size_for_record,
                        "ts": now_ts,
                    }
                )
    return updated




def _as_dict(obj: Any) -> Optional[Dict[str, Any]]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "dict"):
        try:
            return obj.dict()
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        try:
            return dict(obj.__dict__)
        except Exception:
            pass
    return None


def _coerce_list(payload: Any) -> List[Any]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "orders", "result", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return []
    for key in ("data", "orders", "result", "items"):
        value = getattr(payload, key, None)
        if isinstance(value, list):
            return value
    return []


def _parse_created_ts(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            parsed = int(value)
            return parsed if parsed > 0 else None
        if isinstance(value, str):
            text = value.strip()
            num = safe_float(text)
            if num is not None and num > 0:
                numeric = int(num)
                return numeric // 1000 if numeric > 10_000_000_000 else numeric
            from datetime import datetime, timezone

            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed_dt = datetime.fromisoformat(text)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return int(parsed_dt.timestamp())
    except Exception:
        return None
    return None


def _normalize_open_order(order: Any) -> Optional[Dict[str, Any]]:
    data = _as_dict(order)
    if not data:
        return None

    order_id = (
        data.get("id")
        or data.get("order_id")
        or data.get("orderId")
        or data.get("orderID")
        or data.get("order_hash")
        or data.get("orderHash")
    )
    token_id = (
        data.get("asset_id")
        or data.get("assetId")
        or data.get("token_id")
        or data.get("tokenId")
        or data.get("clobTokenId")
        or data.get("clob_token_id")
    )
    if not order_id or not token_id:
        return None

    side = data.get("side") or data.get("taker_side") or data.get("maker_side")
    side_norm = side.upper() if isinstance(side, str) else str(side).upper()

    price = safe_float(data.get("price") or data.get("limit_price") or data.get("limitPrice"))
    size = safe_float(
        data.get("size")
        or data.get("original_size")
        or data.get("originalSize")
        or data.get("remaining_size")
        or data.get("remainingSize")
        or data.get("amount")
    )

    created_ts = _parse_created_ts(data.get("created_at") or data.get("createdAt") or data.get("timestamp"))
    return {
        "order_id": str(order_id),
        "token_id": str(token_id),
        "side": side_norm,
        "price": price,
        "size": size,
        "created_ts": created_ts,
    }


def fetch_open_orders_norm(client: Any) -> tuple[list[dict[str, Any]], bool, str | None]:
    from py_clob_client.clob_types import OpenOrderParams

    def _resolve_open_orders_url() -> str | None:
        for attr in ("host", "base_url", "url"):
            base = getattr(client, attr, None)
            if base:
                base = str(base).rstrip("/")
                return f"{base}/orders"
        return None

    def _supports_timeout(func: Any) -> bool:
        try:
            return "timeout" in inspect.signature(func).parameters
        except Exception:
            return False

    def _call_get_orders(params: Any | None, timeout: float) -> Any:
        kwargs: Dict[str, Any] = {}
        if _supports_timeout(client.get_orders):
            kwargs["timeout"] = timeout
        if params is None:
            return client.get_orders(**kwargs)
        return client.get_orders(params, **kwargs)

    timeout = 15.0
    retry_delays = [0.5, 1.0, 2.0]
    max_attempts = len(retry_delays) + 1
    payload = None
    last_exc: Exception | None = None
    retries_used = 0
    for attempt in range(max_attempts):
        try:
            payload = _call_get_orders(OpenOrderParams(), timeout)
            last_exc = None
            break
        except Exception as exc:
            try:
                payload = _call_get_orders(None, timeout)
                last_exc = None
                break
            except Exception as exc2:
                last_exc = exc2 or exc
        if last_exc is not None and attempt < len(retry_delays):
            retries_used = attempt + 1
            time.sleep(retry_delays[attempt])
            continue
        break
    if last_exc is not None:
        err_detail = (
            f"{last_exc} url={_resolve_open_orders_url()} timeout={timeout} retries={retries_used}"
        )
        return [], False, err_detail

    orders = _coerce_list(payload)
    normalized: List[Dict[str, Any]] = []
    for order in orders:
        parsed = _normalize_open_order(order)
        if not parsed:
            continue
        normalized.append(
            {
                "order_id": parsed["order_id"],
                "token_id": parsed["token_id"],
                "side": parsed["side"],
                "price": float(parsed["price"]) if parsed["price"] is not None else None,
                "size": float(parsed["size"]) if parsed["size"] is not None else None,
                "ts": parsed["created_ts"],
            }
        )
    filtered = [item for item in normalized if item["price"] is not None and item["size"] is not None]
    return filtered, True, None
