from __future__ import annotations

from typing import Any, Dict, Optional, Tuple


def accumulator_check(
    token_id: str,
    order_notional: float,
    state: Dict[str, Any],
    cfg: Dict[str, object],
    side: Optional[str] = None,
    local_delta: float = 0.0,
    planned_token_notional: Optional[float] = None,
) -> Tuple[bool, str, float]:
    """
    First line of defense: check local buy notional accumulator.
    This provides a hard limit independent of position API synchronization.

    Args:
        local_delta: Accumulator delta from previous orders in the same batch
                     (to prevent batch bypass vulnerability)
        planned_token_notional: Actual position value (shares * mid_price + open buy orders)
                                 If provided, uses min(accumulator, planned) as baseline

    Returns:
        (ok, reason, available_notional)
        - ok: True if order can proceed as-is, False if exceeds limit
        - reason: "ok" or the limit that was hit
        - available_notional: How much USD is available for this order (0 if none)
    """
    side_u = str(side).upper() if side is not None else ""
    if side_u != "BUY":
        return True, "ok", float("inf")

    max_per_token = float(cfg.get("max_notional_per_token") or 0)
    max_position_per_token = float(cfg.get("max_position_usd_per_token") or 0)

    accumulator = state.get("buy_notional_accumulator")
    if not isinstance(accumulator, dict):
        accumulator_usd = 0.0
    else:
        token_acc = accumulator.get(token_id)
        if not isinstance(token_acc, dict):
            accumulator_usd = 0.0
        else:
            accumulator_usd = float(token_acc.get("usd", 0.0))

    # CRITICAL: Use actual position value if provided, otherwise use accumulator
    # This prevents blocking orders when accumulator is high due to historical cost
    # but actual position value is low due to price drops
    if planned_token_notional is not None:
        # Use planned notional as the "used" amount to reflect current exposure.
        # This allows buy capacity to recover after sells/claims reduce holdings.
        effective_current = planned_token_notional + local_delta
    else:
        # Fallback to accumulator only (legacy behavior)
        effective_current = accumulator_usd + local_delta

    # Check limits and calculate available notional
    if max_per_token > 0:
        available = max_per_token - effective_current
        if available <= 0:
            return False, "accumulator_max_notional_per_token", 0.0
        if order_notional > available:
            # Order exceeds limit, but some room is available
            return False, "accumulator_max_notional_per_token", available

    if max_position_per_token > 0:
        available = max_position_per_token - effective_current
        if available <= 0:
            return False, "accumulator_max_position_usd_per_token", 0.0
        if order_notional > available:
            return False, "accumulator_max_position_usd_per_token", available

    # Order is within limits
    return True, "ok", float("inf")


def risk_check(
    token_key: str,
    order_shares: float,
    my_shares: float,
    ref_price: float,
    cfg: Dict[str, object],
    token_title: Optional[str] = None,
    side: Optional[str] = None,
    planned_total_notional: Optional[float] = None,
    planned_token_notional: Optional[float] = None,
    cumulative_total_usd: Optional[float] = None,
    cumulative_token_usd: Optional[float] = None,
) -> Tuple[bool, str]:
    blacklist = cfg.get("blacklist_token_keys") or []
    if blacklist:
        token_title_l = (str(token_title).lower() if token_title is not None else "")
        for item in blacklist:
            if item is None:
                continue
            item_str = str(item)
            if token_key == item_str:
                return False, "blacklist"
            if token_title_l and item_str.strip():
                if item_str.lower() in token_title_l:
                    return False, "blacklist"

    max_per_token = float(cfg.get("max_notional_per_token") or 0)
    max_position_per_token = float(cfg.get("max_position_usd_per_token") or 0)
    order_notional = abs(order_shares) * ref_price if ref_price else 0.0

    side_u = str(side).upper() if side is not None else ""
    allow_short = bool(cfg.get("allow_short", False))
    if side_u == "SELL":
        if my_shares > 0:
            return True, "ok"
        if not allow_short:
            return False, "short_disabled"
    apply_token_cap = side_u == "BUY" or (side_u == "SELL" and allow_short)
    if max_per_token > 0 and apply_token_cap:
        base_token = (
            float(planned_token_notional)
            if planned_token_notional is not None
            else float(cumulative_token_usd or 0.0)
        )
        if base_token + order_notional > max_per_token:
            return False, "max_notional_per_token"
    if max_position_per_token > 0 and apply_token_cap:
        base_token = (
            float(planned_token_notional)
            if planned_token_notional is not None
            else float(cumulative_token_usd or 0.0)
        )
        if base_token + order_notional > max_position_per_token:
            return False, "max_position_usd_per_token"

    max_total = float(cfg.get("max_notional_total") or 0)
    apply_total_cap = side_u == "BUY" or (side_u == "SELL" and allow_short)
    if max_total > 0 and apply_total_cap:
        base_total = (
            float(planned_total_notional)
            if planned_total_notional is not None
            else float(cumulative_total_usd or 0.0)
        )
        if base_total + order_notional > max_total:
            return False, "max_notional_total"

    return True, "ok"
