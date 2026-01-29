from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from .models import AggregatedStats, ClosedPosition, MarketAggregation, Position, Trade, UserSummary


@dataclass
class _Position:
    shares: float = 0.0
    cash_flow: float = 0.0
    volume: float = 0.0


def filter_trades_by_time(
    trades: Iterable[Trade],
    *,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
) -> List[Trade]:
    results: List[Trade] = []
    for trade in trades:
        ts = trade.timestamp
        if start_time and ts < start_time:
            continue
        if end_time and ts > end_time:
            continue
        results.append(trade)
    results.sort(key=lambda t: t.timestamp)
    return results


def aggregate_markets(
    trades: Iterable[Trade],
    *,
    resolutions: Optional[Dict[str, str]] = None,
    user: str = "",
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
) -> AggregatedStats:
    resolutions = resolutions or {}
    filtered = filter_trades_by_time(trades, start_time=start_time, end_time=end_time)

    market_positions: Dict[str, Dict[str, _Position]] = defaultdict(lambda: defaultdict(_Position))
    meta: Dict[str, Dict[str, Any]] = defaultdict(dict)

    for trade in filtered:
        pos = market_positions[trade.market_id][trade.outcome or "UNKNOWN"]
        notional = trade.price * trade.size
        pos.volume += abs(notional)
        if trade.side == "SELL":
            pos.cash_flow += abs(notional)
            pos.shares -= trade.size
        else:
            pos.cash_flow -= abs(notional)
            pos.shares += trade.size

        info = meta[trade.market_id]
        info.setdefault("slug", trade.market_slug)
        info.setdefault("first_trade_at", trade.timestamp)
        info["last_trade_at"] = trade.timestamp
        info["count"] = info.get("count", 0) + 1

    market_results: List[MarketAggregation] = []
    resolved_markets = 0
    win_markets = 0
    total_volume = 0.0
    resolved_pnl = 0.0

    for market_id, outcomes in market_positions.items():
        slug = meta[market_id].get("slug")
        remaining = {name: pos.shares for name, pos in outcomes.items()}
        volume = sum(pos.volume for pos in outcomes.values())
        total_volume += volume

        resolved_outcome = resolutions.get(market_id)
        resolved = resolved_outcome is not None
        pnl: Optional[float] = None
        win: Optional[bool] = None

        cash_flow_total = sum(pos.cash_flow for pos in outcomes.values())
        if resolved:
            payout = 0.0
            for outcome_name, pos in outcomes.items():
                if outcome_name and outcome_name == resolved_outcome:
                    payout += pos.shares
            pnl = cash_flow_total + payout
            resolved_markets += 1
            resolved_pnl += pnl
            win = pnl > 0
            if win:
                win_markets += 1

        market_results.append(
            MarketAggregation(
                market_id=market_id,
                slug=slug,
                resolved_outcome=resolved_outcome,
                volume=volume,
                cash_flow=cash_flow_total,
                remaining_positions=remaining,
                pnl=pnl,
                resolved=resolved,
                win=win,
                trades_count=meta[market_id].get("count", 0),
                first_trade_at=meta[market_id]["first_trade_at"],
                last_trade_at=meta[market_id]["last_trade_at"],
            )
        )

    win_rate = None
    if resolved_markets:
        win_rate = win_markets / resolved_markets

    return AggregatedStats(
        user=user,
        start_time=start_time,
        end_time=end_time,
        total_volume=total_volume,
        resolved_pnl=resolved_pnl,
        win_rate=win_rate,
        resolved_markets=resolved_markets,
        unresolved_markets=len(market_positions) - resolved_markets,
        markets=sorted(market_results, key=lambda m: m.volume, reverse=True),
    )


def summarize_user(
    closed_positions: Iterable[ClosedPosition],
    open_positions: Iterable[Position],
    *,
    user: str,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
    asof_time: Optional[dt.datetime] = None,
    account_start_time: Optional[dt.datetime] = None,
    lifetime_realized_pnl_sum: Optional[float] = None,
    lifetime_closed_count: Optional[int] = None,
    lifetime_incomplete: Optional[bool] = None,
    lifetime_status: Optional[str] = None,
) -> UserSummary:
    closed_count = 0
    closed_realized_pnl_sum = 0.0
    win_count = 0
    loss_count = 0
    flat_count = 0

    for position in closed_positions:
        ts = position.timestamp
        if start_time and ts < start_time:
            continue
        if end_time and ts > end_time:
            continue
        closed_count += 1
        closed_realized_pnl_sum += position.realized_pnl
        if position.realized_pnl > 0:
            win_count += 1
        elif position.realized_pnl < 0:
            loss_count += 1
        else:
            flat_count += 1

    win_rate_all = win_count / closed_count if closed_count else None
    win_rate_no_flat = (
        win_count / (win_count + loss_count) if (win_count + loss_count) else None
    )

    open_positions_list = list(open_positions)
    open_count = len(open_positions_list)
    open_unrealized_pnl_sum = sum(pos.cash_pnl for pos in open_positions_list)
    open_realized_pnl_sum = sum(pos.realized_pnl for pos in open_positions_list)

    if asof_time is None:
        asof_time = dt.datetime.now(tz=dt.timezone.utc)

    account_age_days = None
    if account_start_time is not None:
        account_age_days = max((asof_time - account_start_time).total_seconds() / 86400, 0.0)

    return UserSummary(
        user=user,
        start_time=start_time,
        end_time=end_time,
        account_start_time=account_start_time,
        account_age_days=account_age_days,
        lifetime_realized_pnl_sum=lifetime_realized_pnl_sum,
        lifetime_closed_count=lifetime_closed_count,
        lifetime_incomplete=lifetime_incomplete,
        lifetime_status=lifetime_status,
        closed_count=closed_count,
        closed_realized_pnl_sum=closed_realized_pnl_sum,
        win_count=win_count,
        loss_count=loss_count,
        flat_count=flat_count,
        win_rate_all=win_rate_all,
        win_rate_no_flat=win_rate_no_flat,
        open_count=open_count,
        open_unrealized_pnl_sum=open_unrealized_pnl_sum,
        open_realized_pnl_sum=open_realized_pnl_sum,
        asof_time=asof_time,
    )
