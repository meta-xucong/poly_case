from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class Trade:
    """标准化的成交记录，来源于 Polymarket `/trades` 接口。"""

    tx_hash: str
    market_id: str
    outcome: Optional[str]
    side: str
    price: float
    size: float
    cost: float
    timestamp: dt.datetime
    market_slug: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def notional(self) -> float:
        return abs(self.price * self.size)

    @classmethod
    def from_api(cls, raw: Dict[str, Any]) -> Optional["Trade"]:
        """从 `/trades` 返回的原始字典构建 Trade，兼容常见字段。"""

        tx_hash = str(raw.get("tx_hash") or raw.get("txHash") or raw.get("transactionHash") or "").strip()
        market_id = str(
            raw.get("conditionId")
            or raw.get("market")
            or raw.get("marketId")
            or raw.get("market_id")
            or ""
        ).strip()
        outcome = raw.get("outcome") or raw.get("tokenName") or raw.get("outcome_name")
        side = (raw.get("type") or raw.get("side") or "").upper()
        price = _coerce_float(raw.get("price") or raw.get("avgPrice") or raw.get("fillPrice"))
        size = _coerce_float(raw.get("size") or raw.get("amount") or raw.get("quantity"))
        cost = _coerce_float(raw.get("cost") or raw.get("value"))
        ts = _parse_timestamp(raw.get("timestamp") or raw.get("time") or raw.get("created_at"))
        slug = raw.get("marketSlug") or raw.get("slug")

        if not tx_hash or not market_id or price is None or size is None or ts is None:
            return None

        if cost is None:
            cost = price * size

        return cls(
            tx_hash=tx_hash,
            market_id=market_id,
            outcome=str(outcome) if outcome is not None else None,
            side=side or "",
            price=price,
            size=size,
            cost=cost,
            timestamp=ts,
            market_slug=str(slug) if slug is not None else None,
            raw=raw,
        )


@dataclass
class TradeAction:
    """按交易哈希聚合后的成交动作。"""

    tx_hash: str
    timestamp: dt.datetime


@dataclass
class Position:
    """标准化的持仓记录，来源于 Polymarket `/positions` 接口。"""

    user: str
    condition_id: str
    outcome: Optional[str]
    outcome_index: Optional[int]
    title: Optional[str]
    slug: Optional[str]
    size: float
    avg_price: float
    initial_value: float
    current_value: float
    cash_pnl: float
    realized_pnl: float
    cur_price: Optional[float]
    end_date: Optional[dt.datetime]
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api(cls, raw: Dict[str, Any], *, user: str) -> Optional["Position"]:
        condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "").strip()
        if not condition_id:
            return None
        outcome = raw.get("outcome") or raw.get("outcomeName") or raw.get("tokenName")
        outcome_index = _coerce_int(raw.get("outcomeIndex") or raw.get("outcome_index"))
        title = raw.get("title")
        slug = raw.get("slug") or raw.get("marketSlug")
        size = _coerce_float(raw.get("size")) or 0.0
        avg_price = _coerce_float(raw.get("avgPrice") or raw.get("avg_price")) or 0.0
        initial_value = _coerce_float(raw.get("initialValue") or raw.get("initial_value")) or 0.0
        current_value = _coerce_float(raw.get("currentValue") or raw.get("current_value")) or 0.0
        cash_pnl = _coerce_float(raw.get("cashPnl") or raw.get("cash_pnl")) or 0.0
        realized_pnl = _coerce_float(raw.get("realizedPnl") or raw.get("realized_pnl")) or 0.0
        cur_price = _coerce_float(raw.get("curPrice") or raw.get("cur_price"))
        end_date = _parse_timestamp(raw.get("endDate") or raw.get("end_date"))

        return cls(
            user=user,
            condition_id=condition_id,
            outcome=str(outcome) if outcome is not None else None,
            outcome_index=outcome_index,
            title=str(title) if title is not None else None,
            slug=str(slug) if slug is not None else None,
            size=size,
            avg_price=avg_price,
            initial_value=initial_value,
            current_value=current_value,
            cash_pnl=cash_pnl,
            realized_pnl=realized_pnl,
            cur_price=cur_price,
            end_date=end_date,
            raw=raw,
        )


@dataclass
class ClosedPosition:
    """标准化的已平仓记录，来源于 Polymarket `/closed-positions` 接口。"""

    user: str
    condition_id: str
    outcome: Optional[str]
    outcome_index: Optional[int]
    title: Optional[str]
    slug: Optional[str]
    avg_price: float
    total_bought: float
    realized_pnl: float
    cur_price: Optional[float]
    timestamp: dt.datetime
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api(cls, raw: Dict[str, Any], *, user: str) -> Optional["ClosedPosition"]:
        condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "").strip()
        timestamp = _parse_timestamp(raw.get("timestamp") or raw.get("time"))
        if not condition_id or timestamp is None:
            return None
        outcome = raw.get("outcome") or raw.get("outcomeName") or raw.get("tokenName")
        outcome_index = _coerce_int(raw.get("outcomeIndex") or raw.get("outcome_index"))
        title = raw.get("title")
        slug = raw.get("slug") or raw.get("marketSlug")
        avg_price = _coerce_float(raw.get("avgPrice") or raw.get("avg_price")) or 0.0
        total_bought = _coerce_float(raw.get("totalBought") or raw.get("total_bought")) or 0.0
        realized_pnl = _coerce_float(raw.get("realizedPnl") or raw.get("realized_pnl")) or 0.0
        cur_price = _coerce_float(raw.get("curPrice") or raw.get("cur_price"))

        return cls(
            user=user,
            condition_id=condition_id,
            outcome=str(outcome) if outcome is not None else None,
            outcome_index=outcome_index,
            title=str(title) if title is not None else None,
            slug=str(slug) if slug is not None else None,
            avg_price=avg_price,
            total_bought=total_bought,
            realized_pnl=realized_pnl,
            cur_price=cur_price,
            timestamp=timestamp,
            raw=raw,
        )


def _parse_timestamp(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            # `/trades` 时间戳通常为秒或毫秒
            if value > 1e12:
                value /= 1000.0
            return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            return dt.datetime.fromisoformat(text)
    except Exception:
        return None
    return None


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, str):
            cleaned = value.replace(",", "").strip()
            if cleaned == "":
                return None
            return float(cleaned)
    except Exception:
        return None
    return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            cleaned = value.replace(",", "").strip()
            if cleaned == "":
                return None
            return int(float(cleaned))
    except Exception:
        return None
    return None


@dataclass
class MarketAggregation:
    market_id: str
    slug: Optional[str]
    resolved_outcome: Optional[str]
    volume: float
    cash_flow: float
    remaining_positions: Dict[str, float]
    pnl: Optional[float]
    resolved: bool
    win: Optional[bool]
    trades_count: int
    first_trade_at: dt.datetime
    last_trade_at: dt.datetime


@dataclass
class AggregatedStats:
    user: str
    start_time: Optional[dt.datetime]
    end_time: Optional[dt.datetime]
    total_volume: float
    resolved_pnl: float
    win_rate: Optional[float]
    resolved_markets: int
    unresolved_markets: int
    markets: List[MarketAggregation]


@dataclass
class UserSummary:
    user: str
    start_time: Optional[dt.datetime]
    end_time: Optional[dt.datetime]
    account_start_time: Optional[dt.datetime]
    account_age_days: Optional[float]
    lifetime_realized_pnl_sum: Optional[float]
    lifetime_closed_count: Optional[int]
    lifetime_incomplete: Optional[bool]
    lifetime_status: Optional[str]
    closed_count: int
    closed_realized_pnl_sum: float
    win_count: int
    loss_count: int
    flat_count: int
    win_rate_all: Optional[float]
    win_rate_no_flat: Optional[float]
    open_count: int
    open_unrealized_pnl_sum: float
    open_realized_pnl_sum: float
    asof_time: dt.datetime
    leaderboard_month_pnl: Optional[float] = None
    suspected_hft: Optional[bool] = None
    hft_reason: Optional[str] = None
    trade_actions_pages: Optional[int] = None
    trade_actions_records: Optional[int] = None
    trade_actions_actions: Optional[int] = None
    status: Optional[str] = None
