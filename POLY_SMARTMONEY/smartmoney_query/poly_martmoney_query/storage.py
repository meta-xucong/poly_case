from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .models import (
    AggregatedStats,
    ClosedPosition,
    MarketAggregation,
    Position,
    Trade,
    TradeAction,
    UserSummary,
)


def append_trades_csv(path: Path, trades: Iterable[Trade]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    existing_hashes = set()
    if path.exists():
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                tx = row.get("tx_hash")
                if tx:
                    existing_hashes.add(tx)

    fieldnames = [
        "tx_hash",
        "market_id",
        "market_slug",
        "outcome",
        "side",
        "price",
        "size",
        "cost",
        "timestamp",
    ]

    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if path.stat().st_size == 0:
            writer.writeheader()
        for trade in trades:
            if trade.tx_hash in existing_hashes:
                continue
            writer.writerow(
                {
                    "tx_hash": trade.tx_hash,
                    "market_id": trade.market_id,
                    "market_slug": trade.market_slug or "",
                    "outcome": trade.outcome or "",
                    "side": trade.side,
                    "price": f"{trade.price:.6f}",
                    "size": f"{trade.size:.6f}",
                    "cost": f"{trade.cost:.6f}",
                    "timestamp": trade.timestamp.isoformat(),
                }
            )
            existing_hashes.add(trade.tx_hash)


def write_market_stats_csv(path: Path, stats: AggregatedStats) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "market_id",
        "slug",
        "resolved",
        "resolved_outcome",
        "win",
        "pnl",
        "volume",
        "cash_flow",
        "remaining_positions",
        "trades_count",
        "first_trade_at",
        "last_trade_at",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in stats.markets:
            writer.writerow(
                {
                    "market_id": m.market_id,
                    "slug": m.slug or "",
                    "resolved": m.resolved,
                    "resolved_outcome": m.resolved_outcome or "",
                    "win": m.win if m.win is not None else "",
                    "pnl": f"{m.pnl:.6f}" if m.pnl is not None else "",
                    "volume": f"{m.volume:.6f}",
                    "cash_flow": f"{m.cash_flow:.6f}",
                    "remaining_positions": _format_positions(m.remaining_positions),
                    "trades_count": m.trades_count,
                    "first_trade_at": m.first_trade_at.isoformat(),
                    "last_trade_at": m.last_trade_at.isoformat(),
                }
            )

    summary_path = path.with_name(path.stem + "_summary" + path.suffix)
    with summary_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "user",
                "start_time",
                "end_time",
                "total_volume",
                "resolved_pnl",
                "win_rate",
                "resolved_markets",
                "unresolved_markets",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "user": stats.user,
                "start_time": stats.start_time.isoformat() if stats.start_time else "",
                "end_time": stats.end_time.isoformat() if stats.end_time else "",
                "total_volume": f"{stats.total_volume:.6f}",
                "resolved_pnl": f"{stats.resolved_pnl:.6f}",
                "win_rate": f"{stats.win_rate:.4f}" if stats.win_rate is not None else "",
                "resolved_markets": stats.resolved_markets,
                "unresolved_markets": stats.unresolved_markets,
            }
        )


def write_closed_positions_csv(path: Path, closed_positions: Iterable[ClosedPosition]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "user",
        "condition_id",
        "outcome",
        "outcome_index",
        "title",
        "slug",
        "avg_price",
        "total_bought",
        "realized_pnl",
        "cur_price",
        "timestamp",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in closed_positions:
            writer.writerow(
                {
                    "user": item.user,
                    "condition_id": item.condition_id,
                    "outcome": item.outcome or "",
                    "outcome_index": item.outcome_index if item.outcome_index is not None else "",
                    "title": item.title or "",
                    "slug": item.slug or "",
                    "avg_price": f"{item.avg_price:.6f}",
                    "total_bought": f"{item.total_bought:.6f}",
                    "realized_pnl": f"{item.realized_pnl:.6f}",
                    "cur_price": f"{item.cur_price:.6f}" if item.cur_price is not None else "",
                    "timestamp": item.timestamp.isoformat(),
                }
            )


def write_trade_actions_csv(path: Path, actions: Iterable[TradeAction]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp",
        "tx_hash",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for action in actions:
            writer.writerow(
                {
                    "timestamp": action.timestamp.isoformat(),
                    "tx_hash": action.tx_hash,
                }
            )


def write_positions_csv(path: Path, positions: Iterable[Position]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "user",
        "condition_id",
        "outcome",
        "outcome_index",
        "title",
        "slug",
        "size",
        "avg_price",
        "initial_value",
        "current_value",
        "cash_pnl",
        "realized_pnl",
        "cur_price",
        "end_date",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in positions:
            writer.writerow(
                {
                    "user": item.user,
                    "condition_id": item.condition_id,
                    "outcome": item.outcome or "",
                    "outcome_index": item.outcome_index if item.outcome_index is not None else "",
                    "title": item.title or "",
                    "slug": item.slug or "",
                    "size": f"{item.size:.6f}",
                    "avg_price": f"{item.avg_price:.6f}",
                    "initial_value": f"{item.initial_value:.6f}",
                    "current_value": f"{item.current_value:.6f}",
                    "cash_pnl": f"{item.cash_pnl:.6f}",
                    "realized_pnl": f"{item.realized_pnl:.6f}",
                    "cur_price": f"{item.cur_price:.6f}" if item.cur_price is not None else "",
                    "end_date": item.end_date.isoformat() if item.end_date else "",
                }
            )


SUMMARY_FIELDNAMES = [
    "user",
    "start_time",
    "end_time",
    "account_start_time",
    "account_age_days",
    "lifetime_realized_pnl_sum",
    "lifetime_closed_count",
    "lifetime_incomplete",
    "lifetime_status",
    "closed_count",
    "closed_realized_pnl_sum",
    "win_count",
    "loss_count",
    "flat_count",
    "win_rate_all",
    "win_rate_no_flat",
    "open_count",
    "open_unrealized_pnl_sum",
    "open_realized_pnl_sum",
    "asof_time",
    "leaderboard_month_pnl",
    "suspected_hft",
    "hft_reason",
    "trade_actions_pages",
    "trade_actions_records",
    "trade_actions_actions",
    "status",
]


def write_user_summary_csv(path: Path, summary: UserSummary) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDNAMES)
        writer.writeheader()
        writer.writerow(_summary_row(summary))


def write_user_summaries_csv(path: Path, summaries: Iterable[UserSummary]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDNAMES)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(_summary_row(summary))


def update_user_summary(path: Path, user: Optional[str], patch: Dict[str, object]) -> bool:
    path = Path(path)
    if not path.exists():
        return False
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or SUMMARY_FIELDNAMES

    updated = False
    for row in rows:
        if user is None or row.get("user") == user:
            for key, value in patch.items():
                if key not in fieldnames:
                    fieldnames.append(key)
                row[key] = _serialize_summary_value(key, value)
            updated = True
            if user is not None:
                break

    if not updated:
        return False

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return True


def _summary_row(summary: UserSummary) -> Dict[str, object]:
    return {
        "user": summary.user,
        "start_time": summary.start_time.isoformat() if summary.start_time else "",
        "end_time": summary.end_time.isoformat() if summary.end_time else "",
        "account_start_time": summary.account_start_time.isoformat()
        if summary.account_start_time
        else "",
        "account_age_days": f"{summary.account_age_days:.2f}"
        if summary.account_age_days is not None
        else "",
        "lifetime_realized_pnl_sum": _serialize_summary_value(
            "lifetime_realized_pnl_sum", summary.lifetime_realized_pnl_sum
        ),
        "lifetime_closed_count": _serialize_summary_value(
            "lifetime_closed_count", summary.lifetime_closed_count
        ),
        "lifetime_incomplete": _serialize_summary_value(
            "lifetime_incomplete", summary.lifetime_incomplete
        ),
        "lifetime_status": summary.lifetime_status or "",
        "closed_count": summary.closed_count,
        "closed_realized_pnl_sum": f"{summary.closed_realized_pnl_sum:.6f}",
        "win_count": summary.win_count,
        "loss_count": summary.loss_count,
        "flat_count": summary.flat_count,
        "win_rate_all": f"{summary.win_rate_all:.6f}" if summary.win_rate_all is not None else "",
        "win_rate_no_flat": f"{summary.win_rate_no_flat:.6f}"
        if summary.win_rate_no_flat is not None
        else "",
        "open_count": summary.open_count,
        "open_unrealized_pnl_sum": f"{summary.open_unrealized_pnl_sum:.6f}",
        "open_realized_pnl_sum": f"{summary.open_realized_pnl_sum:.6f}",
        "asof_time": summary.asof_time.isoformat(),
        "leaderboard_month_pnl": _serialize_summary_value(
            "leaderboard_month_pnl", summary.leaderboard_month_pnl
        ),
        "suspected_hft": _serialize_summary_value("suspected_hft", summary.suspected_hft),
        "hft_reason": summary.hft_reason or "",
        "trade_actions_pages": _serialize_summary_value(
            "trade_actions_pages", summary.trade_actions_pages
        ),
        "trade_actions_records": _serialize_summary_value(
            "trade_actions_records", summary.trade_actions_records
        ),
        "trade_actions_actions": _serialize_summary_value(
            "trade_actions_actions", summary.trade_actions_actions
        ),
        "status": summary.status or "",
    }


def _serialize_summary_value(key: str, value: object) -> str:
    if value is None:
        return ""
    if key in {"lifetime_realized_pnl_sum", "leaderboard_month_pnl"}:
        return f"{float(value):.6f}"
    if key in {
        "lifetime_closed_count",
        "closed_count",
        "win_count",
        "loss_count",
        "flat_count",
        "open_count",
        "trade_actions_pages",
        "trade_actions_records",
        "trade_actions_actions",
        "suspected_hft",
    }:
        try:
            return str(int(value))
        except (TypeError, ValueError):
            return ""
    if key in {"lifetime_incomplete"}:
        return "true" if bool(value) else "false"
    return str(value)


def _format_positions(positions: Dict[str, float]) -> str:
    parts: List[str] = []
    for outcome, size in positions.items():
        parts.append(f"{outcome}:{size:.4f}")
    return ";".join(parts)
