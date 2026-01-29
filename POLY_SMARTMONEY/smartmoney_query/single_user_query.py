"""
交互式单地址查询脚本：输入账号地址，输出与 screen_users.py 最终表一致的格式。
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from poly_martmoney_query.api_client import DataApiClient
from poly_martmoney_query.processors import summarize_user
from poly_martmoney_query.storage import _summary_row
from screen_users import (
    _apply_filters,
    _build_copy_style,
    _build_features,
    _build_notes,
    _build_price_style,
    _compute_copy_score,
    _load_config,
)


REPORT_FIELDNAMES = [
    "user",
    "status",
    "ok",
    "incomplete",
    "closed_count",
    "open_count",
    "closed_incomplete",
    "open_incomplete",
    "closed_hit_max_pages",
    "open_hit_max_pages",
    "lifetime_status",
    "lifetime_incomplete",
    "leaderboard_month_pnl",
    "account_start_time",
    "account_age_days",
    "suspected_hft",
    "hft_reason",
    "trade_actions_pages",
    "trade_actions_records",
    "trade_actions_actions",
    "error_msg",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket 单地址查询（交互式）")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="统计区间天数（默认 30）",
    )
    parser.add_argument(
        "--size-threshold",
        type=float,
        default=0.0,
        help="positions 的 sizeThreshold（默认 0）",
    )
    parser.add_argument(
        "--lifetime-mode",
        choices=("all", "candidates", "none"),
        default="none",
        help="历史收益拉取模式：all=全量，candidates=仅标记，none=跳过",
    )
    parser.add_argument(
        "--trade-actions-page-size",
        type=int,
        default=300,
        help="trade_actions 拉取的分页大小（默认 300）",
    )
    parser.add_argument(
        "--hft-unique-tx-threshold",
        type=int,
        default=20000,
        help="判定为高频并跳过深拉的阈值，使用 trade_actions 的 actions_count/unique_tx（默认 20000）",
    )
    parser.add_argument(
        "--screen-config",
        default="screen_users_config.json",
        help="筛选配置文件路径（默认 screen_users_config.json）",
    )
    return parser.parse_args()


def _extract_address(text: str) -> str:
    cleaned = text.strip()
    if not cleaned:
        raise ValueError("输入为空，请提供目标账号地址")

    match = re.fullmatch(r"0x[a-fA-F0-9]{40}", cleaned)
    if match:
        return cleaned

    raise ValueError("地址格式不正确，请输入 0x 开头的 42 位地址")


def _prompt_address() -> str:
    try:
        return input("请输入目标账号地址（0x...）：").strip()
    except EOFError as exc:
        raise SystemExit("未获取到输入，已退出。") from exc


def _print_section(title: str) -> None:
    print("\n" + "=" * 20)
    print(title)
    print("=" * 20)


def _write_rows(fieldnames: List[str], rows: Iterable[Dict[str, object]]) -> None:
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)


def _format_value(value: object) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        formatted = f"{value:,.4f}".rstrip("0").rstrip(".")
        return formatted or "0"
    return str(value)


def _print_readable_output(row: Dict[str, object]) -> None:
    if not row:
        return
    max_key_len = max(len(str(key)) for key in row.keys())
    for key, value in row.items():
        key_text = str(key).ljust(max_key_len)
        print(f"{key_text} ： {_format_value(value)}")


def _closed_position_row(item) -> Dict[str, object]:
    return {
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


def _open_position_row(item) -> Dict[str, object]:
    return {
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


def _trade_action_row(item) -> Dict[str, object]:
    return {
        "timestamp": item.timestamp.isoformat(),
        "tx_hash": item.tx_hash,
    }


def _build_report_row(
    *,
    addr: str,
    status: str,
    ok: bool,
    incomplete: bool,
    summary,
    closed_info: Optional[Dict[str, object]] = None,
    open_info: Optional[Dict[str, object]] = None,
    trade_info: Optional[Dict[str, object]] = None,
    lifetime_status: Optional[str] = None,
    lifetime_incomplete: Optional[bool] = None,
    error_msg: str = "",
) -> Dict[str, object]:
    closed_info = closed_info or {}
    open_info = open_info or {}
    trade_info = trade_info or {}
    return {
        "user": addr,
        "status": status,
        "ok": ok,
        "incomplete": incomplete,
        "closed_count": summary.closed_count if summary is not None else "",
        "open_count": summary.open_count if summary is not None else "",
        "closed_incomplete": bool(closed_info.get("incomplete")),
        "open_incomplete": bool(open_info.get("incomplete")),
        "closed_hit_max_pages": bool(closed_info.get("hit_max_pages")),
        "open_hit_max_pages": bool(open_info.get("hit_max_pages")),
        "lifetime_status": lifetime_status or "",
        "lifetime_incomplete": lifetime_incomplete if lifetime_incomplete is not None else "",
        "leaderboard_month_pnl": "",
        "account_start_time": summary.account_start_time.isoformat()
        if summary is not None and summary.account_start_time is not None
        else "",
        "account_age_days": summary.account_age_days if summary is not None else "",
        "suspected_hft": 1 if summary is not None and summary.suspected_hft else 0,
        "hft_reason": summary.hft_reason if summary is not None else "",
        "trade_actions_pages": trade_info.get("pages_fetched", ""),
        "trade_actions_records": trade_info.get("activity_records_fetched", ""),
        "trade_actions_actions": trade_info.get("actions_count", ""),
        "error_msg": error_msg,
    }


def main() -> None:
    args = _parse_args()
    address_text = _prompt_address()
    try:
        addr = _extract_address(address_text)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    base_dir = Path(__file__).resolve().parent
    config_path = Path(args.screen_config)
    if not config_path.is_absolute():
        config_path = (base_dir / config_path).resolve()
    config = _load_config(config_path)

    client = DataApiClient()
    now = dt.datetime.now(tz=dt.timezone.utc)
    start = now - dt.timedelta(days=args.days) if args.days > 0 else None
    end = now

    print(f"[INFO] 解析到地址：{addr}", flush=True)
    print("[INFO] 开始抓取数据...", flush=True)

    trade_actions, trade_info = client.fetch_trade_actions_window_from_activity(
        user=addr,
        start_time=start,
        end_time=end,
        page_size=args.trade_actions_page_size,
        progress_every=20,
        return_info=True,
    )

    hit_cap = bool(trade_info.get("hit_cap"))
    cap_reason = trade_info.get("cap_reason") or ""
    trade_pages = int(trade_info.get("pages_fetched") or 0)
    trade_records = int(trade_info.get("activity_records_fetched") or 0)
    trade_actions_cnt = int(
        trade_info.get("actions_count") or trade_info.get("unique_tx") or 0
    )

    suspected_hft = trade_actions_cnt >= args.hft_unique_tx_threshold

    if suspected_hft:
        hft_reason = (
            f"unique_tx/actions_count>={args.hft_unique_tx_threshold} "
            f"(actions={trade_actions_cnt}, records={trade_records}, pages={trade_pages})"
        )
    elif hit_cap and cap_reason:
        hft_reason = f"cap_hit_only: {cap_reason}"
    else:
        hft_reason = ""

    summary = None
    closed_positions = []
    open_positions = []
    closed_info: Dict[str, object] = {}
    open_info: Dict[str, object] = {}
    lifetime_realized_pnl_sum = None
    lifetime_closed_count = None
    lifetime_incomplete = None
    lifetime_status = None

    if suspected_hft:
        account_start_time = client.fetch_account_start_time_from_activity(user=addr)
        summary = summarize_user(
            [],
            [],
            user=addr,
            start_time=start,
            end_time=end,
            asof_time=now,
            account_start_time=account_start_time,
            lifetime_status="skipped",
        )
        summary.status = "hft_skipped_deep_fetch"
        summary.suspected_hft = True
        summary.hft_reason = hft_reason
        summary.trade_actions_pages = trade_pages
        summary.trade_actions_records = trade_records
        summary.trade_actions_actions = trade_actions_cnt
        status = summary.status
        incomplete = True
    else:
        closed_positions, closed_info = client.fetch_closed_positions_window(
            user=addr,
            start_time=start,
            end_time=end,
            return_info=True,
        )
        open_positions, open_info = client.fetch_positions(
            user=addr,
            size_threshold=args.size_threshold,
            return_info=True,
        )
        account_start_time = client.fetch_account_start_time_from_activity(user=addr)

        if args.lifetime_mode == "all":
            lifetime_closed_positions, lifetime_info = client.fetch_closed_positions(
                user=addr,
                return_info=True,
            )
            lifetime_realized_pnl_sum = sum(
                pos.realized_pnl for pos in lifetime_closed_positions
            )
            lifetime_closed_count = len(lifetime_closed_positions)
            lifetime_incomplete = bool(lifetime_info["incomplete"])
            lifetime_status = "ok" if not lifetime_incomplete else "incomplete"
        elif args.lifetime_mode == "candidates":
            lifetime_status = "skipped"
        else:
            lifetime_status = "disabled"

        summary = summarize_user(
            closed_positions,
            open_positions,
            user=addr,
            start_time=start,
            end_time=end,
            asof_time=now,
            account_start_time=account_start_time,
            lifetime_realized_pnl_sum=lifetime_realized_pnl_sum,
            lifetime_closed_count=lifetime_closed_count,
            lifetime_incomplete=lifetime_incomplete,
            lifetime_status=lifetime_status,
        )
        summary.status = "ok"
        summary.suspected_hft = False
        summary.hft_reason = hft_reason
        summary.trade_actions_pages = trade_pages
        summary.trade_actions_records = trade_records
        summary.trade_actions_actions = trade_actions_cnt

        trade_incomplete = bool(trade_info.get("incomplete"))
        incomplete = (
            bool(closed_info.get("incomplete"))
            or bool(open_info.get("incomplete"))
            or trade_incomplete
        )
        if args.lifetime_mode == "all":
            incomplete = incomplete or bool(lifetime_incomplete)
        summary.status = "ok" if not incomplete else "incomplete"
        status = summary.status

    summary_row = _summary_row(summary)
    closed_rows = [_closed_position_row(item) for item in closed_positions]
    open_rows = [_open_position_row(item) for item in open_positions]
    trade_rows = [_trade_action_row(item) for item in trade_actions]

    metrics = _build_features(
        addr,
        closed_rows,
        open_rows,
        summary_row,
        trade_rows,
        config,
    )
    row: Dict[str, object] = {"user": addr}
    row.update(metrics)
    row["copy_score"] = _compute_copy_score(row, config)

    passed, failures, warnings = _apply_filters(row, config)
    row["passed_filter"] = bool(passed)
    row["filter_failures"] = ";".join(failures) if failures else ""
    row["filter_warnings"] = ";".join(warnings) if warnings else ""

    print(f"[DEBUG] passed_filter={row['passed_filter']}", flush=True)
    print(f"[DEBUG] filter_failures={row['filter_failures']}", flush=True)
    print(f"[DEBUG] filter_warnings={row['filter_warnings']}", flush=True)

    label_rules = config.get("label_rules", {})
    price_rules = label_rules.get("price_style", {})
    copy_rules = label_rules.get("copy_style", {})
    final_output = config.get("final_output", {})
    final_columns = final_output.get("columns")
    final_rename = final_output.get("rename", {})

    enriched = dict(row)
    enriched["price_style"] = _build_price_style(enriched, price_rules)
    enriched["copy_style"] = _build_copy_style(enriched, copy_rules)
    enriched["notes"] = _build_notes(enriched, {**price_rules, **copy_rules})
    enriched["profile_url"] = (
        f"https://polymarket.com/profile/{str(row.get('user', '')).lower()}"
    )

    if final_columns:
        filtered = {col: enriched.get(col) for col in final_columns}
    else:
        filtered = enriched

    renamed = {final_rename.get(key, key): value for key, value in filtered.items()}

    _print_section("FINAL_OUTPUT")
    _write_rows(list(renamed.keys()), [renamed])

    _print_section("READABLE_OUTPUT")
    _print_readable_output(renamed)

    if not passed:
        print(
            "[WARN] 未通过筛选条件: "
            f"failures={row['filter_failures']} "
            f"warnings={row['filter_warnings']}",
            flush=True,
        )

    print("\n[INFO] 完成", flush=True)


if __name__ == "__main__":
    main()
