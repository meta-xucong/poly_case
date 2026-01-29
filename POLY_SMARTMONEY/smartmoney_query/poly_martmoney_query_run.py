"""
一键执行示例：使用 Data API 拉取 closed-positions + positions，统计交易笔数、
每笔盈亏、总盈亏与胜率，并写入 data/ 目录。
"""
import argparse
import csv
import datetime as dt
import json
import math
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from poly_martmoney_query.api_client import DataApiClient
from poly_martmoney_query.models import UserSummary
from poly_martmoney_query.processors import summarize_user
from poly_martmoney_query.storage import (
    update_user_summary,
    write_closed_positions_csv,
    write_positions_csv,
    write_trade_actions_csv,
    write_user_summaries_csv,
    write_user_summary_csv,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket smart money query runner")
    parser.add_argument("--user", help="单地址模式，指定要查询的钱包地址")
    parser.add_argument("--days", type=int, default=30, help="统计区间天数（默认 30）")
    parser.add_argument("--top", type=int, default=50, help="批量模式取 leaderboard 前 N 名")
    parser.add_argument("--period", default="MONTH", help="leaderboard 时间维度（默认 MONTH）")
    parser.add_argument("--order-by", default="pnl", help="leaderboard 排序字段（默认 pnl）")
    parser.add_argument(
        "--size-threshold",
        type=float,
        default=0.0,
        help="positions 的 sizeThreshold（默认 0）",
    )
    parser.add_argument(
        "--min-leaderboard-pnl",
        type=float,
        default=None,
        help="仅保留 leaderboard 已实现盈亏 >= 阈值的地址（默认不过滤）",
    )
    parser.add_argument(
        "--min-leaderboard-vol",
        type=float,
        default=None,
        help="仅保留 leaderboard 成交量 >= 阈值的地址（默认不过滤）",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="若 data/users/<addr>/summary.csv 已存在则跳过该地址",
    )
    parser.add_argument(
        "--lifetime-mode",
        choices=("all", "candidates", "none"),
        default="candidates",
        help="历史收益拉取模式：all=全量，candidates=仅候选补齐，none=跳过",
    )
    parser.add_argument(
        "--screen-config",
        default="screen_users_config.json",
        help="screen_users.py 使用的配置文件路径（默认 screen_users_config.json）",
    )
    parser.add_argument(
        "--no-auto-screen",
        action="store_true",
        help="关闭自动运行 screen_users.py（默认开启）",
    )
    parser.add_argument(
        "--keep-prescreen-output",
        action="store_true",
        help="保留预筛输出：users_features_prescreen.csv / candidates_prescreen.csv",
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
        default=700,
        help=(
            "判定为高频并跳过深拉的阈值，使用 trade_actions 的 unique_tx/天（默认 700）"
        ),
    )
    return parser.parse_args()


def _to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_metric(item: dict, keys: Iterable[str]) -> Optional[float]:
    for key in keys:
        if key in item:
            parsed = _to_float(item.get(key))
            if parsed is not None:
                return parsed
    return None


def _collect_users(
    client: DataApiClient, args: argparse.Namespace
) -> tuple[List[str], Dict[str, dict]]:
    if args.user:
        return [args.user], {}

    leaderboard_users = []
    leaderboard_map: Dict[str, dict] = {}
    seen = set()
    print(f"[INFO] 获取 leaderboard（{args.period}，按 {args.order_by}）……", flush=True)
    page_size = 50
    target = max(1, args.top)
    max_pages = math.ceil(target / page_size) + 2
    for item in client.iter_leaderboard(
        period=args.period,
        order_by=args.order_by,
        page_size=page_size,
        max_pages=max_pages,
    ):
        min_pnl = args.min_leaderboard_pnl
        if min_pnl is not None:
            pnl_value = _extract_metric(item, ("pnl", "profit", "PNL"))
            if pnl_value is not None and pnl_value < min_pnl:
                continue
        min_vol = args.min_leaderboard_vol
        if min_vol is not None:
            vol_value = _extract_metric(item, ("volume", "vol", "VOL"))
            if vol_value is not None and vol_value < min_vol:
                continue
        addr = item.get("proxyWallet") or item.get("address")
        if addr:
            normalized = addr.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            leaderboard_users.append(addr)
            leaderboard_map[normalized] = item
        if len(leaderboard_users) >= target:
            break

    if not leaderboard_users:
        raise SystemExit("未获取到 leaderboard 用户，检查网络或 API 可用性")
    if len(leaderboard_users) < target:
        print(
            f"[WARN] leaderboard 去重后仅获取到 {len(leaderboard_users)} 个地址，"
            f"未达目标 {target}。",
            flush=True,
        )

    return leaderboard_users, leaderboard_map


def _parse_datetime(value: str) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = f"{value[:-1]}+00:00"
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_int(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _parse_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_optional_float(value: str) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_optional_int(value: str) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _parse_optional_bool(value: str) -> Optional[bool]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text == "":
        return None
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _load_existing_summary(path: Path) -> Optional[UserSummary]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            row = next(reader, None)
            if not row:
                return None
            return UserSummary(
                user=row.get("user", ""),
                start_time=_parse_datetime(row.get("start_time", "")),
                end_time=_parse_datetime(row.get("end_time", "")),
                account_start_time=_parse_datetime(row.get("account_start_time", "")),
                account_age_days=_to_float(row.get("account_age_days")),
                leaderboard_month_pnl=_parse_optional_float(
                    row.get("leaderboard_month_pnl", "")
                ),
                suspected_hft=_parse_optional_bool(row.get("suspected_hft", "")),
                hft_reason=row.get("hft_reason") or None,
                trade_actions_pages=_parse_optional_int(
                    row.get("trade_actions_pages", "")
                ),
                trade_actions_records=_parse_optional_int(
                    row.get("trade_actions_records", "")
                ),
                trade_actions_actions=_parse_optional_int(
                    row.get("trade_actions_actions", "")
                ),
                lifetime_realized_pnl_sum=_parse_optional_float(
                    row.get("lifetime_realized_pnl_sum", "")
                ),
                lifetime_closed_count=_parse_optional_int(
                    row.get("lifetime_closed_count", "")
                ),
                lifetime_incomplete=_parse_optional_bool(
                    row.get("lifetime_incomplete", "")
                ),
                lifetime_status=row.get("lifetime_status") or None,
                closed_count=_parse_int(row.get("closed_count", "0")),
                closed_realized_pnl_sum=_parse_float(row.get("closed_realized_pnl_sum", "0")),
                win_count=_parse_int(row.get("win_count", "0")),
                loss_count=_parse_int(row.get("loss_count", "0")),
                flat_count=_parse_int(row.get("flat_count", "0")),
                win_rate_all=_to_float(row.get("win_rate_all")),
                win_rate_no_flat=_to_float(row.get("win_rate_no_flat")),
                open_count=_parse_int(row.get("open_count", "0")),
                open_unrealized_pnl_sum=_parse_float(row.get("open_unrealized_pnl_sum", "0")),
                open_realized_pnl_sum=_parse_float(row.get("open_realized_pnl_sum", "0")),
                asof_time=_parse_datetime(row.get("asof_time", "")) or dt.datetime.now(
                    tz=dt.timezone.utc
                ),
                status=row.get("status") or None,
            )
    except Exception:
        return None


def _load_candidate_users(path: Path) -> List[str]:
    if not path.exists():
        return []
    users: List[str] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if str(row.get("passed_filter", "")).strip().lower() != "true":
                    continue
                user = row.get("user")
                if user:
                    users.append(user)
    except Exception:
        return []
    return users


def _write_prescreen_config(src: Path, dst: Path) -> Path:
    cfg = json.loads(src.read_text(encoding="utf-8"))
    filters = cfg.get("filters") or {}
    if "min_lifetime_realized_pnl" in filters:
        filters["min_lifetime_realized_pnl"] = None
    cfg["filters"] = filters
    dst.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    return dst


def _run_screen_users(base_dir: Path, config_path: Path) -> bool:
    cmd = [sys.executable, str(base_dir / "screen_users.py"), "--config", str(config_path)]
    print(f"[INFO] 自动运行 screen_users.py：config={config_path}", flush=True)
    proc = subprocess.run(cmd, cwd=str(base_dir))
    if proc.returncode != 0:
        print(
            f"[WARN] screen_users.py 返回非 0：code={proc.returncode}，后续候选补齐可能跳过",
            flush=True,
        )
        return False
    return True


def _load_screening_filenames(config_path: Path) -> tuple[str, str]:
    default_features = "users_features.csv"
    default_candidates = "candidates.csv"
    if not config_path.exists():
        return default_features, default_candidates
    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return default_features, default_candidates
    features = cfg.get("features_filename") or default_features
    candidates = cfg.get("candidates_filename") or default_candidates
    return str(features), str(candidates)


def main() -> None:
    args = _parse_args()
    client = DataApiClient()
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    users_dir = data_dir / "users"
    data_dir.mkdir(exist_ok=True)
    users_dir.mkdir(exist_ok=True)

    now = dt.datetime.now(tz=dt.timezone.utc)
    start = now - dt.timedelta(days=args.days) if args.days > 0 else None
    end = now

    users, leaderboard_map = _collect_users(client, args)
    summaries = []
    report_rows: List[dict] = []
    lifetime_mode = args.lifetime_mode
    auto_screen = not args.no_auto_screen
    screen_config = (base_dir / args.screen_config).resolve()
    prescreen_config = (data_dir / "_screen_users_prescreen_config.json").resolve()

    for idx, addr in enumerate(users, start=1):
        print(f"[INFO] ({idx}/{len(users)}) 抓取地址 {addr} 的仓位数据……", flush=True)
        user_dir = users_dir / addr
        summary_path = user_dir / "summary.csv"
        if args.resume and summary_path.exists():
            existing_summary = _load_existing_summary(summary_path)
            if existing_summary is not None and (
                existing_summary.status == "ok" or bool(existing_summary.suspected_hft)
            ):
                summaries.append(existing_summary)
                report_rows.append(
                    {
                        "user": addr,
                        "status": "skipped",
                        "ok": True,
                        "incomplete": False,
                        "closed_count": existing_summary.closed_count,
                        "open_count": existing_summary.open_count,
                        "closed_incomplete": False,
                        "open_incomplete": False,
                        "closed_hit_max_pages": False,
                        "open_hit_max_pages": False,
                        "lifetime_status": existing_summary.lifetime_status or "",
                        "lifetime_incomplete": existing_summary.lifetime_incomplete
                        if existing_summary.lifetime_incomplete is not None
                        else "",
                        "leaderboard_month_pnl": existing_summary.leaderboard_month_pnl
                        if existing_summary.leaderboard_month_pnl is not None
                        else "",
                        "account_start_time": existing_summary.account_start_time.isoformat()
                        if existing_summary.account_start_time is not None
                        else "",
                        "account_age_days": existing_summary.account_age_days
                        if existing_summary.account_age_days is not None
                        else "",
                        "suspected_hft": 1
                        if existing_summary.suspected_hft
                        else 0
                        if existing_summary.suspected_hft is not None
                        else "",
                        "hft_reason": existing_summary.hft_reason or "",
                        "trade_actions_pages": existing_summary.trade_actions_pages
                        if existing_summary.trade_actions_pages is not None
                        else "",
                        "trade_actions_records": existing_summary.trade_actions_records
                        if existing_summary.trade_actions_records is not None
                        else "",
                        "trade_actions_actions": existing_summary.trade_actions_actions
                        if existing_summary.trade_actions_actions is not None
                        else "",
                        "error_msg": "resume-skip",
                    }
                )
                print(f"[INFO] 地址 {addr} 已存在 summary.csv，跳过。", flush=True)
                continue
            if existing_summary is None:
                print(
                    f"[WARN] 地址 {addr} summary.csv 无法解析，将重新抓取。",
                    flush=True,
                )
            else:
                print(
                    f"[WARN] 地址 {addr} summary.csv 状态为 {existing_summary.status or 'unknown'}，将重新抓取。",
                    flush=True,
                )

        try:
            entry = leaderboard_map.get(addr.lower(), {})
            lb_month_pnl = _extract_metric(entry, ("pnl", "realizedPnl", "profit"))
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
            window_days = max(1, args.days)
            unique_tx_per_day = trade_actions_cnt / window_days

            suspected_hft = bool(trade_info.get("suspected_hft")) or (
                unique_tx_per_day >= args.hft_unique_tx_threshold
            )

            if suspected_hft:
                if trade_info.get("suspected_hft") and cap_reason:
                    hft_reason = f"{cap_reason}"
                else:
                    hft_reason = (
                        f"unique_tx_per_day>={args.hft_unique_tx_threshold} "
                        f"(unique_tx={trade_actions_cnt}, days={window_days}, "
                        f"per_day={unique_tx_per_day:.2f}, records={trade_records}, "
                        f"pages={trade_pages})"
                    )
            elif hit_cap and cap_reason:
                hft_reason = f"cap_hit_only: {cap_reason}"
            else:
                hft_reason = ""

            if suspected_hft:
                account_start_time = client.fetch_account_start_time_from_activity(
                    user=addr
                )
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
                summary.leaderboard_month_pnl = lb_month_pnl
                summary.suspected_hft = True
                summary.hft_reason = hft_reason
                summary.trade_actions_pages = trade_pages
                summary.trade_actions_records = trade_records
                summary.trade_actions_actions = trade_actions_cnt
                summaries.append(summary)

                write_trade_actions_csv(user_dir / "trade_actions.csv", trade_actions)
                write_user_summary_csv(summary_path, summary)

                report_rows.append(
                    {
                        "user": addr,
                        "status": "hft_skipped_deep_fetch",
                        "ok": True,
                        "incomplete": False,
                        "closed_count": 0,
                        "open_count": 0,
                        "closed_incomplete": False,
                        "open_incomplete": False,
                        "closed_hit_max_pages": False,
                        "open_hit_max_pages": False,
                        "lifetime_status": "skipped",
                        "lifetime_incomplete": "",
                        "leaderboard_month_pnl": lb_month_pnl if lb_month_pnl is not None else "",
                        "account_start_time": summary.account_start_time.isoformat()
                        if summary.account_start_time is not None
                        else "",
                        "account_age_days": summary.account_age_days
                        if summary.account_age_days is not None
                        else "",
                        "suspected_hft": 1,
                        "hft_reason": hft_reason,
                        "trade_actions_pages": trade_pages,
                        "trade_actions_records": trade_records,
                        "trade_actions_actions": trade_actions_cnt,
                        "error_msg": "",
                    }
                )
                print(
                    f"[WARN] 地址 {addr} 命中高频阈值，已跳过深拉：{hft_reason}",
                    flush=True,
                )
                continue

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

            lifetime_realized_pnl_sum = None
            lifetime_closed_count = None
            lifetime_incomplete = None
            lifetime_status = None
            lifetime_info: dict = {"incomplete": False, "error_msg": None}
            if lifetime_mode == "all":
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
            elif lifetime_mode == "candidates":
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
            # === PATCH: make lifetime_realized_pnl_sum comparable to website "ALL" Profit/Loss ===
            # NOTE:
            # - lifetime_realized_pnl_sum is computed from lifetime closed positions (pos.realized_pnl)
            # - website "ALL" is a net P/L style number; so we fold in current open position PnL (realized + unrealized)
            if lifetime_realized_pnl_sum is not None:
                open_real = float(summary.open_realized_pnl_sum or 0.0)
                open_unreal = float(summary.open_unrealized_pnl_sum or 0.0)
                summary.lifetime_realized_pnl_sum = float(lifetime_realized_pnl_sum) + open_real + open_unreal
            summary.leaderboard_month_pnl = lb_month_pnl
            summary.suspected_hft = False
            summary.hft_reason = hft_reason
            summary.trade_actions_pages = trade_pages
            summary.trade_actions_records = trade_records
            summary.trade_actions_actions = trade_actions_cnt

            trade_incomplete = bool(trade_info.get("incomplete"))
            incomplete = (
                bool(closed_info["incomplete"]) or bool(open_info["incomplete"]) or trade_incomplete
            )
            if lifetime_mode == "all":
                incomplete = incomplete or bool(lifetime_info["incomplete"])
            status = "ok" if not incomplete else "incomplete"
            summary.status = status
            summaries.append(summary)

            write_closed_positions_csv(user_dir / "closed_positions.csv", closed_positions)
            write_trade_actions_csv(user_dir / "trade_actions.csv", trade_actions)
            write_positions_csv(user_dir / "positions.csv", open_positions)
            write_user_summary_csv(summary_path, summary)

            error_msg = "; ".join(
                msg
                for msg in [
                    closed_info.get("error_msg"),
                    trade_info.get("error_msg"),
                    open_info.get("error_msg"),
                    lifetime_info.get("error_msg") if lifetime_mode == "all" else None,
                ]
                if msg
            )
            report_rows.append(
                {
                    "user": addr,
                    "status": status,
                    "ok": not incomplete,
                    "incomplete": incomplete,
                    "closed_count": summary.closed_count,
                    "open_count": summary.open_count,
                    "closed_incomplete": bool(closed_info["incomplete"]),
                    "open_incomplete": bool(open_info["incomplete"]),
                    "closed_hit_max_pages": bool(closed_info["hit_max_pages"]),
                    "open_hit_max_pages": bool(open_info["hit_max_pages"]),
                    "lifetime_status": lifetime_status,
                    "lifetime_incomplete": bool(lifetime_incomplete)
                    if lifetime_incomplete is not None
                    else "",
                    "leaderboard_month_pnl": lb_month_pnl if lb_month_pnl is not None else "",
                    "account_start_time": summary.account_start_time.isoformat()
                    if summary.account_start_time is not None
                    else "",
                    "account_age_days": summary.account_age_days
                    if summary.account_age_days is not None
                    else "",
                    "suspected_hft": 0,
                    "hft_reason": hft_reason,
                    "trade_actions_pages": trade_pages,
                    "trade_actions_records": trade_records,
                    "trade_actions_actions": trade_actions_cnt,
                    "error_msg": error_msg,
                }
            )

            win_rate_text = (
                f"{summary.win_rate_all:.2%}" if summary.win_rate_all is not None else "N/A"
            )
            account_days_text = (
                f"{summary.account_age_days:.1f}天"
                if summary.account_age_days is not None
                else "N/A"
            )
            if summary.lifetime_realized_pnl_sum is not None:
                lifetime_text = f"{summary.lifetime_realized_pnl_sum:.4f}"
            elif lifetime_mode == "candidates":
                lifetime_text = "PENDING(candidates)"
            elif lifetime_mode == "none":
                lifetime_text = "DISABLED"
            else:
                lifetime_text = (summary.lifetime_status or "pending").upper()
            print(
                f"[INFO] 地址 {addr}：已平仓={summary.closed_count}，"
                f"已实现盈亏={summary.closed_realized_pnl_sum:.4f}，"
                f"持仓已实现={summary.open_realized_pnl_sum:.4f}，"
                f"持仓浮盈浮亏={summary.open_unrealized_pnl_sum:.4f}，"
                f"胜率={win_rate_text}，"
                f"账号年龄={account_days_text}，"
                f"历史总收益={lifetime_text}",
                flush=True,
            )
            if incomplete:
                print(
                    f"[WARN] 地址 {addr} 数据不完整：{error_msg or 'unknown_error'}",
                    flush=True,
                )
        except Exception as exc:
            report_rows.append(
                {
                    "user": addr,
                    "status": "error",
                    "ok": False,
                    "incomplete": True,
                    "closed_count": "",
                    "open_count": "",
                    "closed_incomplete": True,
                    "open_incomplete": True,
                    "closed_hit_max_pages": False,
                    "open_hit_max_pages": False,
                    "lifetime_status": "error",
                    "lifetime_incomplete": True,
                    "leaderboard_month_pnl": "",
                    "account_start_time": "",
                    "account_age_days": "",
                    "suspected_hft": "",
                    "hft_reason": "",
                    "trade_actions_pages": "",
                    "trade_actions_records": "",
                    "trade_actions_actions": "",
                    "error_msg": str(exc),
                }
            )
            print(f"[WARN] 地址 {addr} 处理失败：{exc}", flush=True)

    write_user_summaries_csv(data_dir / "users_summary.csv", summaries)
    if lifetime_mode == "candidates":
        features_filename, candidates_filename = _load_screening_filenames(screen_config)
        candidates_path = data_dir / candidates_filename
        if auto_screen:
            _write_prescreen_config(screen_config, prescreen_config)
            _run_screen_users(base_dir, prescreen_config)
            if args.keep_prescreen_output:
                f1 = data_dir / features_filename
                f2 = data_dir / candidates_filename
                if f1.exists():
                    shutil.copyfile(
                        f1, data_dir / f"{Path(features_filename).stem}_prescreen.csv"
                    )
                if f2.exists():
                    shutil.copyfile(
                        f2, data_dir / f"{Path(candidates_filename).stem}_prescreen.csv"
                    )
        candidate_users = _load_candidate_users(candidates_path)
        if not candidate_users:
            print(
                f"[WARN] 未检测到候选名单（{candidates_path}），跳过历史收益补齐。",
                flush=True,
            )
        else:
            summary_map = {summary.user.lower(): summary for summary in summaries}
            report_map = {row.get("user", "").lower(): row for row in report_rows}
            lifetime_start = dt.datetime.now(tz=dt.timezone.utc)
            success_count = 0
            failed_count = 0
            for idx, addr in enumerate(candidate_users, start=1):
                print(
                    f"[INFO] (候选{idx}/{len(candidate_users)}) 补抓历史收益：{addr}",
                    flush=True,
                )
                user_dir = users_dir / addr
                if not user_dir.exists():
                    print(
                        f"[WARN] 候选地址 {addr} 未找到用户目录，跳过。",
                        flush=True,
                    )
                    failed_count += 1
                    continue
                try:
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
                except Exception as exc:
                    print(f"[WARN] 候选地址 {addr} 补抓失败：{exc}", flush=True)
                    lifetime_realized_pnl_sum = None
                    lifetime_closed_count = None
                    lifetime_incomplete = True
                    lifetime_status = "error"

                if lifetime_status == "ok":
                    success_count += 1
                else:
                    failed_count += 1

                summary = summary_map.get(addr.lower())
                if summary is not None:
                    summary.lifetime_realized_pnl_sum = lifetime_realized_pnl_sum
                    summary.lifetime_closed_count = lifetime_closed_count
                    summary.lifetime_incomplete = lifetime_incomplete
                    summary.lifetime_status = lifetime_status

                report_row = report_map.get(addr.lower())
                if report_row is not None:
                    report_row["lifetime_status"] = lifetime_status
                    report_row["lifetime_incomplete"] = lifetime_incomplete

                patch = {
                    "lifetime_realized_pnl_sum": lifetime_realized_pnl_sum,
                    "lifetime_closed_count": lifetime_closed_count,
                    "lifetime_incomplete": lifetime_incomplete,
                    "lifetime_status": lifetime_status,
                }
                update_user_summary(user_dir / "summary.csv", addr, patch)

            write_user_summaries_csv(data_dir / "users_summary.csv", summaries)
            elapsed = dt.datetime.now(tz=dt.timezone.utc) - lifetime_start
            print(
                f"[INFO] 历史收益补齐完成：成功 {success_count}，失败 {failed_count}，耗时 {elapsed}.",
                flush=True,
            )
        if auto_screen:
            _run_screen_users(base_dir, screen_config)
            print(
                "[INFO] 已输出最终表：data/users_features.csv & data/candidates.csv（已包含 lifetime 字段）",
                flush=True,
            )
    report_path = data_dir / "run_report.csv"
    with report_path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
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
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)
    print(f"[INFO] 完成：已写入 {data_dir.resolve()}", flush=True)


if __name__ == "__main__":
    main()
