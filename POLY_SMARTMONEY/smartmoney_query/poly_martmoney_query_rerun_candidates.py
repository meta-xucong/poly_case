"""
一次性重拉指定候选用户的数据，避免全量重复抓取。

默认读取 screen_users_config.json，对 final_candidates.csv 中的用户重新拉取：
- closed positions
- open positions
- trade actions
- lifetime closed positions（可选）
并刷新 users_summary.csv 与每个用户目录下的 summary.csv。
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from poly_martmoney_query.api_client import DataApiClient
from poly_martmoney_query.processors import summarize_user
from poly_martmoney_query.storage import (
    SUMMARY_FIELDNAMES,
    _summary_row,
    _serialize_summary_value,
    write_closed_positions_csv,
    write_positions_csv,
    write_trade_actions_csv,
    write_user_summary_csv,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Re-query filtered users without fetching the entire leaderboard",
    )
    parser.add_argument(
        "--config",
        default="screen_users_config.json",
        help="筛选配置文件路径（默认 screen_users_config.json）",
    )
    parser.add_argument(
        "--users-file",
        default=None,
        help="用户列表 CSV 路径（默认优先 final_candidates.csv）",
    )
    parser.add_argument(
        "--user-column",
        default=None,
        help="用户列名（不填则自动识别 user/地址）",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="当 summary 缺失时间区间时使用的窗口天数（默认读取配置）",
    )
    parser.add_argument(
        "--size-threshold",
        type=float,
        default=0.0,
        help="positions 的 sizeThreshold（默认 0）",
    )
    parser.add_argument(
        "--trade-actions-page-size",
        type=int,
        default=300,
        help="trade_actions 拉取分页大小（默认 300）",
    )
    parser.add_argument(
        "--hft-unique-tx-threshold",
        type=int,
        default=700,
        help="高频阈值（unique_tx/天）（默认 700）",
    )
    parser.add_argument(
        "--lifetime-mode",
        choices=("all", "none"),
        default="all",
        help="历史收益拉取模式：all=全量，none=跳过（默认 all）",
    )
    parser.add_argument(
        "--rerun-screen",
        action="store_true",
        help="完成后自动运行 screen_users.py",
    )
    return parser.parse_args()


def _load_config(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"未找到配置文件：{path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_path(base: Path, value: Optional[str]) -> Optional[Path]:
    if not value:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = (base / path).resolve()
    return path


def _read_csv(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, reader.fieldnames or []


def _parse_datetime(value: str) -> Optional[dt.datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _parse_optional_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _detect_user_column(
    fieldnames: Iterable[str],
    rename_map: Dict[str, str],
    preferred: Optional[str],
) -> Optional[str]:
    if preferred:
        return preferred
    field_list = list(fieldnames)
    if "user" in field_list:
        return "user"
    renamed_user = rename_map.get("user")
    if renamed_user and renamed_user in field_list:
        return renamed_user
    for candidate in ("地址", "address", "wallet"):
        if candidate in field_list:
            return candidate
    return None


def _load_users_from_csv(path: Path, user_column: str) -> List[str]:
    rows, _ = _read_csv(path)
    users: List[str] = []
    seen = set()
    for row in rows:
        user = (row.get(user_column) or "").strip()
        if not user:
            continue
        key = user.lower()
        if key in seen:
            continue
        seen.add(key)
        users.append(user)
    return users


def _load_summary_map(path: Path) -> Dict[str, Dict[str, str]]:
    rows, _ = _read_csv(path)
    summary_map: Dict[str, Dict[str, str]] = {}
    for row in rows:
        user = row.get("user")
        if not user:
            continue
        summary_map[user.lower()] = row
    return summary_map


def _select_users_file(
    users_file: Optional[Path],
    output_dir: Path,
    data_dir: Path,
    config: Dict[str, object],
) -> Path:
    if users_file is not None:
        return users_file
    final_filename = str(config.get("final_filename", "final_candidates.csv"))
    candidates_filename = str(config.get("candidates_filename", "candidates.csv"))
    final_path = output_dir / final_filename
    if final_path.exists():
        return final_path
    candidates_path = output_dir / candidates_filename
    if candidates_path.exists():
        return candidates_path
    summary_path = data_dir / "users_summary.csv"
    if summary_path.exists():
        return summary_path
    raise FileNotFoundError("未找到可用的用户列表文件（final/candidates/users_summary）。")


def _compute_window_days(
    start_time: Optional[dt.datetime],
    end_time: Optional[dt.datetime],
    fallback_days: float,
) -> float:
    if start_time and end_time:
        delta = end_time - start_time
        days = max(delta.total_seconds() / 86400, 0.0)
        if days > 0:
            return days
    return float(fallback_days)


def _update_users_summary_csv(
    path: Path,
    summary_row: Dict[str, object],
) -> None:
    rows, fieldnames = _read_csv(path)
    if not fieldnames:
        fieldnames = list(SUMMARY_FIELDNAMES)
    else:
        for key in summary_row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    serialized = {key: _serialize_summary_value(key, value) for key, value in summary_row.items()}
    user_value = str(summary_row.get("user") or "").lower()
    updated = False
    for row in rows:
        if str(row.get("user") or "").lower() == user_value:
            row.update(serialized)
            updated = True
            break
    if not updated:
        rows.append(serialized)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = _parse_args()
    base_dir = Path(__file__).resolve().parent
    config_path = _resolve_path(base_dir, args.config)
    if config_path is None:
        raise SystemExit("配置文件路径为空")
    config = _load_config(config_path)

    data_dir = (base_dir / str(config.get("data_dir", "data"))).resolve()
    users_dir = (base_dir / str(config.get("users_dir", "data/users"))).resolve()
    output_dir = (base_dir / str(config.get("output_dir", "data"))).resolve()

    users_file = _resolve_path(base_dir, args.users_file)
    selected_users_file = _select_users_file(users_file, output_dir, data_dir, config)

    rename_map = {}
    final_output = config.get("final_output", {})
    if isinstance(final_output, dict):
        rename_map = final_output.get("rename", {}) or {}

    rows, fieldnames = _read_csv(selected_users_file)
    user_column = _detect_user_column(fieldnames, rename_map, args.user_column)
    if not user_column:
        raise SystemExit(
            f"无法识别用户列名，请使用 --user-column 指定（文件：{selected_users_file}）"
        )

    users = _load_users_from_csv(selected_users_file, user_column)
    if not users:
        raise SystemExit(f"未在 {selected_users_file} 中找到用户列表")

    summary_path = data_dir / "users_summary.csv"
    summary_map = _load_summary_map(summary_path)

    fallback_days = args.days
    if fallback_days is None:
        fallback_days = int(config.get("window_days_default", 30))

    client = DataApiClient()
    now = dt.datetime.now(tz=dt.timezone.utc)

    print(
        f"[INFO] 读取 {selected_users_file}，共 {len(users)} 个用户。"
        f" 窗口回退={fallback_days} 天，lifetime_mode={args.lifetime_mode}",
        flush=True,
    )

    for idx, user in enumerate(users, start=1):
        user_key = user.lower()
        summary_row = summary_map.get(user_key, {})
        start_time = _parse_datetime(summary_row.get("start_time", ""))
        end_time = _parse_datetime(summary_row.get("end_time", ""))
        if end_time is None:
            end_time = now
        if start_time is None:
            start_time = end_time - dt.timedelta(days=fallback_days)

        window_days = _compute_window_days(start_time, end_time, fallback_days)
        user_dir = users_dir / user
        user_dir.mkdir(parents=True, exist_ok=True)

        print(
            f"[INFO] ({idx}/{len(users)}) 重拉 {user} "
            f"window={start_time.isoformat()} ~ {end_time.isoformat()}",
            flush=True,
        )

        trade_actions, trade_info = client.fetch_trade_actions_window_from_activity(
            user=user,
            start_time=start_time,
            end_time=end_time,
            page_size=args.trade_actions_page_size,
            progress_every=20,
            return_info=True,
        )
        trade_pages = int(trade_info.get("pages_fetched") or 0)
        trade_records = int(trade_info.get("activity_records_fetched") or 0)
        trade_actions_cnt = int(
            trade_info.get("actions_count") or trade_info.get("unique_tx") or 0
        )
        unique_tx_per_day = trade_actions_cnt / max(window_days, 1.0)
        suspected_hft = bool(trade_info.get("suspected_hft")) or (
            unique_tx_per_day >= args.hft_unique_tx_threshold
        )
        hit_cap = bool(trade_info.get("hit_cap"))
        cap_reason = trade_info.get("cap_reason") or ""
        if suspected_hft:
            if trade_info.get("suspected_hft") and cap_reason:
                hft_reason = f"{cap_reason}"
            else:
                hft_reason = (
                    f"unique_tx_per_day>={args.hft_unique_tx_threshold} "
                    f"(unique_tx={trade_actions_cnt}, days={window_days:.2f}, "
                    f"per_day={unique_tx_per_day:.2f}, records={trade_records}, "
                    f"pages={trade_pages})"
                )
        elif hit_cap and cap_reason:
            hft_reason = f"cap_hit_only: {cap_reason}"
        else:
            hft_reason = ""

        closed_positions, closed_info = client.fetch_closed_positions_window(
            user=user,
            start_time=start_time,
            end_time=end_time,
            return_info=True,
        )
        open_positions, open_info = client.fetch_positions(
            user=user,
            size_threshold=args.size_threshold,
            return_info=True,
        )
        account_start_time = client.fetch_account_start_time_from_activity(user=user)

        lifetime_realized_pnl_sum = None
        lifetime_closed_count = None
        lifetime_incomplete = None
        lifetime_status = None
        lifetime_info: Dict[str, object] = {"incomplete": False, "error_msg": None}
        if args.lifetime_mode == "all":
            lifetime_closed_positions, lifetime_info = client.fetch_closed_positions(
                user=user,
                return_info=True,
            )
            lifetime_realized_pnl_sum = sum(
                pos.realized_pnl for pos in lifetime_closed_positions
            )
            lifetime_closed_count = len(lifetime_closed_positions)
            lifetime_incomplete = bool(lifetime_info["incomplete"])
            lifetime_status = "ok" if not lifetime_incomplete else "incomplete"
        else:
            lifetime_status = "disabled"

        summary = summarize_user(
            closed_positions,
            open_positions,
            user=user,
            start_time=start_time,
            end_time=end_time,
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
            summary.lifetime_realized_pnl_sum = (
                float(lifetime_realized_pnl_sum) + open_real + open_unreal
            )

        summary.leaderboard_month_pnl = _parse_optional_float(
            summary_row.get("leaderboard_month_pnl")
        )
        summary.suspected_hft = suspected_hft
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
            incomplete = incomplete or bool(lifetime_info.get("incomplete"))
        summary.status = "ok" if not incomplete else "incomplete"

        write_closed_positions_csv(user_dir / "closed_positions.csv", closed_positions)
        write_positions_csv(user_dir / "positions.csv", open_positions)
        write_trade_actions_csv(user_dir / "trade_actions.csv", trade_actions)
        write_user_summary_csv(user_dir / "summary.csv", summary)

        _update_users_summary_csv(summary_path, _summary_row(summary))

    print(f"[INFO] 完成重拉：更新用户数={len(users)}，summary={summary_path}")

    if args.rerun_screen:
        screen_script = base_dir / "screen_users.py"
        if screen_script.exists():
            import subprocess

            print("[INFO] 重新运行 screen_users.py ...", flush=True)
            subprocess.run(["python", str(screen_script), "--config", str(config_path)], check=False)
        else:
            print("[WARN] 未找到 screen_users.py，跳过 rerun-screen")


if __name__ == "__main__":
    main()
