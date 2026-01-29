"""
读取 poly_martmoney_query_run.py 输出的 CSV，生成用户特征表与候选名单。
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Screen Polymarket smart money users")
    parser.add_argument(
        "--config",
        default="screen_users_config.json",
        help="配置文件路径（默认 screen_users_config.json）",
    )
    return parser.parse_args()


def _load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"未找到配置文件：{path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_float(value: str) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_datetime(value: str) -> Optional[dt.datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
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


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    if q <= 0:
        return min(values)
    if q >= 1:
        return max(values)
    values_sorted = sorted(values)
    idx = (len(values_sorted) - 1) * q
    lower = int(idx)
    upper = min(lower + 1, len(values_sorted) - 1)
    if lower == upper:
        return values_sorted[lower]
    weight = idx - lower
    return values_sorted[lower] * (1 - weight) + values_sorted[upper] * weight


def _mean(values: Iterable[float]) -> Optional[float]:
    values_list = list(values)
    if not values_list:
        return None
    return sum(values_list) / len(values_list)


def _median(values: List[float]) -> Optional[float]:
    return _percentile(values, 0.5)


def _safe_ratio(numerator: float, denominator: float) -> Optional[float]:
    if denominator == 0:
        return None
    return numerator / denominator


def _load_user_summary_map(path: Path) -> Dict[str, Dict[str, str]]:
    summaries = {}
    for row in _read_csv(path):
        user = row.get("user")
        if user:
            summaries[user] = row
    return summaries


def _extract_summary_times(summary: Dict[str, str]) -> Tuple[Optional[dt.datetime], Optional[dt.datetime]]:
    start_time = _parse_datetime(summary.get("start_time", ""))
    end_time = _parse_datetime(summary.get("end_time", ""))
    return start_time, end_time


def _calculate_window_days(
    start_time: Optional[dt.datetime],
    end_time: Optional[dt.datetime],
    default_days: float,
) -> float:
    if start_time and end_time:
        delta = end_time - start_time
        days = max(delta.total_seconds() / 86400, 0.0)
        if days > 0:
            return days
    return float(default_days)


def _collect_daily_counts(timestamps: List[dt.datetime]) -> Dict[dt.date, int]:
    daily_counts: Dict[dt.date, int] = {}
    for ts in timestamps:
        day = ts.date()
        daily_counts[day] = daily_counts.get(day, 0) + 1
    return daily_counts


def _collect_minute_counts(timestamps: List[dt.datetime]) -> Dict[dt.datetime, int]:
    minute_counts: Dict[dt.datetime, int] = {}
    for ts in timestamps:
        minute_bucket = ts.replace(second=0, microsecond=0)
        minute_counts[minute_bucket] = minute_counts.get(minute_bucket, 0) + 1
    return minute_counts


def _compute_burstiness(daily_counts: Dict[dt.date, int]) -> Optional[float]:
    if not daily_counts:
        return None
    counts = list(daily_counts.values())
    mean_daily = _mean(counts)
    if mean_daily in (None, 0):
        return None
    return max(counts) / mean_daily


def _compute_intervals_minutes(timestamps: List[dt.datetime]) -> List[float]:
    if len(timestamps) < 2:
        return []
    timestamps_sorted = sorted(timestamps)
    intervals = []
    for prev, nxt in zip(timestamps_sorted, timestamps_sorted[1:]):
        delta = nxt - prev
        intervals.append(delta.total_seconds() / 60)
    return intervals


def _normalize(value: Optional[float], clamp: Optional[float]) -> float:
    if value is None:
        return 0.0
    if clamp is None or clamp <= 0:
        return value
    return max(min(value, clamp), 0.0) / clamp


def _compute_copy_score(metrics: Dict[str, Any], config: Dict[str, Any]) -> float:
    weights = config.get("score_weights", {})
    clamps = config.get("score_clamps", {})
    score = 0.0
    for key, weight in weights.items():
        if not isinstance(weight, (int, float)):
            continue
        value = metrics.get(key)
        norm_value = _normalize(value, clamps.get(key))
        score += weight * norm_value
    return score


def _clamp01(value: float) -> float:
    if value <= 0:
        return 0.0
    if value >= 1:
        return 1.0
    return value


def _tanh01(value: float) -> float:
    import math

    return 0.5 * (math.tanh(value) + 1.0)


def _safe_div(numerator: float, denominator: float, eps: float = 1e-9) -> float:
    if abs(denominator) <= eps:
        return numerator / eps
    return numerator / denominator


def _compute_max_drawdown(daily_pnls: List[float]) -> Tuple[float, List[float]]:
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    drawdowns: List[float] = []
    for pnl in daily_pnls:
        cum += float(pnl)
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
        drawdowns.append(dd)
    return max_dd, drawdowns


def _compute_ulcer_index(drawdowns: List[float]) -> float:
    import math

    if not drawdowns:
        return 0.0
    mean_sq = sum(dd * dd for dd in drawdowns) / len(drawdowns)
    return math.sqrt(mean_sq)


def _compute_stability_score(metrics: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, float]:
    stability_params = config.get("stability_params", {})
    if not isinstance(stability_params, dict):
        stability_params = {}
    rate_cap = float(stability_params.get("rate_cap", 100.0))
    share_cap = float(stability_params.get("share_cap", 0.60))
    surge_cap = float(stability_params.get("surge_cap", 5.0))
    dd_ratio_cap = float(stability_params.get("dd_ratio_cap", 1.0))
    conc_cap = float(stability_params.get("conc_cap", 0.70))

    age_days = metrics.get("account_age_days")
    lifetime_pnl = metrics.get("lifetime_realized_pnl_sum")
    month_pnl = metrics.get("leaderboard_month_pnl")

    lifetime_rate = 0.0
    recent_pnl_share = 0.0
    recent_surge_ratio = 0.0
    lifetime_score = 0.0

    if isinstance(age_days, (int, float)) and age_days and isinstance(
        lifetime_pnl, (int, float)
    ):
        lifetime_rate = float(lifetime_pnl) / max(float(age_days), 1.0)
        if lifetime_rate <= 0:
            lifetime_rate_score = 0.0
        else:
            lifetime_rate_score = _clamp01(_tanh01(lifetime_rate / rate_cap))

        if isinstance(month_pnl, (int, float)):
            recent_pnl_share = abs(float(month_pnl)) / max(abs(float(lifetime_pnl)), 1e-9)
            share_score = 1.0 - _clamp01(recent_pnl_share / share_cap)

            month_rate = abs(float(month_pnl)) / 30.0
            hist_rate = abs(lifetime_rate)
            recent_surge_ratio = _safe_div(month_rate, hist_rate, eps=1e-9)
            surge_over = max(recent_surge_ratio - 1.0, 0.0)
            surge_score = 1.0 - _clamp01(surge_over / surge_cap)

            lifetime_score = 0.40 * lifetime_rate_score + 0.30 * share_score + 0.30 * surge_score
        else:
            lifetime_score = lifetime_rate_score

    profit_day_ratio = float(metrics.get("profit_day_ratio") or 0.0)
    max_drawdown_ratio = float(metrics.get("max_drawdown_ratio") or 0.0)
    pnl_top1_day_share = float(metrics.get("pnl_top1_day_share") or 0.0)
    sharpe_like = float(metrics.get("daily_sharpe_like") or 0.0)

    dd_score = 1.0 - _clamp01(max_drawdown_ratio / dd_ratio_cap)
    conc_score = 1.0 - _clamp01(pnl_top1_day_share / conc_cap)
    sharpe_score = _clamp01(_tanh01(max(sharpe_like, 0.0) / 2.0))

    window_score = 0.35 * profit_day_ratio + 0.35 * dd_score + 0.15 * conc_score + 0.15 * sharpe_score

    if lifetime_score > 0:
        stability_score = 0.70 * lifetime_score + 0.30 * window_score
    else:
        stability_score = window_score

    return {
        "stability_score": float(stability_score),
        "lifetime_rate": float(lifetime_rate),
        "recent_pnl_share": float(recent_pnl_share),
        "recent_surge_ratio": float(recent_surge_ratio),
        "profit_day_ratio": float(profit_day_ratio),
        "max_drawdown_ratio": float(max_drawdown_ratio),
        "pnl_top1_day_share": float(pnl_top1_day_share),
        "daily_sharpe_like": float(sharpe_like),
    }


def _apply_filters(
    metrics: Dict[str, Any], config: Dict[str, Any]
) -> Tuple[bool, List[str], List[str]]:
    filters = config.get("filters", {})
    label_rules = config.get("label_rules", {})
    copy_rules = label_rules.get("copy_style", {})
    failures = []
    warnings = []

    if str(metrics.get("suspected_hft", "")).strip() in ("1", "true", "True"):
        failures.append("suspected_hft_unique_tx")
        return False, failures, warnings

    if config.get("require_action_timestamps"):
        min_action_timing = int(config.get("min_action_timing_count", 10))
        if min_action_timing < 1:
            min_action_timing = 1
        action_timing_count = metrics.get("action_timing_count")
        if action_timing_count is None or action_timing_count < min_action_timing:
            failures.append(f"action_timing_count<{min_action_timing}")
            return False, failures, warnings

    def _check_min(key: str, label: str) -> None:
        threshold = filters.get(key)
        value = metrics.get(label)
        if threshold is None:
            return
        if value is None or value < threshold:
            failures.append(f"{label}<{threshold}")

    def _check_max(key: str, label: str) -> None:
        threshold = filters.get(key)
        value = metrics.get(label)
        if threshold is None:
            return
        if value is None or value > threshold:
            failures.append(f"{label}>{threshold}")

    _check_min("min_closed_count", "closed_count")
    _check_min("min_bayes_win_rate", "bayes_win_rate")
    _check_min("min_median_roi", "median_roi")
    _check_min("min_mid_ratio", "mid_ratio")
    _check_min("min_interval_median_minutes", "interval_median_minutes")
    _check_min("min_account_age_days", "account_age_days")
    _check_max("max_trades_per_day", "trades_per_day")
    _check_max("max_daily_trades", "max_trades_per_day")
    _check_max("max_p90_cost", "p90_cost")
    _check_max("max_cost", "max_cost")
    _check_max("max_open_exposure", "open_exposure")
    _check_max("max_tail_high_ratio", "tail_high_ratio")
    _check_max("max_tail_low_ratio", "tail_low_ratio")

    max_minute_burst_ratio = filters.get("max_minute_burst_ratio")
    if max_minute_burst_ratio is not None:
        minute_burst_ratio = metrics.get("minute_burst_ratio")
        near_expiry_ratio = metrics.get("near_expiry_ratio") or 0.0
        near_expiry_high = float(copy_rules.get("near_expiry_ratio_high", 0.3))
        if minute_burst_ratio is None:
            failures.append(f"minute_burst_ratio>{max_minute_burst_ratio}")
        elif minute_burst_ratio > max_minute_burst_ratio:
            if near_expiry_ratio >= near_expiry_high:
                warnings.append("minute_burst_ratio_high_but_near_expiry")
            else:
                failures.append(f"minute_burst_ratio>{max_minute_burst_ratio}")

    max_loss_threshold = filters.get("max_loss")
    max_loss = metrics.get("max_loss")
    if max_loss_threshold is not None:
        if max_loss is None:
            failures.append(f"max_loss<{max_loss_threshold}")
        elif max_loss < max_loss_threshold:
            failures.append(f"max_loss<{max_loss_threshold}")

    min_lifetime_pnl = filters.get("min_lifetime_realized_pnl")
    lifetime_pnl = metrics.get("lifetime_realized_pnl_sum")
    lifetime_status = metrics.get("lifetime_status")

    if min_lifetime_pnl is not None:
        # 总收益必须可用且为 ok；否则直接淘汰（杜绝 pending/skipped/error 账号混入最终表）
        if lifetime_status != "ok" or lifetime_pnl is None:
            failures.append("lifetime_required_but_missing_or_not_ok")
        elif lifetime_pnl <= min_lifetime_pnl:
            failures.append(f"lifetime_realized_pnl_sum<={min_lifetime_pnl}")

    return (len(failures) == 0), failures, warnings


def _build_features(
    user: str,
    closed_rows: List[Dict[str, str]],
    open_rows: List[Dict[str, str]],
    summary_row: Optional[Dict[str, str]],
    trade_action_rows: List[Dict[str, str]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    flat_eps = float(config.get("flat_pnl_epsilon", 1e-9))
    min_cost_for_roi = float(config.get("min_cost_for_roi", 1.0))
    bayes_alpha = float(config.get("bayes_alpha", 2.0))
    bayes_beta = float(config.get("bayes_beta", 2.0))
    price_bands = config.get("price_bands", {})
    tail_high = float(price_bands.get("tail_high", 0.9))
    tail_low = float(price_bands.get("tail_low", 0.1))
    mid_low = float(price_bands.get("mid_low", 0.2))
    mid_high = float(price_bands.get("mid_high", 0.8))

    timestamps: List[dt.datetime] = []
    pnls: List[float] = []
    costs: List[float] = []
    roi_values: List[float] = []
    prices: List[float] = []
    daily_pnl: Dict[dt.date, float] = {}

    win_count = 0
    loss_count = 0
    flat_count = 0

    for row in closed_rows:
        pnl = _parse_float(row.get("realized_pnl", ""))
        avg_price = _parse_float(row.get("avg_price", ""))
        total_bought = _parse_float(row.get("total_bought", ""))
        ts = _parse_datetime(row.get("timestamp", ""))

        if pnl is not None:
            pnls.append(pnl)
            if pnl > flat_eps:
                win_count += 1
            elif pnl < -flat_eps:
                loss_count += 1
            else:
                flat_count += 1

        if avg_price is not None:
            prices.append(avg_price)
            if total_bought is not None:
                cost = avg_price * total_bought
                costs.append(cost)
                if pnl is not None and cost >= min_cost_for_roi:
                    roi_values.append(pnl / cost)

        if ts is not None:
            timestamps.append(ts)
            if pnl is not None:
                day = ts.date()
                daily_pnl[day] = daily_pnl.get(day, 0.0) + pnl

    action_timestamps: List[dt.datetime] = []
    for row in trade_action_rows:
        ts = _parse_datetime(row.get("timestamp", ""))
        if ts is not None:
            action_timestamps.append(ts)

    min_action_timing = int(config.get("min_action_timing_count", 10))
    if min_action_timing < 1:
        min_action_timing = 1
    require_action_timestamps = bool(config.get("require_action_timestamps"))
    timing_timestamps = action_timestamps
    if not require_action_timestamps and len(action_timestamps) < min_action_timing:
        timing_timestamps = timestamps
    timing_count = len(timing_timestamps)

    closed_count = len(closed_rows)
    win_rate_no_flat = None
    if win_count + loss_count > 0:
        win_rate_no_flat = win_count / (win_count + loss_count)

    bayes_win_rate = None
    if win_count + loss_count > 0:
        bayes_win_rate = (win_count + bayes_alpha) / (
            win_count + loss_count + bayes_alpha + bayes_beta
        )

    window_days = float(config.get("window_days_default", 30))
    asof_time = None
    start_time = None
    end_time = None
    if summary_row:
        start_time, end_time = _extract_summary_times(summary_row)
        window_days = _calculate_window_days(start_time, end_time, window_days)
        asof_time = _parse_datetime(summary_row.get("asof_time", ""))

    account_age_days = None
    lifetime_realized_pnl_sum = None
    lifetime_status = None
    suspected_hft = None
    hft_reason = None
    trade_actions_pages = None
    trade_actions_records = None
    trade_actions_actions = None
    leaderboard_month_pnl = None
    if summary_row:
        account_age_days = _parse_float(summary_row.get("account_age_days", ""))
        lifetime_realized_pnl_sum = _parse_float(
            summary_row.get("lifetime_realized_pnl_sum", "")
        )
        lifetime_status = summary_row.get("lifetime_status") or None
        suspected_hft = summary_row.get("suspected_hft")
        hft_reason = summary_row.get("hft_reason") or None
        trade_actions_pages = _parse_float(summary_row.get("trade_actions_pages", ""))
        trade_actions_records = _parse_float(
            summary_row.get("trade_actions_records", "")
        )
        trade_actions_actions = _parse_float(
            summary_row.get("trade_actions_actions", "")
        )
        leaderboard_month_pnl = _parse_float(summary_row.get("leaderboard_month_pnl", ""))

    trades_per_day = None
    if window_days > 0:
        trades_per_day = timing_count / window_days

    daily_counts = _collect_daily_counts(timing_timestamps)
    max_trades_per_day = max(daily_counts.values()) if daily_counts else None
    p95_trades_per_day = _percentile(list(daily_counts.values()), 0.95)
    burstiness = _compute_burstiness(daily_counts)

    minute_counts = _collect_minute_counts(timing_timestamps)
    max_minute_trades = max(minute_counts.values()) if minute_counts else None
    minute_burst_ratio = (
        max_minute_trades / timing_count if timing_count > 0 and max_minute_trades else None
    )

    intervals_minutes = _compute_intervals_minutes(timing_timestamps)
    interval_p10 = _percentile(intervals_minutes, 0.1)
    interval_median = _median(intervals_minutes)

    mean_pnl = _mean(pnls)
    median_pnl = _median(pnls)
    max_loss = min(pnls) if pnls else None

    loss_values = [p for p in pnls if p < 0]
    p95_loss = _percentile(loss_values, 0.95)

    mean_cost = _mean(costs)
    median_cost = _median(costs)
    p90_cost = _percentile(costs, 0.9)
    max_cost = max(costs) if costs else None
    sum_cost = sum(costs) if costs else None

    mean_roi = _mean(roi_values)
    median_roi = _median(roi_values)

    win_pnl_sum = sum(p for p in pnls if p > 0)
    loss_pnl_sum = sum(p for p in pnls if p < 0)
    profit_factor = None
    if loss_pnl_sum < 0:
        profit_factor = win_pnl_sum / abs(loss_pnl_sum)
    elif win_pnl_sum > 0:
        profit_factor = float("inf")

    open_values: List[float] = []
    open_end_dates: List[dt.datetime] = []
    for row in open_rows:
        current_value = _parse_float(row.get("current_value", ""))
        if current_value is not None:
            open_values.append(current_value)
        end_date = _parse_datetime(row.get("end_date", ""))
        if end_date is not None:
            open_end_dates.append(end_date)

    open_exposure = sum(open_values) if open_values else 0.0
    open_count = len(open_rows)
    top1_current_value = max(open_values) if open_values else None
    concentration = (
        _safe_ratio(top1_current_value, open_exposure) if open_exposure > 0 else None
    )

    near_expiry_days = float(config.get("near_expiry_days", 3))
    if asof_time is None:
        asof_time = dt.datetime.now(tz=dt.timezone.utc)

    end_day = (end_time or asof_time).date()
    if start_time:
        start_day = start_time.date()
    else:
        back_days = int(max(1.0, float(window_days) + 0.9999))
        start_day = (asof_time - dt.timedelta(days=back_days)).date()

    total_days = (end_day - start_day).days + 1
    if total_days <= 0:
        total_days = 1

    daily_series: List[float] = []
    profit_days = 0
    sum_daily = 0.0
    for i in range(total_days):
        day = start_day + dt.timedelta(days=i)
        pnl = float(daily_pnl.get(day, 0.0))
        daily_series.append(pnl)
        sum_daily += pnl
        if pnl > flat_eps:
            profit_days += 1

    profit_day_ratio = profit_days / float(total_days)

    if total_days >= 2:
        mean_daily = sum_daily / float(total_days)
        var = sum((p - mean_daily) ** 2 for p in daily_series) / float(total_days)
        std_daily = var ** 0.5
    else:
        mean_daily = sum_daily
        std_daily = 0.0

    daily_sharpe_like = mean_daily / (std_daily + 1e-9)

    pos_sum = sum(max(p, 0.0) for p in daily_series)

    max_drawdown, drawdown_series = _compute_max_drawdown(daily_series)
    drawdown_denom = max(abs(sum_daily), pos_sum, 1.0)
    max_drawdown_ratio = max_drawdown / drawdown_denom
    top1 = max((max(p, 0.0) for p in daily_series), default=0.0)
    if pos_sum <= 1e-9:
        pnl_top1_day_share = 1.0
    else:
        pnl_top1_day_share = top1 / pos_sum

    ulcer_index = _compute_ulcer_index(drawdown_series)

    near_expiry_value = 0.0
    for row in open_rows:
        end_date = _parse_datetime(row.get("end_date", ""))
        current_value = _parse_float(row.get("current_value", ""))
        if end_date is None or current_value is None:
            continue
        seconds_to_expiry = (end_date - asof_time).total_seconds()
        if 0 <= seconds_to_expiry <= near_expiry_days * 86400:
            near_expiry_value += current_value

    near_expiry_ratio = (
        near_expiry_value / open_exposure if open_exposure > 0 else None
    )

    tail_high_ratio = (
        sum(1 for p in prices if p >= tail_high) / len(prices) if prices else None
    )
    tail_low_ratio = (
        sum(1 for p in prices if p <= tail_low) / len(prices) if prices else None
    )
    mid_ratio = (
        sum(1 for p in prices if mid_low <= p <= mid_high) / len(prices)
        if prices
        else None
    )
    price_median = _median(prices)

    metrics: Dict[str, Any] = {
        "closed_count": float(closed_count),
        "win_count": float(win_count),
        "loss_count": float(loss_count),
        "flat_count": float(flat_count),
        "win_rate_no_flat": win_rate_no_flat,
        "bayes_win_rate": bayes_win_rate,
        "trades_per_day": trades_per_day,
        "max_trades_per_day": float(max_trades_per_day) if max_trades_per_day else None,
        "p95_trades_per_day": p95_trades_per_day,
        "burstiness": burstiness,
        "minute_burst_ratio": minute_burst_ratio,
        "minute_burst_max": float(max_minute_trades) if max_minute_trades else None,
        "interval_p10_minutes": interval_p10,
        "interval_median_minutes": interval_median,
        "mean_pnl": mean_pnl,
        "median_pnl": median_pnl,
        "max_loss": max_loss,
        "p95_loss": p95_loss,
        "mean_cost": mean_cost,
        "median_cost": median_cost,
        "p90_cost": p90_cost,
        "max_cost": max_cost,
        "sum_cost": sum_cost,
        "mean_roi": mean_roi,
        "median_roi": median_roi,
        "profit_factor": profit_factor,
        "open_exposure": open_exposure,
        "open_count": float(open_count),
        "concentration": concentration,
        "near_expiry_ratio": near_expiry_ratio,
        "tail_high_ratio": tail_high_ratio,
        "tail_low_ratio": tail_low_ratio,
        "mid_ratio": mid_ratio,
        "price_median": price_median,
        "account_age_days": account_age_days,
        "lifetime_realized_pnl_sum": lifetime_realized_pnl_sum,
        "lifetime_status": lifetime_status,
        "suspected_hft": suspected_hft,
        "hft_reason": hft_reason,
        "trade_actions_pages": trade_actions_pages,
        "trade_actions_records": trade_actions_records,
        "trade_actions_actions": trade_actions_actions,
        "leaderboard_month_pnl": leaderboard_month_pnl,
        "action_timing_count": len(action_timestamps),
        "profit_day_ratio": profit_day_ratio,
        "daily_sharpe_like": daily_sharpe_like,
        "max_drawdown": max_drawdown,
        "max_drawdown_ratio": max_drawdown_ratio,
        "pnl_top1_day_share": pnl_top1_day_share,
        "ulcer_index": ulcer_index,
    }

    return metrics


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8", newline="") as f:
            f.write("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _build_price_style(metrics: Dict[str, Optional[float]], rules: Dict[str, Any]) -> str:
    tail_high_ratio = metrics.get("tail_high_ratio") or 0.0
    tail_low_ratio = metrics.get("tail_low_ratio") or 0.0
    mid_ratio = metrics.get("mid_ratio") or 0.0

    tail_high_threshold = float(rules.get("tail_high_ratio_tail", 0.6))
    tail_low_threshold = float(rules.get("tail_low_ratio_longshot", 0.25))
    mid_threshold = float(rules.get("mid_ratio_balanced", 0.4))

    if tail_high_ratio >= tail_high_threshold:
        return "尾单偏多"
    if tail_low_ratio >= tail_low_threshold:
        return "长shot偏多"
    if mid_ratio >= mid_threshold:
        return "均衡"
    return "混合"


def _build_copy_style(metrics: Dict[str, Optional[float]], rules: Dict[str, Any]) -> str:
    minute_burst_ratio = metrics.get("minute_burst_ratio") or 0.0
    interval_median = metrics.get("interval_median_minutes") or 0.0
    trades_per_day = metrics.get("trades_per_day") or 0.0
    burstiness = metrics.get("burstiness") or 0.0
    near_expiry_ratio = metrics.get("near_expiry_ratio") or 0.0
    recent_pnl_share = metrics.get("recent_pnl_share") or 0.0
    recent_surge_ratio = metrics.get("recent_surge_ratio") or 0.0

    minute_burst_threshold = float(rules.get("minute_burst_ratio_high", 0.25))
    interval_fast = float(rules.get("interval_median_minutes_fast", 2))
    trades_per_day_high = float(rules.get("trades_per_day_high", 25))
    burstiness_high = float(rules.get("burstiness_high", 4))
    near_expiry_high = float(rules.get("near_expiry_ratio_high", 0.3))

    if minute_burst_ratio >= minute_burst_threshold and near_expiry_ratio >= near_expiry_high:
        return "成交爆发(临近到期)"
    if minute_burst_ratio >= minute_burst_threshold:
        return "成交爆发"
    if (
        interval_median <= interval_fast
        or trades_per_day >= trades_per_day_high
        or burstiness >= burstiness_high
    ):
        return "时效强"
    return "可复制"


def _build_notes(metrics: Dict[str, Optional[float]], rules: Dict[str, Any]) -> str:
    notes: List[str] = []
    tail_high_ratio = metrics.get("tail_high_ratio") or 0.0
    tail_low_ratio = metrics.get("tail_low_ratio") or 0.0
    mid_ratio = metrics.get("mid_ratio") or 0.0
    minute_burst_ratio = metrics.get("minute_burst_ratio") or 0.0
    interval_median = metrics.get("interval_median_minutes") or 0.0
    trades_per_day = metrics.get("trades_per_day") or 0.0
    burstiness = metrics.get("burstiness") or 0.0
    near_expiry_ratio = metrics.get("near_expiry_ratio") or 0.0
    recent_pnl_share = metrics.get("recent_pnl_share") or 0.0
    recent_surge_ratio = metrics.get("recent_surge_ratio") or 0.0

    tail_high_threshold = float(rules.get("tail_high_ratio_tail", 0.6))
    tail_low_threshold = float(rules.get("tail_low_ratio_longshot", 0.25))
    mid_threshold = float(rules.get("mid_ratio_balanced", 0.4))
    minute_burst_threshold = float(rules.get("minute_burst_ratio_high", 0.25))
    interval_fast = float(rules.get("interval_median_minutes_fast", 2))
    trades_per_day_high = float(rules.get("trades_per_day_high", 25))
    burstiness_high = float(rules.get("burstiness_high", 4))
    near_expiry_high = float(rules.get("near_expiry_ratio_high", 0.3))

    if tail_high_ratio >= tail_high_threshold:
        notes.append("尾单占比高")
    if tail_low_ratio >= tail_low_threshold:
        notes.append("长shot占比高")
    if mid_ratio < mid_threshold:
        notes.append("均衡占比偏低")
    if minute_burst_ratio >= minute_burst_threshold:
        notes.append("分钟爆发高")
        if near_expiry_ratio >= near_expiry_high:
            notes.append("可能到期成交集中")
    if interval_median <= interval_fast:
        notes.append("成交间隔偏快")
    if trades_per_day >= trades_per_day_high:
        notes.append("日均交易偏多")
    if burstiness >= burstiness_high:
        notes.append("日内爆发度高")
    if recent_pnl_share >= 0.55:
        notes.append("近月收益占比高(爆发型?)")
    if recent_surge_ratio >= 4.0:
        notes.append("近月收益远高于历史")

    return "；".join(notes[:3])


def main() -> None:
    args = _parse_args()
    base_dir = Path(__file__).resolve().parent
    config_path = (base_dir / args.config).resolve()
    config = _load_config(config_path)

    data_dir = (base_dir / config.get("data_dir", "data")).resolve()
    users_dir = (base_dir / config.get("users_dir", "data/users")).resolve()
    output_dir = (base_dir / config.get("output_dir", "data")).resolve()

    features_filename = config.get("features_filename", "users_features.csv")
    candidates_filename = config.get("candidates_filename", "candidates.csv")
    final_filename = config.get("final_filename", "final_candidates.csv")
    metadata_filename = config.get("metadata_filename", "screening_metadata.json")
    metadata_path = Path(metadata_filename)
    if not metadata_path.is_absolute():
        metadata_path = (base_dir / metadata_path).resolve()

    summary_map = _load_user_summary_map(data_dir / "users_summary.csv")

    features_rows: List[Dict[str, Any]] = []
    candidate_rows: List[Dict[str, Any]] = []

    for user_dir in sorted(users_dir.iterdir() if users_dir.exists() else []):
        if not user_dir.is_dir():
            continue
        user = user_dir.name
        closed_rows = _read_csv(user_dir / "closed_positions.csv")
        open_rows = _read_csv(user_dir / "positions.csv")
        trade_action_rows = _read_csv(user_dir / "trade_actions.csv")
        summary_row = None
        if (user_dir / "summary.csv").exists():
            summary_rows = _read_csv(user_dir / "summary.csv")
            if summary_rows:
                summary_row = summary_rows[0]
        elif user in summary_map:
            summary_row = summary_map[user]

        metrics = _build_features(
            user,
            closed_rows,
            open_rows,
            summary_row,
            trade_action_rows,
            config,
        )
        row: Dict[str, Any] = {"user": user}
        row.update(metrics)
        base_copy_score = _compute_copy_score(row, config)
        row["base_copy_score"] = base_copy_score

        stability = _compute_stability_score(row, config)
        row.update(stability)

        stability_weight = float(config.get("stability_weight", 0.55))
        if stability_weight < 0:
            stability_weight = 0.0
        if stability_weight > 1:
            stability_weight = 1.0

        final_score = (1.0 - stability_weight) * base_copy_score + stability_weight * row.get(
            "stability_score", 0.0
        )
        row["copy_score"] = final_score

        passed, failures, warnings = _apply_filters(row, config)
        row["passed_filter"] = passed
        row["filter_failures"] = ";".join(failures)
        row["filter_warnings"] = ";".join(warnings)

        features_rows.append(row)
        if passed:
            candidate_rows.append(row)

    features_rows = sorted(
        features_rows, key=lambda row: row.get("copy_score", 0), reverse=True
    )
    candidate_rows = sorted(
        candidate_rows, key=lambda row: row.get("copy_score", 0), reverse=True
    )

    _write_csv(output_dir / features_filename, features_rows)
    _write_csv(output_dir / candidates_filename, candidate_rows)

    final_rows: List[Dict[str, Any]] = []
    label_rules = config.get("label_rules", {})
    price_rules = label_rules.get("price_style", {})
    copy_rules = label_rules.get("copy_style", {})
    final_output = config.get("final_output", {})
    final_columns = final_output.get("columns")
    final_rename = final_output.get("rename", {})

    for row in candidate_rows:
        enriched = dict(row)
        enriched["price_style"] = _build_price_style(enriched, price_rules)
        enriched["copy_style"] = _build_copy_style(enriched, copy_rules)
        enriched["notes"] = _build_notes(enriched, {**price_rules, **copy_rules})
        enriched["profile_url"] = f"https://polymarket.com/profile/{str(row.get('user', '')).lower()}"

        if final_columns:
            filtered = {col: enriched.get(col) for col in final_columns}
        else:
            filtered = enriched

        renamed = {final_rename.get(key, key): value for key, value in filtered.items()}
        final_rows.append(renamed)

    sort_by = final_output.get("sort_by")
    descending = bool(final_output.get("descending", True))
    if sort_by:
        sort_key = final_rename.get(sort_by, sort_by)
        final_rows = sorted(
            final_rows,
            key=lambda row: row.get(sort_key, 0) or 0,
            reverse=descending,
        )

    _write_csv(output_dir / final_filename, final_rows)

    metadata = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "config": config,
        "features_file": str((output_dir / features_filename).resolve()),
        "candidates_file": str((output_dir / candidates_filename).resolve()),
        "final_file": str((output_dir / final_filename).resolve()),
        "users_count": len(features_rows),
        "candidates_count": len(candidate_rows),
    }
    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    print(
        f"[INFO] 完成筛选：全量={len(features_rows)}，候选={len(candidate_rows)}，"
        f"输出目录={output_dir}"
    )


if __name__ == "__main__":
    main()
