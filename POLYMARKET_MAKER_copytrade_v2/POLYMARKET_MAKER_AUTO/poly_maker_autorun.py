"""
poly_maker_autorun
-------------------

基础骨架：配置加载、主循环、命令/交互入口。
当前版本通过 copytrade 产出的 token 文件驱动话题调度。
"""
from __future__ import annotations

import argparse
import copy
import json
import math
import os
import random
import queue
import select
import signal
import subprocess
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

# =====================
# 配置与常量
# =====================
PROJECT_ROOT = Path(__file__).resolve().parent
MAKER_ROOT = PROJECT_ROOT / "POLYMARKET_MAKER"
if str(MAKER_ROOT) not in sys.path:
    sys.path.insert(0, str(MAKER_ROOT))

DEFAULT_GLOBAL_CONFIG = {
    "copytrade_poll_sec": 30.0,
    "command_poll_sec": 5.0,
    "max_concurrent_tasks": 10,
    "log_dir": str(PROJECT_ROOT / "logs" / "autorun"),
    "data_dir": str(PROJECT_ROOT / "data"),
    "handled_topics_path": str(PROJECT_ROOT / "data" / "handled_topics.json"),
    "copytrade_tokens_path": str(
        PROJECT_ROOT.parent / "copytrade" / "tokens_from_copytrade.json"
    ),
    "copytrade_sell_signals_path": str(
        PROJECT_ROOT.parent / "copytrade" / "copytrade_sell_signals.json"
    ),
    "process_start_retries": 1,
    "process_retry_delay_sec": 2.0,
    "process_graceful_timeout_sec": 5.0,
    "process_stagger_max_sec": 3.0,
    "topic_start_cooldown_sec": 5.0,
    "log_excerpt_interval_sec": 15.0,
    "runtime_status_path": str(PROJECT_ROOT / "data" / "autorun_status.json"),
    "ws_debug_raw": False,
}
ORDER_SIZE_DECIMALS = 4  # Polymarket 下单数量精度（按买单精度取整）


# ========== 错误日志记录函数 ==========
def _log_error(error_type: str, error_data: Dict[str, Any]) -> None:
    """
    记录错误到独立的错误日志文件。

    :param error_type: 错误类型标识（如 WS_AGGREGATOR_ERROR, TASK_START_ERROR 等）
    :param error_data: 错误相关数据（字典格式）
    """
    try:
        # 确定错误日志文件路径
        log_dir = PROJECT_ROOT / "logs" / "autorun"
        log_dir.mkdir(parents=True, exist_ok=True)
        error_log_path = log_dir / "error_log.txt"

        # 构建日志条目
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_entry = {
            "timestamp": timestamp,
            "error_type": error_type,
            "data": error_data
        }

        # 追加写入日志文件
        with open(error_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    except Exception as e:
        # 错误日志记录失败时，仅打印到控制台，不中断程序
        print(f"[ERROR_LOG] 写入错误日志失败: {e}")


def _topic_id_from_entry(entry: Any) -> str:
    """从 copytrade token 条目中提取 token_id。"""

    if isinstance(entry, str):
        return entry.strip()
    if isinstance(entry, dict):
        token_id = entry.get("token_id") or entry.get("tokenId")
        if isinstance(token_id, str) and token_id.strip():
            return token_id.strip()
        return ""
    return str(entry).strip()


def _safe_topic_filename(topic_id: str) -> str:
    return topic_id.replace("/", "_").replace("\\", "_")


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, str):
            raw = value.replace(",", "").strip()
            if not raw:
                return None
            return float(raw)
    except Exception:
        return None
    return None


def _env_flag(name: str) -> bool:
    raw = os.getenv(name, "").strip().lower()
    return raw in {"1", "true", "yes", "on", "y", "debug"}


def _ceil_to_precision(value: float, decimals: int) -> float:
    factor = 10 ** decimals
    return math.ceil(value * factor - 1e-12) / factor


def _scale_order_size_by_volume(
    base_size: float,
    total_volume: float,
    *,
    base_volume: Optional[float] = None,
    growth_factor: float = 0.5,
    decimals: int = ORDER_SIZE_DECIMALS,
) -> float:
    """根据市场成交量对基础下单份数进行递增（边际递减）。"""

    if base_size <= 0 or total_volume <= 0:
        return base_size

    effective_base_volume = _coerce_float(base_volume) or total_volume
    if effective_base_volume <= 0:
        return base_size

    effective_growth = max(growth_factor, 0.0)
    vol_ratio = max(total_volume / effective_base_volume, 1.0)
    # 使用对数增长控制放大：
    #   - base_volume 附近仅有轻微提升；
    #   - 成交量每提升 10 倍仅线性增加 growth_factor，边际效用递减。
    weight = 1.0 + effective_growth * math.log10(vol_ratio)
    weighted_size = base_size * weight
    return _ceil_to_precision(weighted_size, decimals)


def _load_json_file(path: Path) -> Dict[str, Any]:
    """读取 JSON 配置，不存在则返回空 dict。"""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as exc:  # pragma: no cover - 粗略校验
            raise RuntimeError(f"无法解析 JSON 配置: {path}: {exc}") from exc


def _dump_json_file(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def read_handled_topics(path: Path) -> set[str]:
    """读取历史已处理话题集合，空文件或字段缺失则返回空集合。"""

    data = _load_json_file(path)
    topics = data.get("topics") or data.get("handled_topics")
    if topics is None:
        return set()
    if not isinstance(topics, list):  # pragma: no cover - 容错
        print(f"[WARN] handled_topics 文件格式异常，已忽略: {path}")
        return set()
    return {str(t) for t in topics}


def write_handled_topics(path: Path, topics: set[str]) -> None:
    """写入最新的已处理话题集合。"""

    payload = {
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total": len(topics),
        "topics": sorted(topics),
    }
    _dump_json_file(path, payload)


def compute_new_topics(latest: List[Any], handled: set[str]) -> List[str]:
    """从最新筛选结果中筛出尚未处理的话题列表。"""

    result: List[str] = []
    for entry in latest:
        topic_id = _topic_id_from_entry(entry)
        if topic_id and topic_id not in handled:
            result.append(topic_id)
    return result


@dataclass
class GlobalConfig:
    copytrade_poll_sec: float = DEFAULT_GLOBAL_CONFIG["copytrade_poll_sec"]
    command_poll_sec: float = DEFAULT_GLOBAL_CONFIG["command_poll_sec"]
    max_concurrent_tasks: int = DEFAULT_GLOBAL_CONFIG["max_concurrent_tasks"]
    log_dir: Path = field(default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["log_dir"]))
    data_dir: Path = field(default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["data_dir"]))
    handled_topics_path: Path = field(
        default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["handled_topics_path"])
    )
    copytrade_tokens_path: Path = field(
        default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["copytrade_tokens_path"])
    )
    copytrade_sell_signals_path: Path = field(
        default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["copytrade_sell_signals_path"])
    )
    process_start_retries: int = DEFAULT_GLOBAL_CONFIG["process_start_retries"]
    process_retry_delay_sec: float = DEFAULT_GLOBAL_CONFIG["process_retry_delay_sec"]
    process_graceful_timeout_sec: float = DEFAULT_GLOBAL_CONFIG[
        "process_graceful_timeout_sec"
    ]
    process_stagger_max_sec: float = DEFAULT_GLOBAL_CONFIG["process_stagger_max_sec"]
    topic_start_cooldown_sec: float = DEFAULT_GLOBAL_CONFIG["topic_start_cooldown_sec"]
    log_excerpt_interval_sec: float = DEFAULT_GLOBAL_CONFIG["log_excerpt_interval_sec"]
    runtime_status_path: Path = field(
        default_factory=lambda: Path(DEFAULT_GLOBAL_CONFIG["runtime_status_path"])
    )
    ws_debug_raw: bool = bool(DEFAULT_GLOBAL_CONFIG["ws_debug_raw"])

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GlobalConfig":
        data = data or {}
        scheduler = data.get("scheduler") or {}
        paths = data.get("paths") or {}
        debug = data.get("debug") or {}
        flat_overrides = {k: v for k, v in data.items() if k not in {"scheduler", "paths"}}
        merged = {**DEFAULT_GLOBAL_CONFIG, **flat_overrides}

        log_dir = Path(
            paths.get("log_directory")
            or merged.get("log_dir", DEFAULT_GLOBAL_CONFIG["log_dir"])
        )
        data_dir = Path(
            paths.get("data_directory")
            or merged.get("data_dir", DEFAULT_GLOBAL_CONFIG["data_dir"])
        )

        handled_topics_path = Path(
            merged.get("handled_topics_path")
            or paths.get("handled_topics_file")
            or data_dir / "handled_topics.json"
        )
        copytrade_tokens_path = Path(
            merged.get("copytrade_tokens_path")
            or paths.get("copytrade_tokens_file")
            or PROJECT_ROOT.parent / "copytrade" / "tokens_from_copytrade.json"
        )
        copytrade_sell_signals_path = Path(
            merged.get("copytrade_sell_signals_path")
            or paths.get("copytrade_sell_signals_file")
            or PROJECT_ROOT.parent / "copytrade" / "copytrade_sell_signals.json"
        )
        runtime_status_path = Path(
            merged.get("runtime_status_path")
            or paths.get("run_state_file")
            or data_dir / "autorun_status.json"
        )

        return cls(
            copytrade_poll_sec=float(
                scheduler.get("copytrade_poll_seconds")
                or merged.get(
                    "copytrade_poll_sec", DEFAULT_GLOBAL_CONFIG["copytrade_poll_sec"]
                )
            ),
            command_poll_sec=float(
                scheduler.get("command_poll_seconds")
                or scheduler.get("poll_interval_seconds")
                or merged.get("command_poll_sec", DEFAULT_GLOBAL_CONFIG["command_poll_sec"])
            ),
            max_concurrent_tasks=int(
                scheduler.get(
                    "max_concurrent_tasks", DEFAULT_GLOBAL_CONFIG["max_concurrent_tasks"]
                )
            ),
            log_dir=log_dir,
            data_dir=data_dir,
            handled_topics_path=handled_topics_path,
            copytrade_tokens_path=copytrade_tokens_path,
            copytrade_sell_signals_path=copytrade_sell_signals_path,
            process_start_retries=int(
                merged.get("process_start_retries", cls.process_start_retries)
            ),
            process_retry_delay_sec=float(
                merged.get("process_retry_delay_sec", cls.process_retry_delay_sec)
            ),
            process_graceful_timeout_sec=float(
                merged.get(
                    "process_graceful_timeout_sec", cls.process_graceful_timeout_sec
                )
            ),
            process_stagger_max_sec=float(
                merged.get("process_stagger_max_sec", cls.process_stagger_max_sec)
            ),
            topic_start_cooldown_sec=float(
                merged.get("topic_start_cooldown_sec", cls.topic_start_cooldown_sec)
            ),
            log_excerpt_interval_sec=float(
                merged.get("log_excerpt_interval_sec", cls.log_excerpt_interval_sec)
            ),
            runtime_status_path=runtime_status_path,
            ws_debug_raw=bool(
                debug.get("ws_debug_raw")
                or debug.get("ws_raw")
                or merged.get("ws_debug_raw", cls.ws_debug_raw)
            ),
        )

    def ensure_dirs(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class TopicTask:
    topic_id: str
    status: str = "pending"
    start_time: float = field(default_factory=time.time)
    last_heartbeat: Optional[float] = None
    notes: List[str] = field(default_factory=list)
    process: Optional[subprocess.Popen] = None
    log_path: Optional[Path] = None
    config_path: Optional[Path] = None
    log_excerpt: str = ""
    restart_attempts: int = 0
    no_restart: bool = False
    end_reason: Optional[str] = None
    last_log_excerpt_ts: float = 0.0

    def heartbeat(self, message: str) -> None:
        self.last_heartbeat = time.time()
        self.notes.append(message)

    def is_running(self) -> bool:
        return bool(self.process) and (self.process.poll() is None)


class AutoRunManager:
    def __init__(
        self,
        global_config: GlobalConfig,
        strategy_defaults: Dict[str, Any],
        run_params_template: Dict[str, Any],
    ):
        self.config = global_config
        self.strategy_defaults = strategy_defaults
        self.run_params_template = run_params_template or {}
        self.stop_event = threading.Event()
        self.command_queue: "queue.Queue[str]" = queue.Queue()
        self.tasks: Dict[str, TopicTask] = {}
        self.latest_topics: List[Dict[str, Any]] = []
        self.topic_details: Dict[str, Dict[str, Any]] = {}
        self.handled_topics: set[str] = set()
        self.pending_topics: List[str] = []
        self.pending_exit_topics: List[str] = []
        self._next_topics_refresh: float = 0.0
        self._next_status_dump: float = 0.0
        self._next_topic_start_at: float = 0.0
        self.status_path = self.config.runtime_status_path
        self._ws_cache_path = self.config.data_dir / "ws_cache.json"
        self._ws_cache_lock = threading.Lock()
        self._ws_cache: Dict[str, Dict[str, Any]] = {}
        self._ws_cache_dirty = False
        self._ws_cache_last_flush = 0.0
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_thread_stop: Optional[threading.Event] = None
        self._ws_token_ids: List[str] = []
        self._ws_aggregator_thread: Optional[threading.Thread] = None
        self._ws_debug_raw = _env_flag("POLY_WS_DEBUG_RAW") or self.config.ws_debug_raw

    # ========== 核心循环 ==========
    def run_loop(self) -> None:
        self.config.ensure_dirs()
        self._load_handled_topics()
        self._restore_runtime_status()
        print(f"[INIT] autorun start | copytrade_poll={self.config.copytrade_poll_sec}s")
        self._start_ws_aggregator()
        try:
            while not self.stop_event.is_set():
                try:
                    now = time.time()
                    self._process_commands()
                    self._poll_tasks()
                    self._schedule_pending_exit_cleanup()
                    self._schedule_pending_topics()
                    self._purge_inactive_tasks()
                    if now >= self._next_topics_refresh:
                        self._refresh_topics()
                        self._next_topics_refresh = now + self.config.copytrade_poll_sec
                    if now >= self._next_status_dump:
                        self._print_status()
                        self._dump_runtime_status()
                        self._next_status_dump = now + max(
                            5.0, self.config.command_poll_sec
                        )
                    time.sleep(self.config.command_poll_sec)
                except Exception as exc:  # pragma: no cover - 防御性保护
                    print(f"[ERROR] 主循环异常已捕获，将继续运行: {exc}")
                    traceback.print_exc()
                    _log_error("MAIN_LOOP_ERROR", {
                        "message": "主循环异常",
                        "error": str(exc),
                        "traceback": traceback.format_exc()
                    })
                    time.sleep(max(1.0, self.config.command_poll_sec))
        finally:
            self._stop_ws_aggregator()
            self._cleanup_all_tasks()
            self._dump_runtime_status()
            print("[DONE] autorun stopped")

    def _start_ws_aggregator(self) -> None:
        if self._ws_aggregator_thread and self._ws_aggregator_thread.is_alive():
            return
        self._ws_aggregator_thread = threading.Thread(
            target=self._ws_aggregator_loop,
            daemon=True,
        )
        self._ws_aggregator_thread.start()

    def _stop_ws_aggregator(self) -> None:
        self._stop_ws_subscription()
        if self._ws_aggregator_thread and self._ws_aggregator_thread.is_alive():
            self._ws_aggregator_thread.join(timeout=3)

    def _desired_ws_token_ids(self) -> List[str]:
        """获取需要订阅的token列表（包括运行中的和待启动的）"""
        token_ids = []

        # 1. 运行中的任务
        for topic_id, task in self.tasks.items():
            if task.is_running():
                token_ids.append(topic_id)

        # 2. 待启动的pending tokens（提前订阅，避免启动后等待）
        for topic_id in self.pending_topics:
            if topic_id not in token_ids:
                token_ids.append(topic_id)

        return sorted({tid for tid in token_ids if tid})

    def _ws_aggregator_loop(self) -> None:
        last_health_check = 0.0
        last_event_count = 0  # 跟踪上次检查时的事件数

        while not self.stop_event.is_set():
            desired = self._desired_ws_token_ids()
            if desired != self._ws_token_ids:
                self._restart_ws_subscription(desired)
            self._flush_ws_cache_if_needed()

            # 定期健康检查（每10秒，加快故障检测和恢复）
            now = time.time()
            if now - last_health_check >= 10.0:
                current_count = getattr(self, '_ws_event_count', 0)

                # 检查数据流是否停滞
                if current_count == last_event_count and self._ws_token_ids:
                    print(f"[WARN] WS 聚合器10秒内未收到任何新事件（订阅了 {len(self._ws_token_ids)} 个token）")
                elif current_count > last_event_count:
                    # 数据流正常，每小时打印一次统计（避免刷屏）
                    if not hasattr(self, '_last_flow_log'):
                        self._last_flow_log = 0.0
                    if now - self._last_flow_log >= 3600.0:
                        print(f"[WS][FLOW] 数据流正常，10秒内收到 {current_count - last_event_count} 个事件")
                        self._last_flow_log = now

                last_event_count = current_count
                self._health_check()
                last_health_check = now

            time.sleep(0.1)  # 从1.0秒改为0.1秒，提高缓存写入频率

    def _restart_ws_subscription(self, token_ids: List[str]) -> None:
        old_ids = set(self._ws_token_ids)
        new_ids = set(token_ids)
        added = new_ids - old_ids
        removed = old_ids - new_ids

        if added:
            print(f"[WS][AGGREGATOR] ✓ 新增订阅 {len(added)} 个token:")
            for tid in list(added)[:5]:
                print(f"    {tid[:8]}...{tid[-8:]}")
        if removed:
            print(f"[WS][AGGREGATOR] ✗ 移除订阅 {len(removed)} 个token:")
            for tid in list(removed)[:5]:
                print(f"    {tid[:8]}...{tid[-8:]}")

        # 调试：打印完整订阅列表（启动时）
        if not hasattr(self, '_subscription_list_printed'):
            self._subscription_list_printed = True
            print(f"[WS][AGGREGATOR][DEBUG] 完整订阅列表 ({len(token_ids)} 个):")
            for idx, tid in enumerate(token_ids, 1):
                print(f"    [{idx}] {tid[:8]}...{tid[-8:]}")
            print()

        self._stop_ws_subscription()
        self._ws_token_ids = token_ids
        if not token_ids:
            print("[WS][AGGREGATOR] 无token需要订阅，WS连接已停止")
            return
        self._start_ws_subscription(token_ids)

    def _start_ws_subscription(self, token_ids: List[str]) -> None:
        # 验证 WS 模块导入
        try:
            from Volatility_arbitrage_main_ws import ws_watch_by_ids
        except Exception as exc:
            print(f"[ERROR] 无法导入 WS 模块: {exc}")
            print("[ERROR] WS 聚合器启动失败，子进程将使用独立 WS 连接")
            _log_error("WS_AGGREGATOR_IMPORT_ERROR", {
                "message": "无法导入 WS 模块",
                "error": str(exc),
                "tokens_count": len(token_ids)
            })
            # 不抛出异常，让系统继续运行（子进程会fallback到独立WS）
            return

        # 验证 websocket-client 依赖
        try:
            import websocket
        except ImportError:
            print("[ERROR] 缺少依赖 websocket-client")
            print("[ERROR] 请运行: pip install websocket-client")
            print("[ERROR] WS 聚合器启动失败，子进程将使用独立 WS 连接")
            _log_error("WS_AGGREGATOR_DEPENDENCY_ERROR", {
                "message": "缺少依赖 websocket-client",
                "tokens_count": len(token_ids)
            })
            return

        stop_event = threading.Event()
        self._ws_thread_stop = stop_event
        self._ws_thread = threading.Thread(
            target=ws_watch_by_ids,
            kwargs={
                "asset_ids": token_ids,
                "label": "autorun-aggregator",
                "on_event": self._on_ws_event,
                "verbose": self._ws_debug_raw,
                "stop_event": stop_event,
            },
            daemon=True,
        )
        self._ws_thread.start()
        print(f"[WS][AGGREGATOR] 聚合订阅启动，tokens={len(token_ids)}")
        print(f"[WS][AGGREGATOR] 缓存文件: {self._ws_cache_path}")

        # 验证线程是否成功启动
        time.sleep(2)
        if not self._ws_thread.is_alive():
            print("[WS][AGGREGATOR] ✗ WS线程启动后立即退出")
            print("[WS][AGGREGATOR] 子进程将使用独立 WS 连接")
            _log_error("WS_AGGREGATOR_THREAD_ERROR", {
                "message": "WS线程启动后立即退出",
                "tokens_count": len(token_ids),
                "token_ids": token_ids[:5]  # 只记录前5个
            })
            self._ws_thread = None
            self._ws_thread_stop = None
        else:
            print(f"[WS][AGGREGATOR] ✓ WS线程运行正常")

    def _stop_ws_subscription(self) -> None:
        if self._ws_thread_stop is not None:
            self._ws_thread_stop.set()
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3)
        self._ws_thread = None
        self._ws_thread_stop = None

    def _update_token_timestamp_from_trade(self, ev: Dict[str, Any]) -> None:
        """
        处理 last_trade_price 事件，仅更新时间戳以避免假僵尸token。
        这类事件表明市场有交易活动，即使价格未显著变化。
        """
        # 获取asset_id（可能在不同字段）
        asset_id = ev.get("asset_id") or ev.get("token_id") or ev.get("tokenId")
        if not asset_id:
            return

        token_id = str(asset_id)

        # 仅更新缓存中的时间戳，保留其他字段
        with self._ws_cache_lock:
            if token_id in self._ws_cache:
                # token已存在，仅更新时间戳
                self._ws_cache[token_id]["updated_at"] = time.time()
                self._ws_cache_dirty = True

                # 可选：更新last_trade_price字段
                trade_price = _coerce_float(ev.get("price") or ev.get("last_trade_price"))
                if trade_price is not None and trade_price > 0:
                    self._ws_cache[token_id]["price"] = trade_price

    def _on_ws_event(self, ev: Dict[str, Any]) -> None:
        # 统计事件接收和过滤情况
        if not hasattr(self, '_ws_event_count'):
            self._ws_event_count = 0
            self._ws_filtered_count = 0
            self._ws_filtered_types: Dict[str, int] = {}
            self._ws_last_stats_log = 0.0

        self._ws_event_count += 1

        if self._ws_debug_raw:
            try:
                print(f"[WS][RAW] {json.dumps(ev, ensure_ascii=False)}")
            except Exception:
                print(f"[WS][RAW] {ev}")

        if not isinstance(ev, dict):
            self._ws_filtered_count += 1
            return

        event_type = ev.get("event_type")

        # 处理 price_change 事件（完整更新）
        if event_type == "price_change":
            pcs = ev.get("price_changes", [])
        elif "price_changes" in ev:
            pcs = ev.get("price_changes", [])
        # ✅ 新增：处理 book 和 tick 事件（订单簿更新）
        elif event_type in ("book", "tick"):
            # 尝试从事件中提取价格信息并转换为 price_changes 格式
            asset_id = ev.get("asset_id") or ev.get("token_id")
            if asset_id:
                bid = _coerce_float(ev.get("best_bid") or ev.get("bid"))
                ask = _coerce_float(ev.get("best_ask") or ev.get("ask"))

                # 如果有有效的bid/ask，就构造price_change格式
                if bid or ask:
                    # ✅ 使用mid=(bid+ask)/2作为最可靠的当前价格
                    mid = (bid + ask) / 2.0 if bid and ask else (bid or ask)
                    # ✅ 只使用last_trade_price，不使用price字段（含义不明确）
                    last = _coerce_float(ev.get("last_trade_price"))
                    pcs = [{
                        "asset_id": asset_id,
                        "best_bid": bid,
                        "best_ask": ask,
                        "last_trade_price": last or mid
                    }]
                    # ✅ 调试日志：确认book/tick事件被处理
                    if not hasattr(self, '_book_tick_log_count'):
                        self._book_tick_log_count = 0
                        self._book_tick_last_log = 0.0
                    self._book_tick_log_count += 1
                    now = time.time()
                    if now - self._book_tick_last_log >= 60:
                        print(f"[WS][AGGREGATOR] ✅ 处理book/tick事件: {self._book_tick_log_count} 次/分钟")
                        self._book_tick_last_log = now
                        self._book_tick_log_count = 0
                else:
                    pcs = []
            else:
                pcs = []
        # 处理 last_trade_price 事件（仅更新时间戳，避免假僵尸）
        elif event_type == "last_trade_price":
            self._update_token_timestamp_from_trade(ev)
            return
        else:
            # 记录被过滤的事件类型
            self._ws_filtered_count += 1
            evt_type = event_type or "unknown"
            self._ws_filtered_types[evt_type] = self._ws_filtered_types.get(evt_type, 0) + 1

            # 每60秒打印一次统计
            now = time.time()
            if now - self._ws_last_stats_log >= 60.0:
                print(
                    f"[WS][STATS] 总事件: {self._ws_event_count}, "
                    f"已处理: {self._ws_event_count - self._ws_filtered_count}, "
                    f"已过滤: {self._ws_filtered_count}"
                )
                if self._ws_filtered_types:
                    print(f"[WS][STATS] 过滤事件类型: {self._ws_filtered_types}")
                self._ws_last_stats_log = now
            return
        ts = ev.get("timestamp") or ev.get("ts") or ev.get("time")
        # 确保时间戳总是有效的
        if ts is None:
            ts = time.time()

        status_keys = (
            "status",
            "market_status",
            "marketStatus",
            "is_closed",
            "market_closed",
            "closed",
            "isMarketClosed",
        )
        event_status = {k: ev.get(k) for k in status_keys if k in ev}
        for pc in pcs:
            token_id = str(pc.get("asset_id") or "")
            if not token_id:
                continue

            # ✅ P0修复：只缓存订阅列表中的token
            # Polymarket的market订阅会返回整个市场（YES+NO），需要过滤
            if token_id not in self._ws_token_ids:
                # 静默跳过未订阅的token（可能是配对token）
                if not hasattr(self, '_ws_unsubscribed_tokens'):
                    self._ws_unsubscribed_tokens = set()
                    self._ws_unsubscribed_log_ts = 0.0
                    self._ws_filter_detail_logged = False

                # 只在第一次遇到时记录
                if token_id not in self._ws_unsubscribed_tokens:
                    self._ws_unsubscribed_tokens.add(token_id)

                    # 第一次过滤时，打印详细信息（调试用）
                    if not self._ws_filter_detail_logged:
                        print(f"[WS][FILTER][DEBUG] 发现未订阅的token: {token_id[:8]}...{token_id[-8:]}")
                        print(f"[WS][FILTER][DEBUG] 当前订阅列表 ({len(self._ws_token_ids)} 个):")
                        for sub_tid in list(self._ws_token_ids)[:5]:
                            print(f"    {sub_tid[:8]}...{sub_tid[-8:]}")
                        if len(self._ws_token_ids) > 5:
                            print(f"    ... (还有 {len(self._ws_token_ids) - 5} 个)")
                        self._ws_filter_detail_logged = True

                    # 每5分钟打印一次汇总（避免刷屏）
                    now = time.time()
                    if now - self._ws_unsubscribed_log_ts >= 300:
                        print(f"[WS][FILTER] 过滤未订阅的token（可能是配对token）: {len(self._ws_unsubscribed_tokens)} 个")
                        if len(self._ws_unsubscribed_tokens) <= 5:
                            for utid in self._ws_unsubscribed_tokens:
                                print(f"  - {utid[:8]}...{utid[-8:]}")
                        self._ws_unsubscribed_log_ts = now
                continue

            bid = _coerce_float(pc.get("best_bid")) or 0.0
            ask = _coerce_float(pc.get("best_ask")) or 0.0

            # ✅ 计算mid价格（最可靠的当前市场价格）
            mid = (bid + ask) / 2.0 if bid and ask else (bid or ask or 0.0)

            # ✅ 只使用last_trade_price作为真实成交价，不使用price字段（price含义不明确）
            # price字段可能是历史价格或订单簿深度价格，不适合用作当前价格
            last = _coerce_float(pc.get("last_trade_price"))

            # 如果没有last_trade_price，直接使用mid
            if last is None or last == 0.0:
                last = mid

            # 获取当前序列号并递增（用于去重）
            with self._ws_cache_lock:
                old_data = self._ws_cache.get(token_id, {})
                seq = old_data.get("seq", 0) + 1

            payload = {
                "price": last,
                "best_bid": bid,
                "best_ask": ask,
                "ts": ts,
                "updated_at": time.time(),
                "event_type": ev.get("event_type"),
                "seq": seq,  # 单调递增的序列号
            }
            for key in status_keys:
                val = pc.get(key)
                if val is None:
                    val = event_status.get(key)
                if val is not None:
                    payload[key] = val
            with self._ws_cache_lock:
                self._ws_cache[token_id] = payload
                self._ws_cache_dirty = True

                # 定期打印缓存更新统计（每分钟）
                if not hasattr(self, '_cache_update_log_ts'):
                    self._cache_update_log_ts = 0
                    self._cache_update_count = 0
                self._cache_update_count += 1
                now = time.time()
                if now - self._cache_update_log_ts >= 60:
                    print(f"[WS][AGGREGATOR] 缓存更新统计: {self._cache_update_count} 次/分钟, "
                          f"tokens={len(self._ws_cache)}, last_seq={seq}")
                    self._cache_update_log_ts = now
                    self._cache_update_count = 0

    def _flush_ws_cache_if_needed(self) -> None:
        now = time.time()
        # ✅ 从1.0秒改为0.1秒：提高缓存写入频率，让子进程能更快看到seq更新
        if not self._ws_cache_dirty and now - self._ws_cache_last_flush < 0.1:
            return
        with self._ws_cache_lock:
            if not self._ws_cache_dirty and now - self._ws_cache_last_flush < 0.1:
                return
            # ✅ 使用深拷贝避免多线程并发修改导致 "dictionary changed size during iteration" 错误
            data = {
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "tokens": copy.deepcopy(self._ws_cache),
            }
            self._ws_cache_dirty = False
            self._ws_cache_last_flush = now
        try:
            self._ws_cache_path.parent.mkdir(parents=True, exist_ok=True)

            # 使用原子写入：先写临时文件，再重命名
            tmp_path = self._ws_cache_path.with_suffix('.tmp')
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            # 原子操作：重命名（在 Unix 系统上是原子的）
            tmp_path.replace(self._ws_cache_path)

        except OSError as exc:
            print(f"[ERROR] 写入 WS 聚合缓存失败: {exc}")
            _log_error("WS_CACHE_WRITE_ERROR", {
                "message": "写入 WS 聚合缓存失败",
                "error": str(exc),
                "cache_path": str(self._ws_cache_path),
                "tokens_count": len(self._ws_cache)
            })
            # 清理临时文件
            try:
                tmp_path = self._ws_cache_path.with_suffix('.tmp')
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    def _health_check(self) -> None:
        """
        WS 聚合器健康检查（增强版）
        - 分级阈值：正常(10min) / 警告(30min) / 清理(60min)
        - 市场状态检测：自动清理已关闭市场
        - 日志优化：减少刷屏频率
        """
        # 周期性打印缓存状态（每5分钟）
        if not hasattr(self, '_last_cache_status_log'):
            self._last_cache_status_log = 0.0

        now = time.time()
        if now - self._last_cache_status_log >= 300:  # 5分钟
            with self._ws_cache_lock:
                if self._ws_cache:
                    print(f"\n[WS][CACHE_STATUS] 缓存中的token状态 ({len(self._ws_cache)} 个):")
                    for idx, (tid, data) in enumerate(list(self._ws_cache.items())[:10], 1):
                        seq = data.get("seq", 0)
                        bid = data.get("best_bid", 0)
                        ask = data.get("best_ask", 0)
                        updated = data.get("updated_at", 0)
                        age = now - updated if updated > 0 else 0
                        tid_short = f"{tid[:8]}...{tid[-8:]}"
                        print(f"  [{idx}] {tid_short}: seq={seq}, bid={bid}, ask={ask}, age={age:.0f}s")
                    if len(self._ws_cache) > 10:
                        print(f"  ... (还有 {len(self._ws_cache) - 10} 个token)")
                    print()
            self._last_cache_status_log = now
        # 检查 WS 线程是否运行
        if self._ws_token_ids and (not self._ws_thread or not self._ws_thread.is_alive()):
            print("[WARN] WS 聚合器线程已停止，尝试重启...")
            self._restart_ws_subscription(self._ws_token_ids)

        # 检查缓存数据
        with self._ws_cache_lock:
            token_count = len(self._ws_cache)
            subscribed_count = len(self._ws_token_ids)

            if subscribed_count > 0 and token_count == 0:
                print(f"[WARN] WS 聚合器订阅了 {subscribed_count} 个token，但缓存为空")
                print("[WARN] 可能尚未接收到数据，或连接异常")

            # 分级阈值检查（秒）
            THRESHOLD_WARNING = 1800  # 30分钟 - 警告
            THRESHOLD_CLEANUP = 3600  # 60分钟 - 清理

            now = time.time()
            warning_tokens = []  # 30分钟未更新
            cleanup_tokens = []  # 60分钟未更新或市场已关闭
            closed_market_tokens = []  # 市场已关闭

            for token_id, data in list(self._ws_cache.items()):
                updated_at = data.get("updated_at", 0)
                age = now - updated_at

                # 检查市场状态
                is_closed = (
                    data.get("market_closed")
                    or data.get("is_closed")
                    or data.get("closed")
                )

                if is_closed:
                    closed_market_tokens.append((token_id, age))
                    cleanup_tokens.append((token_id, age, "市场已关闭"))
                elif age > THRESHOLD_CLEANUP:
                    cleanup_tokens.append((token_id, age, f"{age/60:.0f}分钟无更新"))
                elif age > THRESHOLD_WARNING:
                    warning_tokens.append((token_id, age))

            # 清理过期/关闭的token
            if cleanup_tokens:
                for token_id, age, reason in cleanup_tokens:
                    del self._ws_cache[token_id]
                    # 记录到日志
                    _log_error("TOKEN_CLEANUP", {
                        "token_id": token_id,
                        "age_seconds": age,
                        "reason": reason,
                        "message": "从缓存中清理token"
                    })
                print(f"[CLEANUP] 清理 {len(cleanup_tokens)} 个过期/关闭的token:")
                for token_id, age, reason in cleanup_tokens[:3]:
                    print(f"  - {token_id[:20]}...: {reason}")
                self._ws_cache_dirty = True

            # 警告日志（降低频率：每60秒才打印一次）
            if not hasattr(self, '_last_warning_log'):
                self._last_warning_log = 0

            if warning_tokens and (now - self._last_warning_log >= 60):
                print(f"[HEALTH] {len(warning_tokens)} 个token数据超过30分钟未更新（将在60分钟后清理）")
                # 只显示前2个
                for token_id, age in warning_tokens[:2]:
                    print(f"  - {token_id[:20]}...: {age/60:.0f}分钟前")
                self._last_warning_log = now

        # 检查文件状态
        if self._ws_cache_path.exists():
            try:
                stat = self._ws_cache_path.stat()
                age = time.time() - stat.st_mtime
                if age > 120:  # 2分钟没更新
                    # 降低日志频率
                    if not hasattr(self, '_last_file_warn'):
                        self._last_file_warn = 0
                    if time.time() - self._last_file_warn >= 60:
                        print(f"[WARN] ws_cache.json 文件过期，最后修改: {age:.0f}秒前")
                        self._last_file_warn = time.time()
            except OSError:
                pass
        else:
            if self._ws_token_ids:
                print(f"[WARN] ws_cache.json 文件不存在: {self._ws_cache_path}")

    def _poll_tasks(self) -> None:
        for task in list(self.tasks.values()):
            proc = task.process
            if not proc:
                continue
            rc = proc.poll()
            if rc is None:
                task.status = "running"
                task.last_heartbeat = time.time()
                self._update_log_excerpt(task)
                if self._log_indicates_market_end(task):
                    task.status = "ended"
                    task.no_restart = True
                    task.end_reason = "market closed"
                    task.heartbeat("market end detected from log")
                    print(
                        f"[AUTO] topic={task.topic_id} 日志显示市场已结束，自动结束该话题。"
                    )
                    self._terminate_task(task, reason="market closed (auto)")
                continue
            self._handle_process_exit(task, rc)

        self._purge_inactive_tasks()

    def _handle_process_exit(self, task: TopicTask, rc: int) -> None:
        task.process = None
        if task.status not in {"stopped", "exited", "error", "ended"}:
            task.status = "exited" if rc == 0 else "error"
        task.heartbeat(f"process finished rc={rc}")
        self._update_log_excerpt(task)

        if task.no_restart:
            return

        if rc != 0:
            max_retries = max(0, int(self.config.process_start_retries))
            if task.restart_attempts < max_retries:
                running = sum(1 for t in self.tasks.values() if t.is_running())
                if running >= max(1, int(self.config.max_concurrent_tasks)):
                    if task.topic_id not in self.pending_topics:
                        self.pending_topics.append(task.topic_id)
                    task.status = "pending"
                    task.heartbeat("restart deferred due to max concurrency")
                    return
                task.restart_attempts += 1
                task.status = "restarting"
                task.heartbeat(
                    f"restart attempt {task.restart_attempts}/{max_retries} after rc={rc}"
                )
                time.sleep(self.config.process_retry_delay_sec)
                if self._start_topic_process(task.topic_id):
                    return
                if task.restart_attempts < max_retries and task.topic_id not in self.pending_topics:
                    self.pending_topics.append(task.topic_id)
            task.status = "error"

    def _update_log_excerpt(self, task: TopicTask, max_bytes: int = 2000) -> None:
        now = time.time()
        interval = max(0.0, float(self.config.log_excerpt_interval_sec))
        if interval and now - task.last_log_excerpt_ts < interval:
            return

        if not task.log_path or not task.log_path.exists():
            task.log_excerpt = ""
            return
        try:
            with task.log_path.open("rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - max_bytes))
                data = f.read().decode("utf-8", errors="ignore")
            lines = data.strip().splitlines()
            task.log_excerpt = "\n".join(lines[-5:])
            task.last_log_excerpt_ts = now
        except OSError as exc:  # pragma: no cover - 文件访问异常
            task.log_excerpt = f"<log read error: {exc}>"

    def _log_indicates_market_end(self, task: TopicTask) -> bool:
        excerpt = (task.log_excerpt or "").lower()
        if not excerpt:
            return False
        patterns = (
            "[market] 已确认市场结束",
            "[market] 市场结束",
            "[market] 达到市场截止时间",
            "[market] 收到市场关闭事件",
            "[exit] 最终状态",
        )
        return any(p.lower() in excerpt for p in patterns)

    def _schedule_pending_topics(self) -> None:
        running = sum(1 for t in self.tasks.values() if t.is_running())
        while (
            self.pending_topics
            and running < max(1, int(self.config.max_concurrent_tasks))
        ):
            now = time.time()
            if now < self._next_topic_start_at:
                break
            topic_id = self.pending_topics.pop(0)
            if topic_id in self.tasks and self.tasks[topic_id].is_running():
                continue
            try:
                started = self._start_topic_process(topic_id)
            except Exception as exc:  # pragma: no cover - 防御性保护
                print(f"[ERROR] 调度话题 {topic_id} 时异常: {exc}")
                traceback.print_exc()
                _log_error("TASK_SCHEDULE_ERROR", {
                    "message": "调度话题时异常",
                    "topic_id": topic_id,
                    "error": str(exc),
                    "traceback": traceback.format_exc()
                })
                started = False
            if not started and topic_id not in self.pending_topics:
                # 启动失败时重新入队，避免话题被遗忘
                self.pending_topics.append(topic_id)
            elif started:
                self._next_topic_start_at = now + max(
                    0.0, float(self.config.topic_start_cooldown_sec)
                )
            running = sum(1 for t in self.tasks.values() if t.is_running())

    def _schedule_pending_exit_cleanup(self) -> None:
        running = sum(1 for t in self.tasks.values() if t.is_running())
        while (
            self.pending_exit_topics
            and running < max(1, int(self.config.max_concurrent_tasks))
        ):
            token_id = self.pending_exit_topics.pop(0)
            if token_id in self.tasks and self.tasks[token_id].is_running():
                continue
            self._start_exit_cleanup(token_id)
            running = sum(1 for t in self.tasks.values() if t.is_running())

    def _get_order_base_volume(self) -> Optional[float]:
        return None

    def _build_run_config(self, topic_id: str) -> Dict[str, Any]:
        base_template_raw = json.loads(json.dumps(self.run_params_template or {}))
        base_template = {k: v for k, v in base_template_raw.items() if v is not None}

        base_raw = self.strategy_defaults.get("default", {}) or {}
        base = {k: v for k, v in base_raw.items() if v is not None}

        topic_overrides_raw = (self.strategy_defaults.get("topics") or {}).get(
            topic_id, {}
        )
        topic_overrides = {
            k: v for k, v in topic_overrides_raw.items() if v is not None
        }

        merged = {**base_template, **base, **topic_overrides}

        topic_info = self.topic_details.get(topic_id, {})
        slug = topic_info.get("slug")
        if slug:
            merged["market_url"] = f"https://polymarket.com/market/{slug}"
        merged["topic_id"] = topic_id

        if topic_info.get("title"):
            merged["topic_name"] = topic_info.get("title")
        if topic_info.get("token_id"):
            merged["token_id"] = topic_info.get("token_id")
        if not merged.get("token_id"):
            merged["token_id"] = topic_id
        merged["exit_signal_path"] = str(self._exit_signal_path(topic_id))
        if topic_info.get("yes_token"):
            merged["yes_token"] = topic_info.get("yes_token")
        if topic_info.get("no_token"):
            merged["no_token"] = topic_info.get("no_token")
        if topic_info.get("end_time"):
            merged["end_time"] = topic_info.get("end_time")

        base_order_size = _coerce_float(merged.get("order_size"))
        total_volume = _coerce_float(topic_info.get("total_volume"))
        volume_growth_factor = _coerce_float(merged.get("volume_growth_factor"))
        if base_order_size is not None and total_volume is not None:
            scaled_size = _scale_order_size_by_volume(
                base_order_size,
                total_volume,
                base_volume=self._get_order_base_volume(),
                growth_factor=volume_growth_factor
                if volume_growth_factor is not None and volume_growth_factor > 0
                else 0.5,
            )
            merged["order_size"] = scaled_size
        return merged

    def _should_use_shared_ws(self) -> bool:
        """
        判断是否应该使用共享 WS 模式（基于缓存文件新鲜度，而非线程状态）

        优势：
        - 避免启动时的竞态条件（线程可能正在初始化）
        - 避免运行时的竞态条件（线程重启过程中）
        - 更可靠：只要缓存数据新鲜就能用，不管线程是否临时崩溃

        Returns:
            bool: True 表示应该使用共享 WS 缓存
        """
        # 优先检查缓存文件新鲜度（最可靠的判断方式）
        try:
            if not self._ws_cache_path.exists():
                if hasattr(self, '_debug_shared_ws_check'):
                    print(f"[WS][CHECK] 缓存文件不存在: {self._ws_cache_path}")
                return False

            # 检查缓存文件是否在最近2分钟内更新过
            cache_age = time.time() - self._ws_cache_path.stat().st_mtime
            if cache_age < 120:  # 2分钟
                if hasattr(self, '_debug_shared_ws_check'):
                    print(f"[WS][CHECK] ✓ 缓存文件新鲜 (age={cache_age:.1f}s)")
                return True
            else:
                if hasattr(self, '_debug_shared_ws_check'):
                    print(f"[WS][CHECK] 缓存文件过期 (age={cache_age:.1f}s)")
        except OSError as e:
            if hasattr(self, '_debug_shared_ws_check'):
                print(f"[WS][CHECK] 文件访问失败: {e}")
            # 文件访问失败，继续下面的备用检查
            pass

        # 备用检查：聚合器线程和 WS 线程都存活
        # （只在缓存文件检查失败时才使用，作为双重保险）
        aggregator_alive = (
            self._ws_aggregator_thread
            and self._ws_aggregator_thread.is_alive()
        )
        ws_alive = self._ws_thread and self._ws_thread.is_alive()

        if hasattr(self, '_debug_shared_ws_check'):
            print(f"[WS][CHECK] 备用检查: aggregator={aggregator_alive}, ws={ws_alive}")

        return aggregator_alive and ws_alive

    def _start_topic_process(self, topic_id: str) -> bool:
        config_data = self._build_run_config(topic_id)
        cfg_path = self.config.data_dir / f"run_params_{_safe_topic_filename(topic_id)}.json"
        _dump_json_file(cfg_path, config_data)

        log_path = self.config.log_dir / f"autorun_{_safe_topic_filename(topic_id)}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            log_file = log_path.open("a", encoding="utf-8")
        except OSError as exc:  # pragma: no cover - 文件系统异常
            print(f"[ERROR] 无法创建日志文件 {log_path}: {exc}")
            return False

        max_stagger = max(0.0, float(self.config.process_stagger_max_sec))
        if max_stagger > 0:
            delay = random.uniform(0, max_stagger)
            if delay > 0:
                print(
                    f"[SCHEDULE] topic={topic_id} 启动前随机延迟 {delay:.2f}s 以错峰运行"
                )
                time.sleep(delay)

        # 构建命令行参数（不再使用环境变量）
        cmd = [
            sys.executable,
            str(MAKER_ROOT / "Volatility_arbitrage_run.py"),
            str(cfg_path),
        ]

        # 基于缓存新鲜度判断是否使用共享 WS 模式
        # 启用调试输出（首次子进程启动时）
        if not hasattr(self, '_first_child_started'):
            self._debug_shared_ws_check = True
            self._first_child_started = True

        should_use_shared = self._should_use_shared_ws()

        # 禁用调试输出（避免刷屏）
        if hasattr(self, '_debug_shared_ws_check'):
            delattr(self, '_debug_shared_ws_check')

        if should_use_shared:
            # 通过命令行参数传递共享缓存路径
            cmd.append(f"--shared-ws-cache={self._ws_cache_path}")
            print(f"[WS][CHILD] topic={topic_id[:8]}... → 共享 WS 模式 ✓")
        else:
            print(f"[WS][CHILD] topic={topic_id[:8]}... → 独立 WS 模式 ✗")

        proc: Optional[subprocess.Popen] = None
        attempts = max(1, int(self.config.process_start_retries))
        env = os.environ.copy()
        for attempt in range(1, attempts + 1):
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    env=env,
                )
                log_file.close()
                break
            except Exception as exc:  # pragma: no cover - 子进程异常
                print(
                    f"[ERROR] 启动 topic={topic_id} 失败（尝试 {attempt}/{attempts}）: {exc}"
                )
                if attempt >= attempts:
                    log_file.close()
                    return False
                time.sleep(self.config.process_retry_delay_sec)

        if not proc or proc.poll() is not None:
            rc_text = proc.poll() if proc else "?"
            print(
                f"[ERROR] topic={topic_id} 启动后立即退出 rc={rc_text}，将重试"
            )
            return False

        task = self.tasks.get(topic_id) or TopicTask(topic_id=topic_id)
        task.process = proc
        task.config_path = cfg_path
        task.log_path = log_path
        task.status = "running"
        task.heartbeat("started")
        self.tasks[topic_id] = task
        self._update_handled_topics([topic_id])
        print(f"[START] topic={topic_id} pid={proc.pid} log={log_path}")
        return True

    def _start_exit_cleanup(self, token_id: str) -> None:
        task = self.tasks.get(token_id)
        if task and task.is_running():
            return
        if token_id in self.pending_topics:
            try:
                self.pending_topics.remove(token_id)
            except ValueError:
                pass
        if token_id in self.pending_exit_topics:
            try:
                self.pending_exit_topics.remove(token_id)
            except ValueError:
                pass

        config_data = self._build_run_config(token_id)
        config_data["exit_only"] = True
        config_data["token_id"] = config_data.get("token_id") or token_id
        cfg_path = self.config.data_dir / f"run_params_{_safe_topic_filename(token_id)}.json"
        _dump_json_file(cfg_path, config_data)

        log_path = self.config.log_dir / f"autorun_exit_{_safe_topic_filename(token_id)}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            log_file = log_path.open("a", encoding="utf-8")
        except OSError as exc:  # pragma: no cover - 文件系统异常
            print(f"[ERROR] 无法创建清仓日志文件 {log_path}: {exc}")
            return

        # 构建命令行参数（不再使用环境变量）
        cmd = [
            sys.executable,
            str(MAKER_ROOT / "Volatility_arbitrage_run.py"),
            str(cfg_path),
        ]

        # 清仓进程总是尝试使用共享缓存（如果可用）
        if self._should_use_shared_ws():
            cmd.append(f"--shared-ws-cache={self._ws_cache_path}")
            print(f"[WS] 清仓进程将使用共享 WS 模式")
        else:
            print(f"[WS] 清仓进程将使用独立 WS 模式")

        env = os.environ.copy()
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=env,
            )
        except Exception as exc:  # pragma: no cover - 子进程异常
            log_file.close()
            print(f"[ERROR] 启动清仓进程失败 token={token_id}: {exc}")
            return
        log_file.close()

        task = task or TopicTask(topic_id=token_id)
        task.process = proc
        task.config_path = cfg_path
        task.log_path = log_path
        task.status = "running"
        task.no_restart = True
        task.end_reason = "sell signal cleanup"
        task.heartbeat("sell signal cleanup started")
        self.tasks[token_id] = task
        self._update_handled_topics([token_id])
        print(f"[EXIT-CLEAN] token={token_id} pid={proc.pid} log={log_path}")

    # ========== 历史记录 ==========
    def _load_handled_topics(self) -> None:
        self.handled_topics = read_handled_topics(self.config.handled_topics_path)
        if self.handled_topics:
            preview = ", ".join(sorted(self.handled_topics)[:5])
            print(
                f"[INIT] 已加载历史话题 {len(self.handled_topics)} 个 preview={preview}"
            )
        else:
            print("[INIT] 尚无历史处理话题记录")

    def _update_handled_topics(self, new_topics: List[str]) -> None:
        if not new_topics:
            return
        self.handled_topics.update(new_topics)
        write_handled_topics(self.config.handled_topics_path, self.handled_topics)

    # ========== 命令处理 ==========
    def enqueue_command(self, command: str) -> None:
        self.command_queue.put(command)

    def _process_commands(self) -> None:
        while True:
            try:
                cmd = self.command_queue.get_nowait()
            except queue.Empty:
                break
            print(f"[CMD] processing: {cmd}")
            self._handle_command(cmd.strip())

    def _handle_command(self, cmd: str) -> None:
        if not cmd:
            print("[CMD] 忽略空命令（可能未正确捕获输入或输入仅为空白）")
            return
        if cmd in {"quit", "exit"}:
            print("[CHOICE] exit requested")
            self.stop_event.set()
            return
        if cmd == "list":
            self._print_status()
            return
        if cmd.startswith("stop "):
            _, topic_id = cmd.split(" ", 1)
            self._stop_topic(topic_id.strip())
            return
        if cmd == "refresh":
            self._refresh_topics()
            return
        print(f"[WARN] 未识别命令: {cmd}")

    def _print_status(self) -> None:
        if not self.tasks:
            print("[RUN] 当前无运行中的话题")
            return
        running_tasks = self._ordered_running_tasks()
        if not running_tasks:
            print("[RUN] 当前无运行中的话题")
            return

        for idx, task in enumerate(running_tasks, 1):
            self._print_single_task(task, idx)

    def _print_single_task(self, task: TopicTask, index: Optional[int] = None) -> None:
        hb = task.last_heartbeat
        hb_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(hb)) if hb else "-"
        pid_text = str(task.process.pid) if task.process else "-"
        log_name = task.log_path.name if task.log_path else "-"
        log_hint = (task.log_excerpt.splitlines() or ["-"])[-1].strip()

        prefix = f"[RUN {index}]" if index is not None else "[RUN]"
        print(
            f"{prefix} topic={task.topic_id} status={task.status} "
            f"start={time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(task.start_time))} "
            f"pid={pid_text} hb={hb_text} notes={len(task.notes)} "
            f"log={log_name} last_line={log_hint or '-'}"
        )

    def _ordered_running_tasks(self) -> List[TopicTask]:
        return sorted(
            [task for task in self.tasks.values() if task.is_running()],
            key=lambda t: (t.start_time, t.topic_id),
        )

    def _stop_topic(self, topic_or_index: str) -> None:
        topic_id = self._resolve_topic_identifier(topic_or_index)
        if not topic_id:
            return
        task = self.tasks.get(topic_id)
        if not task:
            print(f"[WARN] topic {topic_id} 不在运行列表中")
            return
        task.no_restart = True
        task.end_reason = "stopped by user"
        # 标记为已处理，避免后续 refresh 把同一话题再次入队
        if topic_id not in self.handled_topics:
            self.handled_topics.add(topic_id)
            write_handled_topics(self.config.handled_topics_path, self.handled_topics)
        if topic_id in self.pending_topics:
            try:
                self.pending_topics.remove(topic_id)
            except ValueError:
                pass
        self._terminate_task(task, reason="stopped by user")
        self._purge_inactive_tasks()
        print(f"[CHOICE] stop topic={topic_id}")

    def _resolve_topic_identifier(self, text: str) -> Optional[str]:
        text = text.strip()
        if not text:
            print("[WARN] stop 命令缺少参数")
            return None
        if text.isdigit():
            index = int(text)
            running_tasks = self._ordered_running_tasks()
            if 1 <= index <= len(running_tasks):
                return running_tasks[index - 1].topic_id
            print(
                f"[WARN] 无效的序号 {index}，当前运行中的任务数为 {len(running_tasks)}"
            )
            return None
        return text

    def _terminate_task(self, task: TopicTask, reason: str) -> None:
        proc = task.process
        if proc and proc.poll() is None:
            try:
                proc.terminate()
            except Exception as exc:  # pragma: no cover - 终止异常
                print(f"[WARN] 无法终止 topic {task.topic_id}: {exc}")
            try:
                proc.wait(timeout=self.config.process_graceful_timeout_sec)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                    proc.wait(timeout=1.0)
                except Exception as exc:  # pragma: no cover - kill 失败
                    print(f"[WARN] 无法强杀 topic {task.topic_id}: {exc}")
        if task.status not in {"error", "ended"}:
            task.status = "stopped"
        task.heartbeat(reason)

    def _purge_inactive_tasks(self) -> None:
        """移除已停止/结束且不再需要展示的任务。"""

        removable: List[str] = []
        for topic_id, task in list(self.tasks.items()):
            if task.is_running():
                continue
            if task.status in {"stopped", "ended", "exited", "error"} or task.no_restart:
                removable.append(topic_id)

        if not removable:
            return

        for topic_id in removable:
            self.tasks.pop(topic_id, None)
            if topic_id in self.pending_topics:
                try:
                    self.pending_topics.remove(topic_id)
                except ValueError:
                    pass

    def _refresh_topics(self) -> None:
        try:
            self.latest_topics = self._load_copytrade_tokens()
            self.topic_details = {}
            for item in self.latest_topics:
                topic_id = _topic_id_from_entry(item)
                if not topic_id:
                    continue
                detail = dict(item)
                detail.setdefault("topic_id", topic_id)
                self.topic_details[topic_id] = detail
            sell_signals = self._load_copytrade_sell_signals()
            self._apply_sell_signals(sell_signals)
            new_topics = [
                topic_id
                for topic_id in compute_new_topics(self.latest_topics, self.handled_topics)
                if topic_id not in sell_signals
            ]
            if new_topics:
                preview = ", ".join(new_topics[:5])
                print(
                    f"[INCR] 新话题 {len(new_topics)} 个，将更新历史记录 preview={preview}"
                )
                for topic_id in new_topics:
                    if topic_id in self.pending_topics:
                        continue
                    if topic_id in self.tasks and self.tasks[topic_id].is_running():
                        continue
                    self.pending_topics.append(topic_id)
            else:
                print("[INCR] 无新增话题")
        except Exception as exc:  # pragma: no cover - 网络/外部依赖
            print(f"[ERROR] 读取 copytrade token 失败：{exc}")
            self.latest_topics = []

    def _load_copytrade_tokens(self) -> List[Dict[str, Any]]:
        path = self.config.copytrade_tokens_path
        if not path.exists():
            print(f"[WARN] copytrade token 文件不存在：{path}")
            return []
        payload = _load_json_file(path)
        raw_tokens = payload.get("tokens")
        if not isinstance(raw_tokens, list):
            print(f"[WARN] copytrade token 文件格式异常：{path}")
            return []
        topics: List[Dict[str, Any]] = []
        for item in raw_tokens:
            if not isinstance(item, dict):
                continue
            token_id = item.get("token_id") or item.get("tokenId")
            if not token_id:
                continue
            market_slug = item.get("market_slug") or item.get("slug")
            topics.append(
                {
                    "topic_id": str(token_id),
                    "token_id": str(token_id),
                    "slug": market_slug,
                    "last_seen": item.get("last_seen"),
                }
            )
        print(f"[COPYTRADE] 已读取 token {len(topics)} 条 | {path}")
        return topics

    def _load_copytrade_sell_signals(self) -> set[str]:
        path = self.config.copytrade_sell_signals_path
        if not path.exists():
            return set()
        payload = _load_json_file(path)
        raw_tokens = payload.get("sell_tokens")
        if not isinstance(raw_tokens, list):
            print(f"[WARN] copytrade sell_signal 文件格式异常：{path}")
            return set()
        signals: set[str] = set()
        skipped = 0
        for item in raw_tokens:
            if not isinstance(item, dict):
                continue
            token_id = item.get("token_id") or item.get("tokenId")
            if not token_id:
                continue
            if not item.get("introduced_by_buy", False):
                skipped += 1
                continue
            signals.add(str(token_id))
        if signals:
            preview = ", ".join(list(signals)[:5])
            print(f"[COPYTRADE] 已读取 sell 信号 {len(signals)} 条 preview={preview}")
        if skipped:
            print(f"[COPYTRADE] 已跳过未引入的 sell 信号 {skipped} 条")
        return signals

    def _exit_signal_path(self, token_id: str) -> Path:
        safe_id = _safe_topic_filename(token_id)
        return self.config.data_dir / f"exit_signal_{safe_id}.json"

    def _issue_exit_signal(self, token_id: str) -> None:
        path = self._exit_signal_path(token_id)
        payload = {
            "token_id": token_id,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        _dump_json_file(path, payload)

    def _apply_sell_signals(self, sell_signals: set[str]) -> None:
        if not sell_signals:
            return
        for token_id in sell_signals:
            task = self.tasks.get(token_id)
            has_running_task = bool(task and task.is_running())
            has_history = token_id in self.handled_topics
            if not has_running_task and not has_history:
                print(
                    "[COPYTRADE] 忽略 sell 信号，未进入 maker 队列: "
                    f"token_id={token_id}"
                )
                continue
            if token_id in self.pending_topics:
                try:
                    self.pending_topics.remove(token_id)
                except ValueError:
                    pass
            if task and task.is_running():
                task.no_restart = True
                task.end_reason = "sell signal"
                task.heartbeat("sell signal received")
            self._issue_exit_signal(token_id)
            if not (task and task.is_running()):
                if (
                    token_id not in self.pending_exit_topics
                    and token_id not in self.pending_topics
                ):
                    self.pending_exit_topics.append(token_id)

    def _cleanup_all_tasks(self) -> None:
        for task in list(self.tasks.values()):
            if task.is_running():
                print(f"[CLEAN] 停止 topic={task.topic_id} ...")
                self._terminate_task(task, reason="cleanup")
        # 写回 handled_topics，确保最新状态落盘
        write_handled_topics(self.config.handled_topics_path, self.handled_topics)

    def _restore_runtime_status(self) -> None:
        """尝试从上次运行的状态文件恢复待处理队列等信息。"""

        if not self.status_path.exists():
            return
        try:
            payload = _load_json_file(self.status_path)
            handled_topics = payload.get("handled_topics") or []
            pending_topics = payload.get("pending_topics") or []
            tasks_snapshot = payload.get("tasks") or {}
        except Exception as exc:  # pragma: no cover - 容错
            print(f"[WARN] 无法读取运行状态文件，已忽略: {exc}")
            return

        if handled_topics:
            self.handled_topics.update(str(t) for t in handled_topics)

        restored_topics: List[str] = []
        for topic_id in pending_topics:
            topic_id = str(topic_id)
            if topic_id in self.pending_topics or topic_id in self.handled_topics:
                continue
            restored_topics.append(topic_id)
            self.pending_topics.append(topic_id)

        for topic_id, info in tasks_snapshot.items():
            topic_id = str(topic_id)
            if topic_id in self.handled_topics:
                continue
            if topic_id not in self.pending_topics:
                restored_topics.append(topic_id)
                self.pending_topics.append(topic_id)

            task = TopicTask(topic_id=topic_id)
            task.status = "pending"
            task.notes.append("restored from runtime_status")
            config_path = info.get("config_path")
            log_path = info.get("log_path")
            if config_path:
                task.config_path = Path(config_path)
            if log_path:
                task.log_path = Path(log_path)
            self.tasks[topic_id] = task

        if restored_topics:
            preview = ", ".join(restored_topics[:5])
            print(f"[RESTORE] 已从运行状态恢复 {len(restored_topics)} 个话题：{preview}")

        pending_exit_topics = payload.get("pending_exit_topics") or []
        for topic_id in pending_exit_topics:
            topic_id = str(topic_id)
            if topic_id in self.pending_exit_topics:
                continue
            self.pending_exit_topics.append(topic_id)

    def _dump_runtime_status(self) -> None:
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "handled_topics_total": len(self.handled_topics),
            "handled_topics": sorted(self.handled_topics),
            "pending_topics": list(self.pending_topics),
            "pending_exit_topics": list(self.pending_exit_topics),
            "tasks": {},
        }
        for topic_id, task in self.tasks.items():
            payload["tasks"][topic_id] = {
                "status": task.status,
                "pid": task.process.pid if task.process else None,
                "last_heartbeat": task.last_heartbeat,
                "notes": task.notes,
                "log_path": str(task.log_path) if task.log_path else None,
                "config_path": str(task.config_path) if task.config_path else None,
            }
        _dump_json_file(self.status_path, payload)
        print(f"[STATE] 已写入运行状态到 {self.status_path}")

    # ========== 入口方法 ==========
    def command_loop(self) -> None:
        try:
            prompt_shown = False
            while not self.stop_event.is_set():
                try:
                    if not prompt_shown:
                        # 主动刷新提示符，避免被后台日志刷屏覆盖
                        print("poly> ", end="", flush=True)
                        prompt_shown = True

                    ready, _, _ = select.select(
                        [sys.stdin], [], [], self.config.command_poll_sec
                    )
                    if not ready:
                        continue

                    line = sys.stdin.readline()
                    if line == "":
                        cmd = "exit"
                    else:
                        cmd = line.rstrip("\n")
                    prompt_shown = False
                except EOFError:
                    cmd = "exit"
                except Exception as exc:  # pragma: no cover - 保护交互循环不被意外异常终止
                    print(f"[ERROR] command loop input failed: {exc}")
                    traceback.print_exc()
                    time.sleep(self.config.command_poll_sec)
                    continue
                # 立刻反馈收到的命令，避免在日志刷屏时用户误以为命令未被捕获
                if cmd:
                    print(f"[CMD] received: {cmd}")
                else:
                    # 空行依旧入队，后续会在 _handle_command 里被忽略
                    print("[CMD] received: <empty>")
                self.enqueue_command(cmd)
                # 轻微休眠，防止输入为空或重复换行时产生过多提示刷屏
                time.sleep(self.config.command_poll_sec)
        except KeyboardInterrupt:
            print("\n[WARN] Ctrl+C detected, stopping...")
            self.stop_event.set()
        except Exception as exc:  # pragma: no cover - 防御性保护
            print(f"[ERROR] command loop crashed: {exc}")
            traceback.print_exc()


# =====================
# CLI 入口
# =====================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket maker autorun")
    parser.add_argument(
        "--global-config",
        type=Path,
        default=MAKER_ROOT / "config" / "global_config.json",
        help="全局调度配置 JSON 路径",
    )
    parser.add_argument(
        "--strategy-config",
        type=Path,
        default=MAKER_ROOT / "config" / "strategy_defaults.json",
        help="策略参数模板 JSON 路径",
    )
    parser.add_argument(
        "--run-config-template",
        type=Path,
        default=MAKER_ROOT / "config" / "run_params.json",
        help="运行参数模板 JSON 路径（传递给 Volatility_arbitrage_run.py）",
    )
    parser.add_argument(
        "--no-repl",
        action="store_true",
        help="禁用交互式命令循环，仅按配置运行",
    )
    parser.add_argument(
        "--command",
        action="append",
        help="启动后自动执行的命令（可多次提供），例如 list 或 stop <topic_id>",
    )
    return parser.parse_args(argv)


def load_configs(
    args: argparse.Namespace,
) -> tuple[GlobalConfig, Dict[str, Any], Dict[str, Any]]:
    global_conf_raw = _load_json_file(args.global_config)
    strategy_conf_raw = _load_json_file(args.strategy_config)
    run_params_template = _load_json_file(args.run_config_template)
    return (
        GlobalConfig.from_dict(global_conf_raw),
        strategy_conf_raw,
        run_params_template,
    )


def main(argv: Optional[List[str]] = None) -> None:
    print("=" * 60)
    print("[INIT] Polymarket Maker AutoRun - 聚合器启动")
    print("[VERSION] 支持book/tick事件处理 (2026-01-21)")
    print("=" * 60)

    args = parse_args(argv)
    global_conf, strategy_conf, run_params_template = load_configs(args)

    manager = AutoRunManager(global_conf, strategy_conf, run_params_template)

    def _handle_sigterm(signum: int, frame: Any) -> None:  # pragma: no cover - 信号处理不可测
        print(f"\n[WARN] signal {signum} received, exiting...")
        manager.stop_event.set()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    worker = threading.Thread(target=manager.run_loop, daemon=True)
    worker.start()

    if args.command:
        for cmd in args.command:
            manager.enqueue_command(cmd)

    if args.no_repl or args.command:
        try:
            while worker.is_alive():
                time.sleep(global_conf.command_poll_sec)
        except KeyboardInterrupt:
            print("\n[WARN] Ctrl+C detected, stopping...")
            manager.stop_event.set()
    else:
        manager.command_loop()

    worker.join()


if __name__ == "__main__":
    main()
