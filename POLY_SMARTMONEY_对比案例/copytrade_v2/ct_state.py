from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


DEFAULT_STATE: Dict[str, Any] = {
    "token_map": {},
    "open_orders": {},
    "open_orders_all": {},
    "managed_order_ids": [],
    "intent_keys": {},
    "last_mid_price_by_token_id": {},
    "last_mid_price_update_ts": 0,
    "last_sync_ts": 0,
    "target_last_shares": {},
    "target_last_seen_ts": {},
    "target_missing_streak": {},
    "cooldown_until": {},
    "target_last_event_ts": {},
    "topic_state": {},
    "target_actions_cursor_ts": 0,
    "target_actions_cursor_ms": 0,
    "seen_action_ids": [],
    "run_start_ms": 0,
    "boot_run_start_ms": 0,
    "bootstrapped": False,
    "boot_token_ids": [],
    "boot_token_keys": [],
    "target_last_shares_by_token_key": {},
    "probed_token_ids": [],
    "last_reprice_ts_by_token": {},
    "adopted_existing_orders": False,
    "shadow_buy_orders": [],
    "taker_buy_orders": [],
    "recent_buy_orders": [],
    "seen_my_trade_ids": [],
    "my_trades_cursor_ms": 0,
    "my_trades_unreliable_until": 0,
}


def load_state(path: str) -> Dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return dict(DEFAULT_STATE)
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return dict(DEFAULT_STATE)
    state = dict(DEFAULT_STATE)
    if isinstance(payload, dict):
        state.update(payload)
    if "token_map" not in state or not isinstance(state["token_map"], dict):
        state["token_map"] = {}
    if "open_orders" not in state or not isinstance(state["open_orders"], dict):
        state["open_orders"] = {}
    if "open_orders_all" not in state or not isinstance(state["open_orders_all"], dict):
        state["open_orders_all"] = {}
    if "managed_order_ids" not in state or not isinstance(state["managed_order_ids"], list):
        state["managed_order_ids"] = []
    if "intent_keys" not in state or not isinstance(state["intent_keys"], dict):
        state["intent_keys"] = {}
    if "last_mid_price_by_token_id" not in state or not isinstance(
        state["last_mid_price_by_token_id"], dict
    ):
        state["last_mid_price_by_token_id"] = {}
    if "last_mid_price_update_ts" not in state or not isinstance(
        state["last_mid_price_update_ts"], (int, float)
    ):
        state["last_mid_price_update_ts"] = 0
    if "target_last_shares" not in state or not isinstance(state["target_last_shares"], dict):
        state["target_last_shares"] = {}
    if "target_last_seen_ts" not in state or not isinstance(state["target_last_seen_ts"], dict):
        state["target_last_seen_ts"] = {}
    if "target_missing_streak" not in state or not isinstance(state["target_missing_streak"], dict):
        state["target_missing_streak"] = {}
    if "cooldown_until" not in state or not isinstance(state["cooldown_until"], dict):
        state["cooldown_until"] = {}
    if "target_last_event_ts" not in state or not isinstance(state["target_last_event_ts"], dict):
        state["target_last_event_ts"] = {}
    if "topic_state" not in state or not isinstance(state["topic_state"], dict):
        state["topic_state"] = {}
    if "target_actions_cursor_ts" not in state or not isinstance(
        state["target_actions_cursor_ts"], (int, float)
    ):
        state["target_actions_cursor_ts"] = 0
    if "target_actions_cursor_ms" not in state or not isinstance(
        state["target_actions_cursor_ms"], (int, float)
    ):
        state["target_actions_cursor_ms"] = 0
    if (
        int(state.get("target_actions_cursor_ms") or 0) <= 0
        and int(state.get("target_actions_cursor_ts") or 0) > 0
    ):
        state["target_actions_cursor_ms"] = int(state["target_actions_cursor_ts"]) * 1000
    if "seen_action_ids" not in state or not isinstance(state["seen_action_ids"], list):
        state["seen_action_ids"] = []
    if "run_start_ms" not in state or not isinstance(state["run_start_ms"], (int, float)):
        state["run_start_ms"] = 0
    if "boot_run_start_ms" not in state or not isinstance(state["boot_run_start_ms"], (int, float)):
        state["boot_run_start_ms"] = 0
    if "bootstrapped" not in state or not isinstance(state["bootstrapped"], bool):
        state["bootstrapped"] = False
    if "boot_token_ids" not in state or not isinstance(state["boot_token_ids"], list):
        state["boot_token_ids"] = []
    if "boot_token_keys" not in state or not isinstance(state["boot_token_keys"], list):
        state["boot_token_keys"] = []
    if "target_last_shares_by_token_key" not in state or not isinstance(
        state["target_last_shares_by_token_key"], dict
    ):
        state["target_last_shares_by_token_key"] = {}
    if "probed_token_ids" not in state or not isinstance(state["probed_token_ids"], list):
        state["probed_token_ids"] = []
    if "last_reprice_ts_by_token" not in state or not isinstance(
        state["last_reprice_ts_by_token"], dict
    ):
        state["last_reprice_ts_by_token"] = {}
    if "adopted_existing_orders" not in state or not isinstance(
        state["adopted_existing_orders"], bool
    ):
        state["adopted_existing_orders"] = False
    if "shadow_buy_orders" not in state or not isinstance(state["shadow_buy_orders"], list):
        state["shadow_buy_orders"] = []
    if "taker_buy_orders" not in state or not isinstance(state["taker_buy_orders"], list):
        state["taker_buy_orders"] = []
    if "recent_buy_orders" not in state or not isinstance(state["recent_buy_orders"], list):
        state["recent_buy_orders"] = []
    if "seen_my_trade_ids" not in state or not isinstance(state["seen_my_trade_ids"], list):
        state["seen_my_trade_ids"] = []
    if "my_trades_cursor_ms" not in state or not isinstance(
        state["my_trades_cursor_ms"], (int, float)
    ):
        state["my_trades_cursor_ms"] = 0
    if "my_trades_unreliable_until" not in state or not isinstance(
        state["my_trades_unreliable_until"], (int, float)
    ):
        state["my_trades_unreliable_until"] = 0
    return state


def save_state(path: str, state: Dict[str, Any]) -> None:
    file_path = Path(path)
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp_path.replace(file_path)
