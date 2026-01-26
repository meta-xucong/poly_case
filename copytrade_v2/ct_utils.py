from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Optional


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def round_to_tick(price: float, tick_size: float, *, direction: str = "down") -> float:
    if tick_size <= 0:
        return float(price)
    quant = Decimal(str(tick_size))
    raw = Decimal(str(price))
    rounding = ROUND_DOWN if direction == "down" else ROUND_UP
    return float(raw.quantize(quant, rounding=rounding))


def safe_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None
