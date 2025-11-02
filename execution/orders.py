from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

OrderSide = Literal["buy", "sell"]
OrderType = Literal["market", "limit"]
OrderStatus = Literal["new", "filled", "partial", "cancelled", "failed"]


@dataclass(slots=True)
class OrderRequest:
    exchange: str
    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType = "market"
    price: float | None = None
    post_only: bool = False
    client_order_id: str | None = None


@dataclass(slots=True)
class OrderFill:
    exchange: str
    symbol: str
    side: OrderSide
    requested_quantity: float
    filled_quantity: float
    average_price: float
    status: OrderStatus = "filled"
    client_order_id: str | None = None
    exchange_order_id: str | None = None

