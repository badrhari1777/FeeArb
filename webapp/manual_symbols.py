from __future__ import annotations


def _normalize_input_symbol(symbol: str) -> str:
    text = (symbol or "").strip().upper()
    if ":" in text:
        text = text.split(":", 1)[0]
    if not text:
        return text
    if "/" in text:
        parts = text.split("/", 1)
        text = parts[0] + parts[1]
    text = text.replace("-", "").replace("_", "")
    if text.endswith("USDTM"):
        text = text[:-1]
    if text.endswith("UMCBL") or text.endswith("DMCBL"):
        text = text[:-5]
    if text.endswith("SWAP"):
        text = text[:-4]
    if text.endswith("PERP"):
        text = text[:-4]
    if text.endswith("USDT") or text.endswith("USD"):
        return text
    return f"{text}USDT"


def _normalize_bybit_symbol(symbol: str) -> str:
    symbol = _normalize_input_symbol(symbol)
    return symbol


def _normalize_bingx_symbol(symbol: str) -> str:
    symbol = _normalize_input_symbol(symbol)
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"
    return symbol


def _normalize_mexc_symbol(symbol: str) -> str:
    symbol = _normalize_input_symbol(symbol)
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}_USDT"
    return symbol


def _normalize_bitget_symbol(symbol: str) -> str:
    symbol = _normalize_input_symbol(symbol)
    return symbol


def _normalize_okx_symbol(symbol: str) -> str:
    symbol = _normalize_input_symbol(symbol)
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        return f"{base}-USDT-SWAP"
    return symbol


def _normalize_gate_symbol(symbol: str) -> str:
    symbol = _normalize_input_symbol(symbol)
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}_USDT"
    return symbol


def _normalize_kucoin_symbol(symbol: str) -> str:
    symbol = _normalize_input_symbol(symbol)
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        if base == "BTC":
            base = "XBT"
        return f"{base}USDTM"
    return symbol
