from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from uuid import uuid4

from config import BASE_DIR

try:
    import ccxt  # type: ignore
except ImportError:  # pragma: no cover - optional runtime dependency
    ccxt = None


logger = logging.getLogger(__name__)

PUMP_TRANSFER_STATE_DIR = BASE_DIR / "data" / "research" / "bybit_pump_short_live"
PUMP_TRANSFER_STATE_FILE = "temporary_transfers.json"
PUMP_TRANSFER_EVENTS_FILE = "temporary_transfer_events.jsonl"
PUMP_TRANSFER_MIN_USDT = Decimal("0.01")
PUMP_TRANSFER_MAX_USDT = Decimal("100000")
PUMP_TRANSFER_IN_CONFIRMATION = "TRANSFER TEMPORARY USDT MAIN TO PUMP"
PUMP_TRANSFER_RETURN_CONFIRMATION = "RETURN TEMPORARY USDT PUMP TO MAIN"
PUMP_TRANSFER_CONFIRM_DELAYS_SEC = (0.0, 0.2, 0.5, 1.0, 2.0)
PUMP_TRANSFER_STATE_RETRY_SEC = (0.0, 0.05, 0.1, 0.2, 0.4)
PUMP_TRANSFER_ENV_PATH = BASE_DIR / "config" / "pump_live.env"
AUTO_TRANSFER_MAIN_FLOOR_USD = 2_000.0
AUTO_TRANSFER_MAX_SINGLE_USD = 50.0
AUTO_TRANSFER_DAILY_CAP_USD = 200.0
AUTO_TRANSFER_COOLDOWN_SEC = 300
AUTO_TRANSFER_ROUND_USD = 5.0


class PumpTransferGateway(Protocol):
    def credentials_status(self) -> dict[str, Any]: ...

    def preflight(self) -> dict[str, Any]: ...

    def fetch_balances(self) -> dict[str, dict[str, Any]]: ...

    def create_transfer(
        self,
        *,
        direction: str,
        amount_usdt: str,
        transfer_id: str,
    ) -> dict[str, Any]: ...

    def fetch_transfer(self, *, direction: str, transfer_id: str) -> dict[str, Any] | None: ...


class PumpTransferAccounting(Protocol):
    def status(self) -> dict[str, Any]: ...

    def record_temporary_transfer(
        self,
        *,
        direction: str,
        amount_usd: float,
        transfer_id: str,
    ) -> dict[str, Any]: ...


def _read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip():
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result else default


def _amount(value: Any) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("pump_temporary_transfer_amount_invalid") from exc
    if not amount.is_finite() or amount < PUMP_TRANSFER_MIN_USDT:
        raise ValueError("pump_temporary_transfer_amount_below_minimum")
    if amount > PUMP_TRANSFER_MAX_USDT:
        raise ValueError("pump_temporary_transfer_amount_above_maximum")
    if amount.as_tuple().exponent < -6:
        raise ValueError("pump_temporary_transfer_amount_precision_exceeded")
    return amount.normalize()


def _amount_text(amount: Decimal) -> str:
    text = format(amount, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def load_auto_transfer_config(path: Path = PUMP_TRANSFER_ENV_PATH) -> dict[str, Any]:
    values = _read_env(path)
    enabled = str(values.get("PUMP_LIVE_AUTO_TRANSFER_ENABLED", "0")).lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    return {
        "enabled": enabled,
        "main_wallet_floor_usd": max(
            0.0,
            _number(
                values.get("PUMP_LIVE_AUTO_TRANSFER_MAIN_WALLET_FLOOR_USD"),
                AUTO_TRANSFER_MAIN_FLOOR_USD,
            ),
        ),
        "max_single_usd": max(
            float(PUMP_TRANSFER_MIN_USDT),
            _number(
                values.get("PUMP_LIVE_AUTO_TRANSFER_MAX_SINGLE_USD"),
                AUTO_TRANSFER_MAX_SINGLE_USD,
            ),
        ),
        "daily_cap_usd": max(
            float(PUMP_TRANSFER_MIN_USDT),
            _number(
                values.get("PUMP_LIVE_AUTO_TRANSFER_DAILY_CAP_USD"),
                AUTO_TRANSFER_DAILY_CAP_USD,
            ),
        ),
        "cooldown_sec": max(
            60,
            int(
                _number(
                    values.get("PUMP_LIVE_AUTO_TRANSFER_COOLDOWN_SEC"),
                    AUTO_TRANSFER_COOLDOWN_SEC,
                )
            ),
        ),
        "round_usd": max(
            float(PUMP_TRANSFER_MIN_USDT),
            _number(
                values.get("PUMP_LIVE_AUTO_TRANSFER_ROUND_USD"),
                AUTO_TRANSFER_ROUND_USD,
            ),
        ),
    }


class BybitPumpTransferGateway:
    """Least-privilege Bybit main/sub universal-transfer adapter."""

    def __init__(
        self,
        *,
        main_env_path: Path = BASE_DIR / ".env",
        pump_env_path: Path = BASE_DIR / "config" / "pump_live.env",
    ) -> None:
        self.main_env_path = main_env_path
        self.pump_env_path = pump_env_path
        self._clients: dict[str, Any] = {}
        self._signatures: dict[str, tuple[str, str, bool]] = {}
        self._identity_cache: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def _credentials(self, role: str) -> tuple[str, str, bool, str]:
        pump = _read_env(self.pump_env_path)
        main = _read_env(self.main_env_path)
        testnet = pump.get("BYBIT_PUMP_TESTNET", "0").lower() in {"1", "true", "yes", "on"}
        if role == "master":
            dedicated_key = pump.get("BYBIT_PUMP_MASTER_TRANSFER_API_KEY", "").strip()
            dedicated_secret = pump.get("BYBIT_PUMP_MASTER_TRANSFER_API_SECRET", "").strip()
            if dedicated_key or dedicated_secret:
                return dedicated_key, dedicated_secret, testnet, "dedicated_pump_transfer"
            return (
                main.get("BYBIT_API_KEY", "").strip(),
                main.get("BYBIT_API_SECRET", "").strip(),
                testnet,
                "main_trading_fallback",
            )
        dedicated_key = pump.get("BYBIT_PUMP_SUB_TRANSFER_API_KEY", "").strip()
        dedicated_secret = pump.get("BYBIT_PUMP_SUB_TRANSFER_API_SECRET", "").strip()
        if dedicated_key or dedicated_secret:
            return dedicated_key, dedicated_secret, testnet, "dedicated_pump_sub_transfer"
        return (
            pump.get("BYBIT_PUMP_API_KEY", "").strip(),
            pump.get("BYBIT_PUMP_API_SECRET", "").strip(),
            testnet,
            "pump_trading_fallback",
        )

    def credentials_status(self) -> dict[str, Any]:
        master_key, master_secret, testnet, source = self._credentials("master")
        pump_key, pump_secret, pump_testnet, pump_source = self._credentials("pump")
        return {
            "master_key_present": bool(master_key),
            "master_secret_present": bool(master_secret),
            "master_key_source": source,
            "pump_key_present": bool(pump_key),
            "pump_secret_present": bool(pump_secret),
            "pump_key_source": pump_source,
            "testnet": testnet,
            "environment_matches": testnet == pump_testnet,
            "ready": bool(master_key and master_secret and pump_key and pump_secret),
        }

    def _client(self, role: str) -> Any:
        if ccxt is None:
            raise RuntimeError("ccxt_not_installed")
        key, secret, testnet, _source = self._credentials(role)
        if not key or not secret:
            raise RuntimeError(f"pump_transfer_{role}_credentials_missing")
        signature = (key, secret, testnet)
        if self._clients.get(role) is not None and self._signatures.get(role) == signature:
            return self._clients[role]
        client = ccxt.bybit(
            {
                "apiKey": key,
                "secret": secret,
                "enableRateLimit": True,
                "options": {
                    "defaultType": "swap",
                    "defaultSettle": "USDT",
                    "adjustForTimeDifference": True,
                    "recvWindow": 10_000,
                },
            }
        )
        if testnet:
            client.set_sandbox_mode(True)
        client.load_time_difference()
        self._clients[role] = client
        self._signatures[role] = signature
        self._identity_cache.pop(role, None)
        return client

    def _request(self, role: str, operation: str, callback: Callable[[], Any]) -> Any:
        try:
            return callback()
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc).lower()
            if "10002" not in message and "recv_window" not in message and "timestamp" not in message:
                raise
            client = self._client(role)
            client.load_time_difference()
            logger.warning("Pump transfer Bybit time sync retry role=%s operation=%s", role, operation)
            return callback()

    def _identity(self, role: str, *, refresh: bool = False) -> dict[str, Any]:
        if not refresh and role in self._identity_cache:
            return dict(self._identity_cache[role])
        client = self._client(role)
        payload = self._request(
            role,
            "query_api_key",
            lambda: client.private_get_v5_user_query_api({}),
        )
        result = dict((payload or {}).get("result") or {})
        self._identity_cache[role] = result
        return dict(result)

    @staticmethod
    def _balance_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
        balance = dict(payload.get("balance") or {})
        transfer_balance = _number(balance.get("transferBalance"), 0.0)
        transfer_safe_raw = str(balance.get("transferSafeAmount") or "").strip()
        transfer_safe = _number(transfer_safe_raw, transfer_balance)
        return {
            "wallet_usd": round(_number(balance.get("walletBalance"), 0.0), 8),
            "transfer_balance_usd": round(transfer_balance, 8),
            "transfer_safe_usd": round(max(0.0, min(transfer_balance, transfer_safe)), 8),
        }

    def fetch_balances(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            master_info = self._identity("master")
            pump_info = self._identity("pump")
            master_uid = str(master_info.get("userID") or "")
            pump_uid = str(pump_info.get("userID") or "")
            if not master_uid or not pump_uid:
                raise RuntimeError("pump_transfer_uid_missing")
            master = self._client("master")
            pump = self._client("pump")
            result: dict[str, dict[str, Any]] = {}
            requests = (
                (
                    "main",
                    "master",
                    master,
                    pump_uid,
                ),
                (
                    "pump",
                    "pump",
                    pump,
                    master_uid,
                ),
            )
            for name, role, client, target_uid in requests:
                try:
                    payload = self._request(
                        role,
                        f"{name}_transfer_balance",
                        lambda client=client, target_uid=target_uid: client.private_get_v5_asset_transfer_query_account_coin_balance(
                            {
                                "accountType": "UNIFIED",
                                "coin": "USDT",
                                "toMemberId": target_uid,
                                "toAccountType": "UNIFIED",
                                "withTransferSafeAmount": 1,
                            }
                        ),
                    )
                    result[name] = self._balance_payload(
                        dict((payload or {}).get("result") or {})
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    result[name] = {
                        "wallet_usd": 0.0,
                        "transfer_balance_usd": 0.0,
                        "transfer_safe_usd": 0.0,
                        "available": False,
                        "error": f"balance_unavailable:{type(exc).__name__}",
                    }
            return result

    def preflight(self) -> dict[str, Any]:
        checked_at_ms = int(time.time() * 1000)
        credentials = self.credentials_status()
        errors: list[str] = []
        warnings: list[str] = []
        if not credentials["ready"]:
            return {
                "ready": False,
                "ready_in": False,
                "ready_out": False,
                "checked_at_ms": checked_at_ms,
                "credentials": credentials,
                "errors": ["pump_transfer_credentials_missing"],
                "warnings": [],
                "minimum_test_usdt": float(PUMP_TRANSFER_MIN_USDT),
            }
        try:
            with self._lock:
                master_info = self._identity("master", refresh=True)
                pump_info = self._identity("pump", refresh=True)
                master_uid = str(master_info.get("userID") or "")
                pump_uid = str(pump_info.get("userID") or "")
                configured_pump_uid = _read_env(self.pump_env_path).get("BYBIT_PUMP_SUB_UID", "")
                master_wallet = {
                    str(item) for item in ((master_info.get("permissions") or {}).get("Wallet") or [])
                }
                pump_wallet = {
                    str(item) for item in ((pump_info.get("permissions") or {}).get("Wallet") or [])
                }
                master_identity_ok = bool(master_info.get("isMaster")) and bool(master_uid)
                pump_identity_ok = not bool(pump_info.get("isMaster")) and bool(pump_uid)
                relation_ok = (
                    bool(master_uid and pump_uid)
                    and str(pump_info.get("parentUid") or "") == master_uid
                    and (not configured_pump_uid or configured_pump_uid == pump_uid)
                )
                master_write = int(master_info.get("readOnly") or 0) == 0
                pump_write = int(pump_info.get("readOnly") or 0) == 0
                master_uta = int(master_info.get("uta") or 0) == 1
                pump_uta = int(pump_info.get("uta") or 0) == 1
                master_permission = {"AccountTransfer", "SubMemberTransfer"}.issubset(
                    master_wallet
                )
                pump_permission = {"AccountTransfer", "SubMemberTransferList"}.issubset(
                    pump_wallet
                )
                if not master_identity_ok:
                    errors.append("pump_transfer_master_key_identity_invalid")
                if not pump_identity_ok:
                    errors.append("pump_transfer_sub_key_identity_invalid")
                if not relation_ok:
                    errors.append("pump_transfer_main_sub_relationship_mismatch")
                if not master_write:
                    errors.append("pump_transfer_master_key_read_only")
                if not pump_write:
                    errors.append("pump_transfer_sub_key_read_only")
                if not master_uta:
                    errors.append("pump_transfer_master_not_uta")
                if not pump_uta:
                    errors.append("pump_transfer_sub_not_uta")
                if not master_permission:
                    errors.append("pump_transfer_master_permission_missing")
                if not pump_permission:
                    errors.append("pump_transfer_sub_permission_missing")
                if "Withdraw" in master_wallet:
                    errors.append("pump_transfer_master_withdraw_permission_forbidden")
                if credentials["master_key_source"] == "main_trading_fallback":
                    warnings.append("pump_transfer_uses_main_trading_key_fallback")

                ready_in = (
                    master_identity_ok
                    and relation_ok
                    and master_write
                    and master_uta
                    and master_permission
                    and "Withdraw" not in master_wallet
                )
                ready_out = (
                    pump_identity_ok
                    and relation_ok
                    and pump_write
                    and pump_uta
                    and pump_permission
                )
                balances: dict[str, dict[str, Any]] = {}
                if ready_in or ready_out:
                    balances = self.fetch_balances()
                if ready_in and not bool((balances.get("main") or {}).get("available", True)):
                    errors.append("pump_transfer_main_safe_balance_unavailable")
                    ready_in = False
                if ready_out and not bool((balances.get("pump") or {}).get("available", True)):
                    errors.append("pump_transfer_sub_safe_balance_unavailable")
                    ready_out = False
                # Bybit's transferable-coin endpoint is only for transfers
                # between different account types and rejects
                # UNIFIED -> UNIFIED with retCode 131203. Universal member
                # transfers use different UIDs, so the fresh USDT
                # transferBalance/transferSafeAmount checks above are the
                # applicable fail-closed capability and amount gate.
                ready = ready_in and ready_out
                return {
                    "ready": ready,
                    "ready_in": ready_in,
                    "ready_out": ready_out,
                    "checked_at_ms": checked_at_ms,
                    "credentials": credentials,
                    "identity": {
                        "master_is_master": bool(master_info.get("isMaster")),
                        "pump_is_master": bool(pump_info.get("isMaster")),
                        "relationship_matches": relation_ok,
                        "master_wallet_permissions": sorted(master_wallet),
                        "pump_wallet_permissions": sorted(pump_wallet),
                    },
                    "balances": balances,
                    "minimum_test_usdt": float(PUMP_TRANSFER_MIN_USDT),
                    "errors": errors,
                    "warnings": warnings,
                }
        except Exception as exc:  # pylint: disable=broad-except
            return {
                "ready": False,
                "ready_in": False,
                "ready_out": False,
                "checked_at_ms": checked_at_ms,
                "credentials": credentials,
                "errors": [f"pump_transfer_preflight_failed:{type(exc).__name__}"],
                "warnings": warnings,
                "minimum_test_usdt": float(PUMP_TRANSFER_MIN_USDT),
            }

    def create_transfer(
        self,
        *,
        direction: str,
        amount_usdt: str,
        transfer_id: str,
    ) -> dict[str, Any]:
        with self._lock:
            master_uid = str(self._identity("master").get("userID") or "")
            pump_uid = str(self._identity("pump").get("userID") or "")
            if direction == "main_to_pump":
                role, source_uid, target_uid = "master", master_uid, pump_uid
            elif direction == "pump_to_main":
                role, source_uid, target_uid = "pump", pump_uid, master_uid
            else:
                raise ValueError("pump_temporary_transfer_direction_invalid")
            client = self._client(role)
            payload = self._request(
                role,
                "create_universal_transfer",
                lambda: client.private_post_v5_asset_transfer_universal_transfer(
                    {
                        "transferId": transfer_id,
                        "coin": "USDT",
                        "amount": amount_usdt,
                        "fromMemberId": int(source_uid),
                        "toMemberId": int(target_uid),
                        "fromAccountType": "UNIFIED",
                        "toAccountType": "UNIFIED",
                    }
                ),
            )
            result = dict((payload or {}).get("result") or {})
            return {
                "transfer_id": str(result.get("transferId") or transfer_id),
                "status": str(result.get("status") or "STATUS_UNKNOWN").upper(),
            }

    def fetch_transfer(self, *, direction: str, transfer_id: str) -> dict[str, Any] | None:
        role = "master" if direction == "main_to_pump" else "pump"
        client = self._client(role)
        payload = self._request(
            role,
            "query_universal_transfer",
            lambda: client.private_get_v5_asset_transfer_query_universal_transfer_list(
                {"transferId": transfer_id, "limit": 1}
            ),
        )
        rows = list(((payload or {}).get("result") or {}).get("list") or [])
        for row in rows:
            if str(row.get("transferId") or "") == transfer_id:
                return {
                    "transfer_id": transfer_id,
                    "status": str(row.get("status") or "STATUS_UNKNOWN").upper(),
                    "coin": str(row.get("coin") or ""),
                    "amount": str(row.get("amount") or ""),
                    "direction": direction,
                }
        return None


class PumpTemporaryTransferController:
    def __init__(
        self,
        *,
        accounting: PumpTransferAccounting,
        gateway: PumpTransferGateway | None = None,
        state_dir: Path = PUMP_TRANSFER_STATE_DIR,
        env_path: Path = PUMP_TRANSFER_ENV_PATH,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.accounting = accounting
        self.gateway = gateway or BybitPumpTransferGateway()
        self.state_dir = state_dir
        self.env_path = env_path
        self.state_path = state_dir / PUMP_TRANSFER_STATE_FILE
        self.events_path = state_dir / PUMP_TRANSFER_EVENTS_FILE
        self._sleep = sleep
        self._lock = threading.RLock()
        self._state = self._load_state()

    def _load_state(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return {
            "schema": "pump_temporary_transfers_v1",
            "temporary_outstanding_usd": round(_number(payload.get("temporary_outstanding_usd")), 6),
            "cumulative_in_usd": round(_number(payload.get("cumulative_in_usd")), 6),
            "cumulative_returned_usd": round(_number(payload.get("cumulative_returned_usd")), 6),
            "pending": payload.get("pending") if isinstance(payload.get("pending"), dict) else None,
            "operations": list(payload.get("operations") or [])[-200:],
            "last_preflight": payload.get("last_preflight"),
            "last_auto_attempt_at_ms": payload.get("last_auto_attempt_at_ms"),
            "last_auto_result": payload.get("last_auto_result"),
            "updated_at_ms": payload.get("updated_at_ms"),
        }

    def status(self) -> dict[str, Any]:
        with self._lock:
            payload = json.loads(json.dumps(self._state))
        payload["credentials"] = self.gateway.credentials_status()
        payload["minimum_test_usdt"] = float(PUMP_TRANSFER_MIN_USDT)
        payload["state_file"] = str(self.state_path)
        payload["events_file"] = str(self.events_path)
        auto = self._auto_status(payload)
        payload["auto_risk"] = auto
        return payload

    def _auto_status(self, state: Mapping[str, Any] | None = None) -> dict[str, Any]:
        config = load_auto_transfer_config(self.env_path)
        if state is None:
            with self._lock:
                snapshot = json.loads(json.dumps(self._state))
        else:
            snapshot = dict(state)
        day_start_ms = int(time.time() * 1000) // 86_400_000 * 86_400_000
        daily_used = sum(
            _number(item.get("amount_usd"))
            for item in snapshot.get("operations") or []
            if item.get("direction") == "main_to_pump"
            and item.get("origin") == "auto_risk"
            and item.get("status") == "complete"
            and int(_number(item.get("completed_at_ms"))) >= day_start_ms
        )
        return {
            **config,
            "daily_used_usd": round(daily_used, 6),
            "daily_remaining_usd": round(
                max(0.0, config["daily_cap_usd"] - daily_used),
                6,
            ),
            "last_attempt_at_ms": snapshot.get("last_auto_attempt_at_ms"),
            "last_result": snapshot.get("last_auto_result"),
        }

    def auto_transfer_for_risk(
        self,
        *,
        requested_usd: float,
        symbol: str,
        liq_buffer_pct: float,
        desired_topup_usd: float,
        available_usd: float,
    ) -> dict[str, Any]:
        config = load_auto_transfer_config(self.env_path)
        if not config["enabled"]:
            return {"status": "disabled", "reason": "auto_transfer_disabled"}
        requested = max(0.0, _number(requested_usd))
        if requested < float(PUMP_TRANSFER_MIN_USDT):
            return {"status": "not_needed", "reason": "cash_shortfall_below_minimum"}
        rounded = math.ceil(requested / config["round_usd"] - 1e-12) * config["round_usd"]
        now = int(time.time() * 1000)
        auto_status = self._auto_status()
        last_attempt = int(_number(auto_status.get("last_attempt_at_ms")))
        context = {
            "symbol": str(symbol),
            "liq_buffer_pct": round(_number(liq_buffer_pct), 6),
            "desired_topup_usd": round(_number(desired_topup_usd), 6),
            "available_usd": round(_number(available_usd), 6),
            "requested_usd": round(requested, 6),
        }
        reason = None
        if self._state.get("pending"):
            reason = "pending_reconciliation"
        elif last_attempt and now - last_attempt < config["cooldown_sec"] * 1000:
            reason = "cooldown"
        elif rounded > config["max_single_usd"] + 1e-9:
            reason = "max_single_exceeded"
        elif rounded > auto_status["daily_remaining_usd"] + 1e-9:
            reason = "daily_cap_exceeded"
        if reason:
            if reason == "cooldown":
                return {
                    "status": "cooldown",
                    "reason": reason,
                    "amount_usd": rounded,
                    **context,
                }
            return self._record_auto_result(
                {"status": "blocked", "reason": reason, "amount_usd": rounded, **context},
                now=now,
            )

        preflight = self.preflight()
        balances = dict(preflight.get("balances") or {})
        main = dict(balances.get("main") or {})
        main_wallet = _number(main.get("wallet_usd"))
        main_above_floor = max(0.0, main_wallet - config["main_wallet_floor_usd"])
        safe_inbound = min(_number(preflight.get("inbound_limit_usd")), main_above_floor)
        if not preflight.get("round_trip_ready"):
            reason = "round_trip_preflight_not_ready"
        elif rounded > safe_inbound + 1e-9:
            reason = "main_wallet_floor_or_transfer_limit"
        if reason:
            return self._record_auto_result(
                {
                    "status": "blocked",
                    "reason": reason,
                    "amount_usd": rounded,
                    "main_wallet_usd": main_wallet,
                    "main_wallet_floor_usd": config["main_wallet_floor_usd"],
                    "safe_inbound_usd": safe_inbound,
                    **context,
                },
                now=now,
            )

        self._record_auto_result(
            {"status": "submitting", "amount_usd": rounded, **context},
            now=now,
        )
        result = self._execute(
            "main_to_pump",
            _amount(rounded),
            preflight,
            origin="auto_risk",
            context=context,
        )
        operation = dict(result.get("operation") or {})
        completed = {
            "status": "complete",
            "amount_usd": rounded,
            "transfer_id": operation.get("transfer_id"),
            **context,
        }
        self._record_auto_result(completed, now=int(time.time() * 1000))
        return completed

    def _record_auto_result(self, result: Mapping[str, Any], *, now: int) -> dict[str, Any]:
        payload = dict(result)
        with self._lock:
            self._state["last_auto_attempt_at_ms"] = now
            self._state["last_auto_result"] = payload
            self._state["updated_at_ms"] = now
            self._save_state_locked()
        self._event("auto_risk_transfer", payload)
        return payload

    def preflight(self) -> dict[str, Any]:
        exchange = self.gateway.preflight()
        pump = self.accounting.status()
        balances = dict(exchange.get("balances") or {})
        pump_balance = dict(balances.get("pump") or {})
        main_balance = dict(balances.get("main") or {})
        with self._lock:
            outstanding = _number(self._state.get("temporary_outstanding_usd"))
            pending = self._state.get("pending")
        open_positions = [
            item for item in pump.get("positions") or [] if item.get("status") not in {"closed"}
        ]
        total_topup = sum(_number(item.get("margin_topup_usd")) for item in open_positions)
        config = dict(pump.get("config") or {})
        remaining_topup = max(0.0, _number(config.get("max_total_topup_usd"), 275.0) - total_topup)
        operating_floor = _number(config.get("operating_cash_floor_usd"), 25.0)
        active_capital = _number(
            (pump.get("capital_manager") or {}).get("active_strategy_capital_usd"),
            _number(config.get("total_capital_usd"), 1000.0),
        )
        live_balance = dict(pump.get("last_balance") or {})
        live_available = _number(live_balance.get("available"))
        live_wallet = _number(live_balance.get("wallet"), _number(live_balance.get("total")))
        exchange_pump_wallet = _number(pump_balance.get("wallet_usd"))
        capital_floor_wallet = exchange_pump_wallet if exchange_pump_wallet > 0 else live_wallet
        reserve_return_limit = max(0.0, live_available - remaining_topup - operating_floor)
        capital_return_limit = max(0.0, capital_floor_wallet - active_capital)
        exchange_return_limit = _number(pump_balance.get("transfer_safe_usd"))
        return_limit = max(
            0.0,
            min(outstanding, reserve_return_limit, capital_return_limit, exchange_return_limit),
        )
        inbound_limit = _number(main_balance.get("transfer_safe_usd"))
        errors = list(exchange.get("errors") or [])
        if pending:
            errors.append("pump_temporary_transfer_pending_reconciliation")
        result = {
            **exchange,
            "ready": bool(exchange.get("ready")) and not pending and not errors,
            "ready_in": bool(exchange.get("ready_in")) and not pending and inbound_limit >= float(PUMP_TRANSFER_MIN_USDT),
            "ready_out": bool(exchange.get("ready_out")) and not pending and return_limit >= float(PUMP_TRANSFER_MIN_USDT),
            "round_trip_ready": (
                bool(exchange.get("ready_in"))
                and bool(exchange.get("ready_out"))
                and not pending
                and inbound_limit >= float(PUMP_TRANSFER_MIN_USDT)
            ),
            "temporary_outstanding_usd": round(outstanding, 6),
            "inbound_limit_usd": round(inbound_limit, 6),
            "return_limit_usd": round(return_limit, 6),
            "return_guard": {
                "live_available_usd": round(live_available, 6),
                "remaining_topup_capacity_usd": round(remaining_topup, 6),
                "operating_floor_usd": round(operating_floor, 6),
                "active_strategy_capital_usd": round(active_capital, 6),
                "live_wallet_usd": round(live_wallet, 6),
                "exchange_pump_wallet_usd": round(exchange_pump_wallet, 6),
            },
            "errors": errors,
        }
        with self._lock:
            self._state["last_preflight"] = result
            self._state["updated_at_ms"] = int(time.time() * 1000)
            self._save_state_locked()
        self._event("transfer_preflight", {"ready": result["ready"], "errors": errors})
        return result

    def transfer_in(self, amount_usdt: Any, confirmation: str) -> dict[str, Any]:
        if confirmation != PUMP_TRANSFER_IN_CONFIRMATION:
            raise ValueError("pump_temporary_transfer_in_confirmation_invalid")
        amount = _amount(amount_usdt)
        preflight = self.preflight()
        if not preflight.get("round_trip_ready"):
            raise RuntimeError("pump_temporary_transfer_round_trip_preflight_not_ready")
        if float(amount) > _number(preflight.get("inbound_limit_usd")) + 1e-9:
            raise RuntimeError("pump_temporary_transfer_in_exceeds_safe_balance")
        return self._execute("main_to_pump", amount, preflight)

    def transfer_return(self, amount_usdt: Any, confirmation: str) -> dict[str, Any]:
        if confirmation != PUMP_TRANSFER_RETURN_CONFIRMATION:
            raise ValueError("pump_temporary_transfer_return_confirmation_invalid")
        amount = _amount(amount_usdt)
        preflight = self.preflight()
        if not preflight.get("ready_out"):
            raise RuntimeError("pump_temporary_transfer_return_preflight_not_ready")
        if float(amount) > _number(preflight.get("return_limit_usd")) + 1e-9:
            raise RuntimeError("pump_temporary_transfer_return_exceeds_safe_limit")
        return self._execute("pump_to_main", amount, preflight)

    def _execute(
        self,
        direction: str,
        amount: Decimal,
        preflight: Mapping[str, Any],
        *,
        origin: str = "operator",
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        transfer_id = str(uuid4())
        now = int(time.time() * 1000)
        operation = {
            "transfer_id": transfer_id,
            "direction": direction,
            "coin": "USDT",
            "amount_usd": float(amount),
            "status": "submitting",
            "accounting_status": "pending",
            "created_at_ms": now,
            "origin": origin,
            "context": dict(context or {}),
            "balances_before": dict(preflight.get("balances") or {}),
        }
        with self._lock:
            if self._state.get("pending"):
                raise RuntimeError("pump_temporary_transfer_pending_reconciliation")
            self._state["pending"] = operation
            self._state["updated_at_ms"] = now
            self._save_state_locked()
        self._event("temporary_transfer_submitting", operation)
        try:
            submitted = self.gateway.create_transfer(
                direction=direction,
                amount_usdt=_amount_text(amount),
                transfer_id=transfer_id,
            )
        except Exception as exc:  # pylint: disable=broad-except
            error_family = type(exc).__name__
            with self._lock:
                pending = dict(self._state.get("pending") or {})
                pending.update({"status": "outcome_unknown", "error": error_family})
                self._state["pending"] = pending
                self._state["updated_at_ms"] = int(time.time() * 1000)
                self._save_state_locked()
            self._event(
                "temporary_transfer_outcome_unknown",
                {"transfer_id": transfer_id, "direction": direction, "error": error_family},
            )
            raise RuntimeError(f"pump_temporary_transfer_outcome_unknown:{transfer_id}") from exc
        with self._lock:
            pending = dict(self._state.get("pending") or {})
            pending["submit_status"] = submitted.get("status")
            pending["status"] = "confirming"
            self._state["pending"] = pending
            self._save_state_locked()
        return self._confirm_pending()

    def reconcile(self) -> dict[str, Any]:
        with self._lock:
            if not self._state.get("pending"):
                return self.status()
        return self._confirm_pending()

    def _confirm_pending(self) -> dict[str, Any]:
        with self._lock:
            pending = dict(self._state.get("pending") or {})
        if not pending:
            return self.status()
        transfer_id = str(pending.get("transfer_id") or "")
        direction = str(pending.get("direction") or "")
        record: dict[str, Any] | None = None
        for delay in PUMP_TRANSFER_CONFIRM_DELAYS_SEC:
            if delay > 0:
                self._sleep(delay)
            record = self.gateway.fetch_transfer(direction=direction, transfer_id=transfer_id)
            if record and record.get("status") in {"SUCCESS", "FAILED"}:
                break
        if not record or record.get("status") not in {"SUCCESS", "FAILED"}:
            with self._lock:
                pending = dict(self._state.get("pending") or {})
                pending["status"] = "confirmation_pending"
                self._state["pending"] = pending
                self._save_state_locked()
            raise RuntimeError(f"pump_temporary_transfer_confirmation_pending:{transfer_id}")
        if record.get("status") == "FAILED":
            with self._lock:
                pending = dict(self._state.get("pending") or {})
                pending.update({"status": "failed", "confirmed_at_ms": int(time.time() * 1000)})
                self._state["operations"] = (self._state.get("operations") or [])[-199:] + [pending]
                self._state["pending"] = None
                self._save_state_locked()
            self._event("temporary_transfer_failed", pending)
            raise RuntimeError(f"pump_temporary_transfer_failed:{transfer_id}")

        amount = _number(pending.get("amount_usd"))
        try:
            confirmed_amount = Decimal(str(record.get("amount") or ""))
        except (InvalidOperation, TypeError, ValueError):
            confirmed_amount = Decimal("-1")
        confirmation_matches = (
            str(record.get("transfer_id") or "") == transfer_id
            and str(record.get("coin") or "").upper() == "USDT"
            and confirmed_amount.is_finite()
            and confirmed_amount == Decimal(str(pending.get("amount_usd")))
        )
        if not confirmation_matches:
            with self._lock:
                pending = dict(self._state.get("pending") or {})
                pending["status"] = "confirmation_mismatch"
                self._state["pending"] = pending
                self._save_state_locked()
            self._event(
                "temporary_transfer_confirmation_mismatch",
                {"transfer_id": transfer_id, "direction": direction},
            )
            raise RuntimeError(
                f"pump_temporary_transfer_confirmation_mismatch:{transfer_id}"
            )
        with self._lock:
            pending = dict(self._state.get("pending") or {})
            pending["status"] = "exchange_confirmed"
            pending["confirmed_at_ms"] = int(time.time() * 1000)
            self._state["pending"] = pending
            self._save_state_locked()
        accounting_result = self.accounting.record_temporary_transfer(
            direction=direction,
            amount_usd=amount,
            transfer_id=transfer_id,
        )
        balances_after = self.gateway.fetch_balances()
        with self._lock:
            pending = dict(self._state.get("pending") or {})
            if direction == "main_to_pump":
                self._state["temporary_outstanding_usd"] = round(
                    _number(self._state.get("temporary_outstanding_usd")) + amount,
                    6,
                )
                self._state["cumulative_in_usd"] = round(
                    _number(self._state.get("cumulative_in_usd")) + amount,
                    6,
                )
            else:
                current = _number(self._state.get("temporary_outstanding_usd"))
                if amount > current + 1e-9:
                    raise RuntimeError("pump_temporary_transfer_accounting_outstanding_underflow")
                self._state["temporary_outstanding_usd"] = round(max(0.0, current - amount), 6)
                self._state["cumulative_returned_usd"] = round(
                    _number(self._state.get("cumulative_returned_usd")) + amount,
                    6,
                )
            pending.update(
                {
                    "status": "complete",
                    "accounting_status": "complete",
                    "balances_after": balances_after,
                    "completed_at_ms": int(time.time() * 1000),
                }
            )
            self._state["operations"] = (self._state.get("operations") or [])[-199:] + [pending]
            self._state["pending"] = None
            self._state["updated_at_ms"] = int(time.time() * 1000)
            self._save_state_locked()
        self._event(
            "temporary_transfer_complete",
            {
                "transfer_id": transfer_id,
                "direction": direction,
                "amount_usd": amount,
                "temporary_outstanding_usd": self._state["temporary_outstanding_usd"],
            },
        )
        return {"operation": pending, "accounting": accounting_result, "status": self.status()}

    def _save_state_locked(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self._state, indent=2, sort_keys=True)
        last_error: OSError | None = None
        for delay in PUMP_TRANSFER_STATE_RETRY_SEC:
            if delay:
                self._sleep(delay)
            temp = self.state_path.with_name(f".{self.state_path.name}.{uuid4().hex}.tmp")
            try:
                temp.write_text(payload, encoding="utf-8")
                os.replace(temp, self.state_path)
                return
            except OSError as exc:
                last_error = exc
            finally:
                try:
                    temp.unlink(missing_ok=True)
                except OSError:
                    pass
        if last_error is not None:
            raise last_error

    def _event(self, event: str, payload: Mapping[str, Any]) -> None:
        row = {"ts_ms": int(time.time() * 1000), "event": event, **dict(payload)}
        self.state_dir.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
