from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping


@dataclass(frozen=True, slots=True)
class MoneyPolicy:
    policy_id: str
    slot_margin_usd: float
    max_positions: int = 4
    guaranteed_topup_per_position_usd: float = 50.0
    shared_emergency_usd: float = 75.0
    operating_floor_usd: float = 25.0

    @property
    def base_commitment_usd(self) -> float:
        return self.slot_margin_usd * self.max_positions

    @property
    def protected_capital_required_usd(self) -> float:
        return (
            self.base_commitment_usd
            + self.guaranteed_topup_per_position_usd * self.max_positions
            + self.shared_emergency_usd
            + self.operating_floor_usd
        )


DEFAULT_POLICIES = (
    MoneyPolicy("current_175", 175.0),
    MoneyPolicy("balanced_150", 150.0),
    MoneyPolicy("conservative_125", 125.0),
)


def target_capital_migration_snapshot(
    state: Mapping[str, Any],
    *,
    wallet_total_usd: float,
    target_capital_usd: float = 3_000.0,
    legacy_slot_margin_usd: float = 175.0,
) -> dict[str, Any]:
    open_positions = [
        item for item in state.get("positions") or [] if item.get("status") in {"open", "opening"}
    ]
    target_deployable = target_capital_usd * 0.70
    target_reserve = target_capital_usd * 0.30
    target_slot = target_deployable / 4.0
    target_guarantee = target_reserve * (200.0 / 300.0) / 4.0
    target_shared = target_reserve * (75.0 / 300.0)
    target_floor = target_reserve * (25.0 / 300.0)
    free_slots = max(0, 4 - len(open_positions))
    gradual_new_slots = min(1, free_slots)
    mixed_deployable_commitment = (
        len(open_positions) * legacy_slot_margin_usd
        + gradual_new_slots * target_slot
    )
    return {
        "target_capital_usd": round(target_capital_usd, 6),
        "wallet_total_usd": round(wallet_total_usd, 6),
        "deposit_to_exact_target_usd": round(max(0.0, target_capital_usd - wallet_total_usd), 6),
        "open_legacy_positions": len(open_positions),
        "free_slots": free_slots,
        "target_deployable_usd": round(target_deployable, 6),
        "target_reserve_usd": round(target_reserve, 6),
        "target_slot_margin_usd": round(target_slot, 6),
        "target_position_notional_at_3x_usd": round(target_slot * 3.0, 6),
        "target_guaranteed_topup_per_position_usd": round(target_guarantee, 6),
        "target_shared_emergency_usd": round(target_shared, 6),
        "target_operating_floor_usd": round(target_floor, 6),
        "target_max_total_topup_usd": round(target_guarantee * 4.0 + target_shared, 6),
        "gradual_first_mixed_commitment_usd": round(
            mixed_deployable_commitment + target_reserve,
            6,
        ),
        "gradual_first_mixed_headroom_usd": round(
            target_capital_usd - mixed_deployable_commitment - target_reserve,
            6,
        ),
        "current_runtime_supports_mixed_cohorts": True,
        "runtime_policy_ids": ["v1_1000", "v2_3000"],
        "initial_v2_concurrent_entry_cap": 1,
        "promotion_confirmation": "PROMOTE PUMP CAPITAL 3000",
        "recommended_transition": "deposit_excluded_then_explicit_promotion_then_one_v2_canary",
        "warning": (
            "do not resize existing ladders; promotion remains explicit and the first "
            "v2 policy is limited to one concurrent new position"
        ),
    }


def tier_prefund_usd(row: Mapping[str, Any]) -> float:
    rule = str(row.get("rule_slug") or "")
    if "legs5_equal" in rule:
        return 30.0
    if "legs3_tapered" in rule:
        return 25.0
    if "legs2_tapered" in rule:
        return 50.0
    # Unknown historical tiers use the largest configured entry prefund.
    return 50.0


def load_main_historical_trades(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    return [row for row in rows if row.get("strategy_id") == "main_pullback_tier"]


def peak_concurrent_prefund_usd(rows: Iterable[Mapping[str, Any]]) -> float:
    events: list[tuple[int, int, float]] = []
    for row in rows:
        entry_ts = int(float(row.get("entry_ts") or 0))
        exit_ts = int(float(row.get("exit_ts") or 0))
        amount = tier_prefund_usd(row)
        if entry_ts <= 0 or exit_ts <= entry_ts:
            continue
        events.append((entry_ts, 1, amount))
        events.append((exit_ts, -1, amount))
    # Process exits before entries at the same timestamp.
    events.sort(key=lambda item: (item[0], item[1]))
    current = 0.0
    peak = 0.0
    for _ts, direction, amount in events:
        current += direction * amount
        peak = max(peak, current)
    return peak


def policy_summary(
    trades: list[Mapping[str, Any]],
    policy: MoneyPolicy,
    *,
    total_capital_usd: float = 1_000.0,
    historical_slot_margin_usd: float = 750.0,
) -> dict[str, Any]:
    scale = policy.slot_margin_usd / historical_slot_margin_usd
    pnl_values = [float(row.get("pnl_usd") or 0.0) * scale for row in trades]
    equity = total_capital_usd
    peak_equity = equity
    max_drawdown = 0.0
    for pnl in sorted(
        zip((int(float(row.get("exit_ts") or 0)) for row in trades), pnl_values),
        key=lambda item: item[0],
    ):
        equity += pnl[1]
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)
    historical_peak_prefund = peak_concurrent_prefund_usd(trades)
    return {
        **asdict(policy),
        "total_capital_usd": round(total_capital_usd, 6),
        "trades": len(trades),
        "historical_scale_from_750": round(scale, 8),
        "historical_net_pnl_usd": round(sum(pnl_values), 6),
        "historical_final_equity_usd": round(total_capital_usd + sum(pnl_values), 6),
        "historical_roi_on_total_pct": round(sum(pnl_values) / total_capital_usd * 100.0, 6),
        "historical_max_drawdown_usd": round(max_drawdown, 6),
        "historical_max_drawdown_pct": round(max_drawdown / total_capital_usd * 100.0, 6),
        "historical_peak_tier_prefund_usd": round(historical_peak_prefund, 6),
        "protected_capital_required_usd": round(policy.protected_capital_required_usd, 6),
        "unallocated_after_protection_usd": round(
            total_capital_usd - policy.protected_capital_required_usd,
            6,
        ),
        "fits_total_capital": policy.protected_capital_required_usd <= total_capital_usd + 1e-9,
        "warning": "linear fixed-slot replay; survivor bias and live slippage remain",
    }


def current_wallet_snapshot(
    state: Mapping[str, Any],
    *,
    wallet_total_usd: float,
    wallet_available_usd: float,
    policy: MoneyPolicy = DEFAULT_POLICIES[0],
) -> dict[str, Any]:
    open_positions = [
        item for item in state.get("positions") or [] if item.get("status") in {"open", "opening"}
    ]
    current_topup = sum(float(item.get("margin_topup_usd") or 0.0) for item in open_positions)
    remaining_topup_capacity = max(
        0.0,
        policy.guaranteed_topup_per_position_usd * policy.max_positions
        + policy.shared_emergency_usd
        - current_topup,
    )
    required_before_next = (
        policy.slot_margin_usd
        + remaining_topup_capacity
        + policy.operating_floor_usd
    )
    return {
        "wallet_total_usd": round(wallet_total_usd, 6),
        "wallet_available_usd": round(wallet_available_usd, 6),
        "open_positions": len(open_positions),
        "free_slots": max(0, policy.max_positions - len(open_positions)),
        "current_topup_usd": round(current_topup, 6),
        "remaining_topup_capacity_usd": round(remaining_topup_capacity, 6),
        "required_available_before_next_slot_usd": round(required_before_next, 6),
        "next_slot_safe_by_dynamic_guard": (
            len(open_positions) < policy.max_positions
            and wallet_available_usd + 1e-9 >= required_before_next
        ),
        "headroom_after_dynamic_guard_usd": round(wallet_available_usd - required_before_next, 6),
        "legacy_static_guard_required_usd": round(300.0 + policy.slot_margin_usd, 6),
        "legacy_static_guard_pass": wallet_available_usd + 1e-9 >= 300.0 + policy.slot_margin_usd,
    }


def write_report(
    *,
    historical_trades_path: Path,
    output_dir: Path,
    live_state_path: Path | None = None,
    wallet_total_usd: float | None = None,
    wallet_available_usd: float | None = None,
    target_capital_usd: float = 3_000.0,
) -> dict[str, Any]:
    trades = load_main_historical_trades(historical_trades_path)
    summaries = [policy_summary(trades, policy) for policy in DEFAULT_POLICIES]
    current: dict[str, Any] | None = None
    migration: dict[str, Any] | None = None
    if (
        live_state_path is not None
        and live_state_path.exists()
        and wallet_total_usd is not None
        and wallet_available_usd is not None
    ):
        state = json.loads(live_state_path.read_text(encoding="utf-8"))
        current = current_wallet_snapshot(
            state,
            wallet_total_usd=wallet_total_usd,
            wallet_available_usd=wallet_available_usd,
        )
        migration = target_capital_migration_snapshot(
            state,
            wallet_total_usd=wallet_total_usd,
            target_capital_usd=target_capital_usd,
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "policy_summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    metadata = {
        "schema": "pump_live_money_management_research_v1",
        "historical_trades_path": str(historical_trades_path),
        "historical_trades": len(trades),
        "policies": summaries,
        "current_wallet_snapshot": current,
        "target_capital_migration": migration,
        "research_only": True,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return metadata
