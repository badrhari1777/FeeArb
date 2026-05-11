from __future__ import annotations

COIN_ANALYSIS_FEATURE_SET_VERSION = "v1"

CANDIDATE_ACTIONS: tuple[str, ...] = (
    "NO_TRADE",
    "ENTRY_SMALL",
    "ENTRY_STRONG",
)

POSITION_ACTIONS: tuple[str, ...] = (
    "HOLD",
    "PARTIAL_EXIT",
    "FULL_EXIT",
    "ADD_SMALL",
    "ADD_BLOCKED",
)

DECISION_PHASES: tuple[str, ...] = (
    "exploratory",
    "mid_interval",
    "pre_boundary_20m",
    "pre_boundary_15m",
    "boundary_immediate",
    "post_boundary",
)

POSITION_TYPES: tuple[str, ...] = (
    "paper",
    "real_manual",
)
