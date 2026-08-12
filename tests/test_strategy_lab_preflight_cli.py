from argparse import Namespace
import asyncio

import pytest

from scripts.strategy_lab_preflight import CONFIRMATIONS, main_async


def _args(**overrides):
    values = {
        "profile": "1h",
        "confirm": CONFIRMATIONS["1h"],
        "duration_sec": 1,
        "cycle_interval_sec": 1,
        "cycle_duration_sec": 1,
        "max_symbols_per_cycle": 1,
        "candidate_limit": 1,
        "output_root": None,
    }
    values.update(overrides)
    return Namespace(**values)


def test_24h_profile_requires_its_exact_confirmation():
    args = _args(profile="24h", confirm=CONFIRMATIONS["1h"], duration_sec=86400)
    with pytest.raises(ValueError, match="invalid_confirmation"):
        asyncio.run(main_async(args))


def test_24h_profile_rejects_shortened_duration_before_io():
    args = _args(profile="24h", confirm=CONFIRMATIONS["24h"], duration_sec=3600)
    with pytest.raises(ValueError, match="requires duration_sec=86400"):
        asyncio.run(main_async(args))


def test_1h_profile_cannot_expand_to_24h_before_io():
    args = _args(duration_sec=86400)
    with pytest.raises(ValueError, match="1h profile duration_sec"):
        asyncio.run(main_async(args))
