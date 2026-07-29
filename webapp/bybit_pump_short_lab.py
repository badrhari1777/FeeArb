from __future__ import annotations

import csv
import json
import re
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

from analysis_collectors.bybit_pump_short import (
    DEFAULT_OUTPUT_DIR,
    BybitCollectorConfig,
    BybitInstrument,
    BybitPumpShortCollector,
    dedupe_instruments,
    is_crypto_pump_short_instrument,
    normalize_symbol,
    now_ms,
)
from analysis_features.bybit_pump_short_outcomes import DEFAULT_OUTPUT_DIR as DEFAULT_ANALYSIS_OUTPUT_DIR
from analysis_features.bybit_pump_short_paper import read_paper_summary
from analysis_features.bybit_pump_short_shadow import (
    DEFAULT_SHADOW_OUTPUT_DIR,
    SLOW_PUMP_WATCH_CONFIGS,
    SLOW_PUMP_WATCH_HISTORY_FILE,
    SLOW_PUMP_WATCH_LATEST_FILE,
    SLOW_PUMP_WATCH_RECENT_HOURS,
    ShadowScanConfig,
    run_shadow_scan,
)
from execution.pump_live import PumpLiveController

PUMP_DASHBOARD_CAPITAL_USD = 1_000.0
PUMP_DASHBOARD_MAX_ACTIVE_COINS = 3
PUMP_DASHBOARD_LEVERAGE = 3.0
PUMP_DASHBOARD_LADDER_LEGS = 4
PUMP_DASHBOARD_LADDER_STEP_PCT = 50.0
PUMP_DASHBOARD_FUNDING_WINDOW_H = 24
PUMP_DASHBOARD_FUNDING_MIN_PCT = -0.5
PUMP_DASHBOARD_TP_PCT = 25.0
PUMP_DASHBOARD_MAX_HOLD_H = 168

PUMP_STRATEGY_MONITOR_CAPITAL_USD = 3_000.0
PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS = 4
PUMP_STRATEGY_MONITOR_LEVERAGE = 3.0
PUMP_STRATEGY_MONITOR_AUDIT_FILE = "strategy_monitor_audit.jsonl"
PUMP_STRATEGY_PAPER_STATE_FILE = "strategy_paper_positions.json"
PUMP_STRATEGY_PAPER_EVENTS_FILE = "strategy_paper_events.jsonl"
PUMP_STRATEGY_PAPER_CSV_FILE = "strategy_paper_positions_latest.csv"
PUMP_STRATEGY_PAPER_FEE_ROUNDTRIP_PCT = 0.18
PUMP_SHADOW_SCHEDULE_STATE_FILE = "shadow_schedule_state.json"
PUMP_CYCLE_PAPER_STATE_FILE = "pump_cycle_paper_positions.json"
PUMP_CYCLE_PAPER_EVENTS_FILE = "pump_cycle_paper_events.jsonl"
PUMP_CYCLE_PAPER_CSV_FILE = "pump_cycle_paper_positions_latest.csv"
PUMP_CYCLE_CANDIDATE_PAPER_STATE_FILE = "pump_cycle_candidate_paper_positions.json"
PUMP_CYCLE_CANDIDATE_PAPER_EVENTS_FILE = "pump_cycle_candidate_paper_events.jsonl"
PUMP_CYCLE_CANDIDATE_PAPER_CSV_FILE = "pump_cycle_candidate_paper_positions_latest.csv"
PUMP_CYCLE_CAPITAL_USD = 3_000.0
PUMP_CYCLE_TOTAL_SLOTS = 6
PUMP_CYCLE_SHORT_SLOTS = 4
PUMP_CYCLE_LONG_SLOTS = 2
PUMP_CYCLE_SLOT_BUDGET_USD = PUMP_CYCLE_CAPITAL_USD / PUMP_CYCLE_TOTAL_SLOTS
PUMP_CYCLE_LONG_LEVERAGE = 2.0
PUMP_CYCLE_SHORT_LEVERAGE = 3.0
PUMP_CYCLE_LONG_TP_PCT = 30.0
PUMP_CYCLE_LONG_STOP_PCT = 25.0
PUMP_CYCLE_LONG_MAX_HOLD_H = 72
PUMP_CYCLE_FEE_ROUNDTRIP_PCT = 0.18
PUMP_CYCLE_CANDIDATE_SHORT_SLOTS = 4
PUMP_CYCLE_CANDIDATE_LONG_SLOTS = 2
PUMP_CYCLE_MAIN_TRACKS = (
    "short_main_tiered",
    "long_broad",
    "long_clean_oi",
    "long_high_conf",
)
PUMP_CYCLE_CANDIDATE_TRACKS = (
    "short_super_250_shadow",
    "short_clean_p100_l3_shadow",
    "long_veto_core_midpremium",
    "long_funding_first_m10",
)
PUMP_ACTIVE_WINDOW_LATEST_FILE = "pump_active_window_latest.json"
PUMP_ACTIVE_WINDOW_CSV_FILE = "pump_active_window_latest.csv"
PUMP_ACTIVE_WINDOW_SAMPLES_FILE = "pump_active_window_samples.jsonl"
PUMP_ACTIVE_WINDOW_ERRORS_FILE = "pump_active_window_errors.jsonl"
PUMP_ACTIVE_WINDOW_INTERVAL = "5"
PUMP_ACTIVE_WINDOW_PRE_HOURS = 6
PUMP_ACTIVE_WINDOW_LOOKBACK_HOURS = 24
PUMP_ACTIVE_WINDOW_MAX_SYMBOLS = 20
PUMP_ACTIVE_WINDOW_SLOW_WATCH_MAX_SYMBOLS = 5

PUMP_STRATEGY_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "strategy_id": "main_pullback_tier",
        "name": "Main pullback-tier",
        "mode": "primary_shadow",
        "capital_usd": PUMP_STRATEGY_MONITOR_CAPITAL_USD,
        "max_active_coins": PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS,
        "funding_min_pct": -1.0,
        "oi_max_pct": 50.0,
        "long_ratio_min": 0.45,
        "long_ratio_max": 0.65,
        "notes": "Primary candidate from pullback-tier regression; changes pullback and ladder by pump strength.",
        "tiers": (
            {"min_pump_pct": 0.0, "entry": "pb25", "rule_slug": "step50_legs5_equal_tp25_720"},
            {"min_pump_pct": 80.0, "entry": "pb20", "rule_slug": "step50_legs2_tapered_tp25_720"},
            {"min_pump_pct": 100.0, "entry": "pb20", "rule_slug": "step50_legs3_tapered_tp25_336"},
            {"min_pump_pct": 250.0, "entry": "pb20", "rule_slug": "step50_legs2_tapered_tp25_720"},
        ),
    },
    {
        "strategy_id": "conservative_control",
        "name": "Conservative control",
        "mode": "live_control_shadow",
        "capital_usd": PUMP_STRATEGY_MONITOR_CAPITAL_USD,
        "max_active_coins": PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS,
        "funding_min_pct": -0.5,
        "oi_max_pct": 50.0,
        "long_ratio_min": 0.45,
        "long_ratio_max": 0.65,
        "notes": "Closer to current live-default; super pumps switch to a longer tapered ladder.",
        "tiers": (
            {"min_pump_pct": 0.0, "entry": "pb20", "rule_slug": "step50_legs4_equal_tp25_168"},
            {"min_pump_pct": 100.0, "entry": "pb20", "rule_slug": "step50_legs3_tapered_tp25_336"},
        ),
    },
    {
        "strategy_id": "super_pump_shadow",
        "name": "Super-pump shadow",
        "mode": "research_shadow",
        "capital_usd": PUMP_STRATEGY_MONITOR_CAPITAL_USD,
        "max_active_coins": PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS,
        "funding_min_pct": -1.0,
        "oi_max_pct": 50.0,
        "long_ratio_min": 0.45,
        "long_ratio_max": 0.65,
        "notes": "More explicit split for >100% and >250% pumps; keep in shadow until live logs confirm it.",
        "tiers": (
            {"min_pump_pct": 0.0, "entry": "pb20", "rule_slug": "step50_legs4_equal_tp25_168"},
            {"min_pump_pct": 100.0, "entry": "pb20", "rule_slug": "step50_legs3_tapered_tp25_336"},
            {"min_pump_pct": 250.0, "entry": "pb20", "rule_slug": "step50_legs2_tapered_tp25_720"},
        ),
    },
    {
        "strategy_id": "pb20_baseline",
        "name": "PB20 baseline",
        "mode": "comparison_shadow",
        "capital_usd": PUMP_STRATEGY_MONITOR_CAPITAL_USD,
        "max_active_coins": PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS,
        "funding_min_pct": -0.5,
        "oi_max_pct": 50.0,
        "long_ratio_min": 0.45,
        "long_ratio_max": 0.65,
        "notes": "Reference line for the old/simple pullback behavior.",
        "tiers": (
            {"min_pump_pct": 0.0, "entry": "pb20", "rule_slug": "step50_legs4_equal_tp25_168"},
        ),
    },
    {
        "strategy_id": "pb25_deeper_pullback",
        "name": "PB25 deeper pullback",
        "mode": "comparison_shadow",
        "capital_usd": PUMP_STRATEGY_MONITOR_CAPITAL_USD,
        "max_active_coins": PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS,
        "funding_min_pct": -1.0,
        "oi_max_pct": 50.0,
        "long_ratio_min": 0.45,
        "long_ratio_max": 0.65,
        "notes": "Checks whether deeper entry avoids early shorts on ordinary pumps.",
        "tiers": (
            {"min_pump_pct": 0.0, "entry": "pb25", "rule_slug": "step50_legs5_equal_tp25_720"},
        ),
    },
)


@dataclass(slots=True)
class BybitPumpShortRunConfig:
    output_dir: Path = DEFAULT_OUTPUT_DIR
    lookback_days: int = 30
    sleep_sec: float = 0.8
    max_symbols: int | None = None
    symbols: list[str] = field(default_factory=list)
    newest_first: bool = True
    resume: bool = True


@dataclass(slots=True)
class BybitPumpShortShadowConfig:
    output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR
    lookback_days: int = 14
    sleep_sec: float = 0.8
    max_symbols: int | None = 50
    symbols: list[str] = field(default_factory=list)
    newest_first: bool = True
    recent_event_hours: int = 168


@dataclass(slots=True)
class BybitPumpShortShadowScheduleConfig(BybitPumpShortShadowConfig):
    interval_sec: int = 3600
    run_immediately: bool = True
    max_runs: int | None = None


class BybitPumpShortLab:
    def __init__(
        self,
        *,
        restore_shadow_schedule: bool = True,
        pump_live_controller: PumpLiveController | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._shadow_thread: threading.Thread | None = None
        self._shadow_schedule_thread: threading.Thread | None = None
        self._stop_requested = threading.Event()
        self._shadow_schedule_stop_requested = threading.Event()
        self._state: dict[str, Any] = self._initial_state()
        self._shadow_state: dict[str, Any] = self._initial_shadow_state()
        self._shadow_schedule_state: dict[str, Any] = self._initial_shadow_schedule_state()
        self._strategy_monitor_last_audit_key: str | None = None
        self._pump_live = pump_live_controller or PumpLiveController()
        if restore_shadow_schedule:
            self._restore_shadow_schedule_if_enabled()

    def start(self, config: BybitPumpShortRunConfig) -> dict[str, Any]:
        with self._lock:
            if self._thread and self._thread.is_alive():
                raise RuntimeError("bybit_pump_short_run_already_active")
            self._stop_requested.clear()
            self._state = self._initial_state()
            self._state.update(
                {
                    "status": "starting",
                    "started_at_ms": now_ms(),
                    "config": self._config_payload(config),
                }
            )
            self._thread = threading.Thread(
                target=self._run_worker,
                args=(config,),
                name="bybit-pump-short-lab",
                daemon=True,
            )
            self._thread.start()
        return self.status()

    def stop(self) -> dict[str, Any]:
        self._stop_requested.set()
        with self._lock:
            if self._state.get("status") in {"running", "starting"}:
                self._state["stop_requested"] = True
        return self.status()

    def start_shadow_scan(self, config: BybitPumpShortShadowConfig) -> dict[str, Any]:
        with self._lock:
            if self._shadow_thread and self._shadow_thread.is_alive():
                raise RuntimeError("bybit_pump_short_shadow_scan_already_active")
            if self._shadow_schedule_thread and self._shadow_schedule_thread.is_alive():
                raise RuntimeError("bybit_pump_short_shadow_schedule_already_active")
            self._shadow_state = self._initial_shadow_state()
            self._shadow_state.update(
                {
                    "status": "starting",
                    "started_at_ms": now_ms(),
                    "config": self._shadow_config_payload(config),
                }
            )
            self._shadow_thread = threading.Thread(
                target=self._run_shadow_worker,
                args=(config,),
                name="bybit-pump-short-shadow",
                daemon=True,
            )
            self._shadow_thread.start()
        return self.shadow_status()

    def start_shadow_schedule(self, config: BybitPumpShortShadowScheduleConfig) -> dict[str, Any]:
        with self._lock:
            if self._shadow_schedule_thread and self._shadow_schedule_thread.is_alive():
                raise RuntimeError("bybit_pump_short_shadow_schedule_already_active")
            if self._shadow_thread and self._shadow_thread.is_alive():
                raise RuntimeError("bybit_pump_short_shadow_scan_already_active")
            self._shadow_schedule_stop_requested.clear()
            self._shadow_schedule_state = self._initial_shadow_schedule_state()
            self._shadow_schedule_state.update(
                {
                    "enabled": True,
                    "status": "starting",
                    "started_at_ms": now_ms(),
                    "updated_at_ms": now_ms(),
                    "config": self._shadow_schedule_config_payload(config),
                    "last_event": "shadow schedule starting",
                }
            )
            self._persist_shadow_schedule_state_locked()
            self._shadow_schedule_thread = threading.Thread(
                target=self._run_shadow_schedule_worker,
                args=(config,),
                name="bybit-pump-short-shadow-schedule",
                daemon=True,
            )
            self._shadow_schedule_thread.start()
        return self.shadow_schedule_status()

    def stop_shadow_schedule(self) -> dict[str, Any]:
        self._shadow_schedule_stop_requested.set()
        with self._lock:
            if self._shadow_schedule_state.get("status") in {"running", "starting", "waiting"}:
                self._shadow_schedule_state.update(
                    {
                        "enabled": False,
                        "status": "stopping",
                        "updated_at_ms": now_ms(),
                        "last_event": "stop requested",
                    }
                )
                self._persist_shadow_schedule_state_locked()
        return self.shadow_schedule_status()

    def shadow_schedule_status(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._shadow_schedule_state, ensure_ascii=True))

    def shadow_status(self) -> dict[str, Any]:
        with self._lock:
            payload = json.loads(json.dumps(self._shadow_state, ensure_ascii=True))
        output_dir = Path(payload.get("config", {}).get("output_dir") or DEFAULT_SHADOW_OUTPUT_DIR)
        payload["latest_rows"] = read_first_csv_rows(output_dir / "shadow_scan_latest.csv", limit=50)
        payload["slow_pump_watch_rows"] = read_first_csv_rows(
            output_dir / SLOW_PUMP_WATCH_LATEST_FILE,
            limit=100,
        )
        payload["metadata"] = read_json_file(output_dir / "shadow_metadata.json")
        payload["paper"] = read_paper_summary(state_path=output_dir / "paper_positions.json", limit=50)
        payload["strategy_paper"] = read_strategy_paper_summary(output_dir=output_dir, limit=200)
        payload["cycle_paper"] = read_cycle_paper_summary(output_dir=output_dir, limit=200)
        payload["active_window"] = read_active_window_summary(output_dir=output_dir)
        payload["latest_errors"] = read_latest_jsonl(output_dir / "shadow_errors.jsonl", limit=20)
        payload["files"] = output_files_payload_for_names(
            output_dir,
            (
                "shadow_metadata.json",
                "shadow_scan_latest.csv",
                "shadow_scan_history.jsonl",
                SLOW_PUMP_WATCH_LATEST_FILE,
                SLOW_PUMP_WATCH_HISTORY_FILE,
                "shadow_errors.jsonl",
                "paper_positions.json",
                "paper_positions_latest.csv",
                "paper_events.jsonl",
                PUMP_STRATEGY_MONITOR_AUDIT_FILE,
                PUMP_STRATEGY_PAPER_STATE_FILE,
                PUMP_STRATEGY_PAPER_CSV_FILE,
                PUMP_STRATEGY_PAPER_EVENTS_FILE,
                PUMP_SHADOW_SCHEDULE_STATE_FILE,
                PUMP_CYCLE_PAPER_STATE_FILE,
                PUMP_CYCLE_PAPER_CSV_FILE,
                PUMP_CYCLE_PAPER_EVENTS_FILE,
                PUMP_CYCLE_CANDIDATE_PAPER_STATE_FILE,
                PUMP_CYCLE_CANDIDATE_PAPER_CSV_FILE,
                PUMP_CYCLE_CANDIDATE_PAPER_EVENTS_FILE,
                PUMP_ACTIVE_WINDOW_LATEST_FILE,
                PUMP_ACTIVE_WINDOW_CSV_FILE,
                PUMP_ACTIVE_WINDOW_SAMPLES_FILE,
                PUMP_ACTIVE_WINDOW_ERRORS_FILE,
            ),
        )
        return payload

    def status(self) -> dict[str, Any]:
        with self._lock:
            payload = json.loads(json.dumps(self._state, ensure_ascii=True))
        output_dir = Path(payload.get("config", {}).get("output_dir") or DEFAULT_OUTPUT_DIR)
        payload["latest_summaries"] = read_latest_csv_rows(output_dir / "symbol_summary.csv", limit=30)
        payload["latest_errors"] = read_latest_jsonl(output_dir / "errors.jsonl", limit=20)
        payload["files"] = output_files_payload(output_dir)
        payload["analysis"] = read_analysis_report(DEFAULT_ANALYSIS_OUTPUT_DIR)
        payload["shadow"] = self.shadow_status()
        payload["shadow_schedule"] = self.shadow_schedule_status()
        return payload

    def pump_dashboard_status(self) -> dict[str, Any]:
        return build_pump_dashboard_state(
            self.shadow_status(),
            self.shadow_schedule_status(),
        )

    def strategy_monitor_status(self) -> dict[str, Any]:
        shadow = self.shadow_status()
        payload = build_pump_strategy_monitor_state(
            shadow,
            self.shadow_schedule_status(),
        )
        output_dir = Path(shadow.get("config", {}).get("output_dir") or DEFAULT_SHADOW_OUTPUT_DIR)
        self._write_strategy_monitor_audit_if_new(output_dir, payload)
        audit = payload.get("audit") if isinstance(payload.get("audit"), dict) else {}
        audit["latest"] = read_latest_jsonl(output_dir / PUMP_STRATEGY_MONITOR_AUDIT_FILE, limit=20)
        payload["pump_live"] = self._pump_live.status()
        return payload

    def pump_live_status(self) -> dict[str, Any]:
        return self._pump_live.status()

    def pump_live_preflight(self) -> dict[str, Any]:
        return self._pump_live.preflight()

    def pump_live_prepare(self, confirmation: str) -> dict[str, Any]:
        return self._pump_live.prepare_account(confirmation)

    def pump_live_arm(self, confirmation: str) -> dict[str, Any]:
        return self._pump_live.arm(confirmation)

    def pump_live_disarm(self) -> dict[str, Any]:
        return self._pump_live.disarm()

    def pump_live_emergency_close(self, confirmation: str) -> dict[str, Any]:
        return self._pump_live.emergency_close_all(confirmation)

    def _restore_shadow_schedule_if_enabled(self) -> None:
        state = load_shadow_schedule_state(DEFAULT_SHADOW_OUTPUT_DIR / PUMP_SHADOW_SCHEDULE_STATE_FILE)
        if not state.get("enabled"):
            return
        config_payload = state.get("config") if isinstance(state.get("config"), dict) else {}
        config = normalize_shadow_schedule_config(
            output_dir=config_payload.get("output_dir"),
            lookback_days=config_payload.get("lookback_days"),
            sleep_sec=config_payload.get("sleep_sec"),
            max_symbols=config_payload.get("max_symbols"),
            symbols=config_payload.get("symbols") or [],
            newest_first=config_payload.get("newest_first"),
            recent_event_hours=config_payload.get("recent_event_hours"),
            interval_sec=config_payload.get("interval_sec"),
            run_immediately=False,
            max_runs=config_payload.get("max_runs"),
        )
        self._shadow_schedule_stop_requested.clear()
        self._shadow_schedule_state = self._initial_shadow_schedule_state()
        self._shadow_schedule_state.update(
            {
                **state,
                "enabled": True,
                "status": "starting",
                "updated_at_ms": now_ms(),
                "finished_at_ms": None,
                "next_run_at_ms": None,
                "config": self._shadow_schedule_config_payload(config),
                "last_event": "shadow schedule restored after restart",
                "last_error": None,
            }
        )
        self._persist_shadow_schedule_state_locked()
        self._shadow_schedule_thread = threading.Thread(
            target=self._run_shadow_schedule_worker,
            args=(config,),
            name="bybit-pump-short-shadow-schedule",
            daemon=True,
        )
        self._shadow_schedule_thread.start()

    def _persist_shadow_schedule_state_locked(self) -> None:
        state = json.loads(json.dumps(self._shadow_schedule_state, ensure_ascii=True))
        output_dir = Path((state.get("config") or {}).get("output_dir") or DEFAULT_SHADOW_OUTPUT_DIR)
        save_shadow_schedule_state(output_dir / PUMP_SHADOW_SCHEDULE_STATE_FILE, state)

    def _write_strategy_monitor_audit_if_new(self, output_dir: Path, payload: dict[str, Any]) -> None:
        audit = payload.get("audit") if isinstance(payload.get("audit"), dict) else {}
        audit_key = str(audit.get("key") or "")
        if not audit_key:
            return
        with self._lock:
            if self._strategy_monitor_last_audit_key == audit_key:
                return
            self._strategy_monitor_last_audit_key = audit_key
        append_jsonl_file(
            output_dir / PUMP_STRATEGY_MONITOR_AUDIT_FILE,
            {
                "event": "strategy_monitor_snapshot",
                "ts_ms": now_ms(),
                "audit_key": audit_key,
                "shadow": payload.get("shadow"),
                "schedule": payload.get("schedule"),
                "strategies": payload.get("strategies"),
                "legacy_paper": payload.get("legacy_paper"),
            },
        )

    def _run_worker(self, run_config: BybitPumpShortRunConfig) -> None:
        collector = BybitPumpShortCollector(
            BybitCollectorConfig(
                output_dir=run_config.output_dir,
                sleep_sec=run_config.sleep_sec,
                lookback_days=run_config.lookback_days,
                stop_on_403=True,
            )
        )
        try:
            instruments = collector.load_instruments()
            instruments = self._select_instruments(instruments, run_config)
            done = collector._read_done_symbols() if run_config.resume else set()  # pylint: disable=protected-access
            with self._lock:
                self._state.update(
                    {
                        "status": "running",
                        "total_symbols": len(instruments),
                        "requests_made": collector.stats.requests_made,
                    }
                )
            for index, instrument in enumerate(instruments, start=1):
                if self._stop_requested.is_set():
                    with self._lock:
                        self._state.update(
                            {
                                "status": "stopped",
                                "finished_at_ms": now_ms(),
                                "current_index": index - 1,
                                "stop_requested": True,
                            }
                        )
                    return
                if run_config.resume and instrument.symbol in done:
                    collector.stats.symbols_skipped += 1
                    self._update_progress(
                        collector,
                        status="running",
                        current_symbol=instrument.symbol,
                        current_index=index,
                        last_event=f"skipped {instrument.symbol}",
                    )
                    continue
                self._update_progress(
                    collector,
                    status="running",
                    current_symbol=instrument.symbol,
                    current_index=index,
                    last_event=f"collecting {instrument.symbol}",
                )
                try:
                    sample = collector.collect_symbol(instrument)
                    collector._append_jsonl(  # pylint: disable=protected-access
                        run_config.output_dir / "symbol_samples.jsonl",
                        sample,
                    )
                    collector._append_summary(sample["summary"])  # pylint: disable=protected-access
                    collector._append_done_symbol(instrument.symbol)  # pylint: disable=protected-access
                    collector.stats.symbols_collected += 1
                    self._update_progress(
                        collector,
                        status="running",
                        current_symbol=instrument.symbol,
                        current_index=index,
                        last_event=f"collected {instrument.symbol}",
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    collector.stats.symbols_failed += 1
                    collector._append_jsonl(  # pylint: disable=protected-access
                        run_config.output_dir / "errors.jsonl",
                        {
                            "ts_ms": now_ms(),
                            "symbol": instrument.symbol,
                            "error": str(exc),
                        },
                    )
                    self._update_progress(
                        collector,
                        status="running",
                        current_symbol=instrument.symbol,
                        current_index=index,
                        last_error=str(exc),
                        last_event=f"failed {instrument.symbol}",
                    )
            self._update_progress(
                collector,
                status="complete",
                current_symbol=None,
                current_index=len(instruments),
                last_event="collection complete",
                finished_at_ms=now_ms(),
            )
        except Exception as exc:  # pylint: disable=broad-except
            with self._lock:
                self._state.update(
                    {
                        "status": "error",
                        "last_error": str(exc),
                        "finished_at_ms": now_ms(),
                        "requests_made": collector.stats.requests_made,
                    }
                )

    def _run_shadow_worker(self, run_config: BybitPumpShortShadowConfig) -> None:
        with self._lock:
            self._shadow_state.update({"status": "running", "updated_at_ms": now_ms()})
        try:
            metadata = self._execute_shadow_scan(run_config)
            with self._lock:
                self._shadow_state.update(
                    {
                        "status": "complete",
                        "updated_at_ms": now_ms(),
                        "finished_at_ms": now_ms(),
                        "metadata": metadata,
                        "last_event": "shadow scan complete",
                    }
                )
        except Exception as exc:  # pylint: disable=broad-except
            with self._lock:
                self._shadow_state.update(
                    {
                        "status": "error",
                        "updated_at_ms": now_ms(),
                        "finished_at_ms": now_ms(),
                        "last_error": str(exc),
                        "last_event": "shadow scan failed",
                    }
                )

    def _run_shadow_schedule_worker(self, run_config: BybitPumpShortShadowScheduleConfig) -> None:
        with self._lock:
            self._shadow_schedule_state.update(
                {
                    "enabled": True,
                    "status": "running",
                    "updated_at_ms": now_ms(),
                    "last_event": "shadow schedule running",
                }
            )
            self._persist_shadow_schedule_state_locked()
        try:
            if not run_config.run_immediately and self._wait_for_next_shadow_schedule_run(run_config.interval_sec):
                return
            while not self._shadow_schedule_stop_requested.is_set():
                self._run_shadow_schedule_once(run_config)
                with self._lock:
                    runs_started = int(self._shadow_schedule_state.get("runs_started") or 0)
                if run_config.max_runs is not None and runs_started >= run_config.max_runs:
                    with self._lock:
                        self._shadow_schedule_state.update(
                            {
                                "enabled": False,
                                "status": "complete",
                                "updated_at_ms": now_ms(),
                                "finished_at_ms": now_ms(),
                                "next_run_at_ms": None,
                                "last_event": "shadow schedule complete",
                            }
                        )
                        self._persist_shadow_schedule_state_locked()
                    return
                if self._wait_for_next_shadow_schedule_run(run_config.interval_sec):
                    return
        except Exception as exc:  # pylint: disable=broad-except
            with self._lock:
                self._shadow_schedule_state.update(
                    {
                        "status": "error",
                        "updated_at_ms": now_ms(),
                        "finished_at_ms": now_ms(),
                        "last_error": str(exc),
                        "last_event": "shadow schedule failed",
                    }
                )
                self._persist_shadow_schedule_state_locked()

    def _run_shadow_schedule_once(self, run_config: BybitPumpShortShadowScheduleConfig) -> None:
        run_started_at = now_ms()
        with self._lock:
            self._shadow_state = self._initial_shadow_state()
            self._shadow_state.update(
                {
                    "status": "running",
                    "started_at_ms": run_started_at,
                    "updated_at_ms": run_started_at,
                    "config": self._shadow_config_payload(run_config),
                    "last_event": "scheduled shadow scan running",
                }
            )
            self._shadow_schedule_state.update(
                {
                    "enabled": True,
                    "status": "running",
                    "updated_at_ms": run_started_at,
                    "last_run_started_at_ms": run_started_at,
                    "next_run_at_ms": None,
                    "runs_started": int(self._shadow_schedule_state.get("runs_started") or 0) + 1,
                    "last_event": "scheduled shadow scan running",
                }
            )
            self._persist_shadow_schedule_state_locked()
        try:
            metadata = self._execute_shadow_scan(run_config)
        except Exception as exc:  # pylint: disable=broad-except
            finished_at = now_ms()
            with self._lock:
                self._shadow_state.update(
                    {
                        "status": "error",
                        "updated_at_ms": finished_at,
                        "finished_at_ms": finished_at,
                        "last_error": str(exc),
                        "last_event": "scheduled shadow scan failed",
                    }
                )
                self._shadow_schedule_state.update(
                    {
                        "updated_at_ms": finished_at,
                        "last_run_finished_at_ms": finished_at,
                        "runs_failed": int(self._shadow_schedule_state.get("runs_failed") or 0) + 1,
                        "last_error": str(exc),
                        "last_event": "scheduled shadow scan failed",
                    }
                )
                self._persist_shadow_schedule_state_locked()
            return
        finished_at = now_ms()
        with self._lock:
            self._shadow_state.update(
                {
                    "status": "complete",
                    "updated_at_ms": finished_at,
                    "finished_at_ms": finished_at,
                    "metadata": metadata,
                    "last_event": "scheduled shadow scan complete",
                    "last_error": None,
                }
            )
            self._shadow_schedule_state.update(
                {
                    "updated_at_ms": finished_at,
                    "last_run_finished_at_ms": finished_at,
                    "runs_completed": int(self._shadow_schedule_state.get("runs_completed") or 0) + 1,
                    "last_metadata": metadata,
                    "last_event": "scheduled shadow scan complete",
                    "last_error": None,
                }
            )
            self._persist_shadow_schedule_state_locked()

    def _wait_for_next_shadow_schedule_run(self, interval_sec: int) -> bool:
        next_run_at = now_ms() + int(interval_sec * 1000)
        with self._lock:
            self._shadow_schedule_state.update(
                {
                    "enabled": True,
                    "status": "waiting",
                    "updated_at_ms": now_ms(),
                    "next_run_at_ms": next_run_at,
                    "last_event": "waiting for next shadow scan",
                }
            )
            self._persist_shadow_schedule_state_locked()
        if self._shadow_schedule_stop_requested.wait(max(1, int(interval_sec))):
            with self._lock:
                self._shadow_schedule_state.update(
                    {
                        "enabled": False,
                        "status": "stopped",
                        "updated_at_ms": now_ms(),
                        "finished_at_ms": now_ms(),
                        "next_run_at_ms": None,
                        "last_event": "shadow schedule stopped",
                    }
                )
                self._persist_shadow_schedule_state_locked()
            return True
        return False

    def _execute_shadow_scan(self, run_config: BybitPumpShortShadowConfig) -> dict[str, Any]:
        metadata = run_shadow_scan(
            ShadowScanConfig(
                output_dir=run_config.output_dir,
                lookback_days=run_config.lookback_days,
                sleep_sec=run_config.sleep_sec,
                max_symbols=run_config.max_symbols,
                symbols=run_config.symbols,
                newest_first=run_config.newest_first,
                recent_event_hours=run_config.recent_event_hours,
            )
        )
        rows = read_first_csv_rows(run_config.output_dir / "shadow_scan_latest.csv", limit=10_000)
        strategy_paper = apply_pump_strategy_paper_rows(rows, output_dir=run_config.output_dir)
        main_strategy = next(
            (
                strategy
                for strategy in PUMP_STRATEGY_CATALOG
                if strategy.get("strategy_id") == "main_pullback_tier"
            ),
            None,
        )
        live_signal_result = (
            self._pump_live.submit_decisions(
                [classify_strategy_signal(main_strategy, row) for row in rows]
            )
            if main_strategy
            else {"accepted": 0, "armed": False}
        )
        cycle_paper = apply_pump_cycle_paper_rows(rows, output_dir=run_config.output_dir)
        active_window = apply_pump_active_window_scan(
            rows,
            output_dir=run_config.output_dir,
            sleep_sec=min(float(run_config.sleep_sec or 0.0), 0.2),
        )
        metadata["strategy_paper_positions"] = strategy_paper.get("positions")
        metadata["strategy_paper_open_positions"] = strategy_paper.get("open_positions")
        metadata["strategy_paper_closed_positions"] = strategy_paper.get("closed_positions")
        metadata["strategy_paper_events"] = strategy_paper.get("events")
        metadata["strategy_paper_current_topup_usd"] = strategy_paper.get("current_topup_usd")
        metadata["strategy_paper_peak_topup_usd"] = strategy_paper.get("peak_topup_usd")
        metadata["pump_live_signals_accepted"] = live_signal_result.get("accepted")
        metadata["pump_live_armed"] = live_signal_result.get("armed")
        metadata["cycle_paper_positions"] = cycle_paper.get("positions")
        metadata["cycle_paper_open_positions"] = cycle_paper.get("open_positions")
        metadata["cycle_paper_closed_positions"] = cycle_paper.get("closed_positions")
        metadata["cycle_paper_events"] = cycle_paper.get("events")
        metadata["cycle_paper_equity_mark_usd"] = cycle_paper.get("equity_mark_usd")
        metadata["cycle_paper_current_topup_usd"] = cycle_paper.get("current_topup_usd")
        metadata["cycle_paper_peak_topup_usd"] = cycle_paper.get("peak_topup_usd")
        candidate_paper = cycle_paper.get("candidate_paper") if isinstance(cycle_paper.get("candidate_paper"), dict) else {}
        candidate_summary = candidate_paper.get("summary") if isinstance(candidate_paper.get("summary"), dict) else {}
        metadata["cycle_candidate_paper_positions"] = candidate_summary.get("positions")
        metadata["cycle_candidate_paper_open_positions"] = candidate_summary.get("open_positions")
        metadata["cycle_candidate_paper_closed_positions"] = candidate_summary.get("closed_positions")
        metadata["cycle_candidate_paper_pnl_usd"] = candidate_summary.get("combined_pnl_usd")
        metadata["active_window_symbols"] = active_window.get("symbols")
        metadata["active_window_errors"] = active_window.get("errors")
        metadata["active_window_requests_made"] = active_window.get("requests_made")
        (run_config.output_dir / "shadow_metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=True, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return metadata

    def _select_instruments(
        self,
        instruments: list[BybitInstrument],
        config: BybitPumpShortRunConfig,
    ) -> list[BybitInstrument]:
        requested = {normalize_symbol(item) for item in config.symbols if normalize_symbol(item)}
        if requested:
            instruments = [item for item in instruments if item.symbol in requested]
        else:
            instruments = [item for item in instruments if is_crypto_pump_short_instrument(item)]
        instruments = sorted(
            instruments,
            key=lambda item: item.launch_time_ms or 0,
            reverse=config.newest_first,
        )
        instruments = dedupe_instruments(instruments)
        if config.max_symbols is not None:
            instruments = instruments[: max(0, config.max_symbols)]
        return instruments

    def _update_progress(
        self,
        collector: BybitPumpShortCollector,
        *,
        status: str,
        current_symbol: str | None,
        current_index: int,
        last_event: str,
        last_error: str | None = None,
        finished_at_ms: int | None = None,
    ) -> None:
        with self._lock:
            self._state.update(
                {
                    "status": status,
                    "updated_at_ms": now_ms(),
                    "current_symbol": current_symbol,
                    "current_index": current_index,
                    "collected": collector.stats.symbols_collected,
                    "skipped": collector.stats.symbols_skipped,
                    "failed": collector.stats.symbols_failed,
                    "requests_made": collector.stats.requests_made,
                    "last_event": last_event,
                }
            )
            if last_error:
                self._state["last_error"] = last_error
            if finished_at_ms:
                self._state["finished_at_ms"] = finished_at_ms

    @staticmethod
    def _config_payload(config: BybitPumpShortRunConfig) -> dict[str, Any]:
        payload = asdict(config)
        payload["output_dir"] = str(config.output_dir)
        return payload

    @staticmethod
    def _shadow_config_payload(config: BybitPumpShortShadowConfig) -> dict[str, Any]:
        payload = asdict(config)
        payload["output_dir"] = str(config.output_dir)
        payload.pop("interval_sec", None)
        payload.pop("run_immediately", None)
        payload.pop("max_runs", None)
        return payload

    @staticmethod
    def _shadow_schedule_config_payload(config: BybitPumpShortShadowScheduleConfig) -> dict[str, Any]:
        payload = asdict(config)
        payload["output_dir"] = str(config.output_dir)
        return payload

    @staticmethod
    def _initial_state() -> dict[str, Any]:
        return {
            "status": "idle",
            "started_at_ms": None,
            "updated_at_ms": None,
            "finished_at_ms": None,
            "total_symbols": 0,
            "current_index": 0,
            "current_symbol": None,
            "collected": 0,
            "skipped": 0,
            "failed": 0,
            "requests_made": 0,
            "stop_requested": False,
            "last_event": None,
            "last_error": None,
            "config": {
                "output_dir": str(DEFAULT_OUTPUT_DIR),
                "lookback_days": 30,
                "sleep_sec": 0.8,
                "max_symbols": None,
                "symbols": [],
                "newest_first": True,
                "resume": True,
            },
        }

    @staticmethod
    def _initial_shadow_state() -> dict[str, Any]:
        return {
            "status": "idle",
            "started_at_ms": None,
            "updated_at_ms": None,
            "finished_at_ms": None,
            "last_event": None,
            "last_error": None,
            "metadata": {},
            "config": {
                "output_dir": str(DEFAULT_SHADOW_OUTPUT_DIR),
                "lookback_days": 14,
                "sleep_sec": 0.8,
                "max_symbols": 50,
                "symbols": [],
                "newest_first": True,
                "recent_event_hours": 168,
            },
        }

    @staticmethod
    def _initial_shadow_schedule_state() -> dict[str, Any]:
        return {
            "enabled": False,
            "status": "idle",
            "started_at_ms": None,
            "updated_at_ms": None,
            "finished_at_ms": None,
            "last_run_started_at_ms": None,
            "last_run_finished_at_ms": None,
            "next_run_at_ms": None,
            "runs_started": 0,
            "runs_completed": 0,
            "runs_failed": 0,
            "last_event": None,
            "last_error": None,
            "last_metadata": {},
            "config": {
                "output_dir": str(DEFAULT_SHADOW_OUTPUT_DIR),
                "lookback_days": 14,
                "sleep_sec": 0.8,
                "max_symbols": 50,
                "symbols": [],
                "newest_first": True,
                "recent_event_hours": 168,
                "interval_sec": 3600,
                "run_immediately": True,
                "max_runs": None,
            },
        }


def read_latest_csv_rows(path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return []
    return rows[-limit:]


def read_latest_jsonl(path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    out: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            out.append(payload)
    return out


def output_files_payload(output_dir: Path) -> dict[str, Any]:
    files: dict[str, Any] = {}
    for name in (
        "instruments_latest.json",
        "symbol_samples.jsonl",
        "symbol_summary.csv",
        "done_symbols.txt",
        "errors.jsonl",
    ):
        path = output_dir / name
        if not path.exists():
            files[name] = {"exists": False, "size": 0, "updated_at": None}
            continue
        stat = path.stat()
        files[name] = {
            "exists": True,
            "size": stat.st_size,
            "updated_at": int(stat.st_mtime * 1000),
        }
    return files


def read_analysis_report(output_dir: Path = DEFAULT_ANALYSIS_OUTPUT_DIR) -> dict[str, Any]:
    metadata = read_json_file(output_dir / "analysis_metadata.json")
    return {
        "metadata": metadata,
        "candidate_profiles": read_first_csv_rows(output_dir / "candidate_rule_profiles.csv", limit=30),
        "anti_overfit": read_first_csv_rows(output_dir / "anti_overfit_report.csv", limit=20),
        "best_rules": read_first_csv_rows(output_dir / "best_rules.csv", limit=12),
        "worst_tail_events": read_first_csv_rows(output_dir / "worst_tail_events.csv", limit=12),
        "funding_regime_summary": read_first_csv_rows(output_dir / "funding_regime_summary.csv", limit=12),
        "oi_regime_summary": read_first_csv_rows(output_dir / "oi_regime_summary.csv", limit=12),
        "behavior_regime_recommendations": read_first_csv_rows(output_dir / "behavior_regime_recommendations.csv", limit=20),
        "strategy_recommendations": read_json_file(output_dir / "strategy_recommendations.json"),
        "episodes": read_latest_csv_rows(output_dir / "pump_episodes.csv", limit=12),
        "files": output_files_payload_for_names(
            output_dir,
            (
                "analysis_metadata.json",
                "pump_episodes.csv",
                "entry_rule_summary.csv",
                "exit_rule_summary.csv",
                "candidate_rule_profiles.csv",
                "robustness_time_split.csv",
                "robustness_symbol_holdout.csv",
                "symbol_concentration.csv",
                "anti_overfit_report.csv",
                "best_rules.csv",
                "worst_tail_events.csv",
                "funding_regime_summary.csv",
                "oi_regime_summary.csv",
                "behavior_regime_summary.csv",
                "behavior_regime_recommendations.csv",
                "strategy_recommendations.json",
            ),
        ),
    }


def read_first_csv_rows(path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            out: list[dict[str, Any]] = []
            for row in csv.DictReader(handle):
                out.append(row)
                if len(out) >= limit:
                    break
            return out
    except OSError:
        return []


def read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def load_shadow_schedule_state(path: Path) -> dict[str, Any]:
    payload = read_json_file(path)
    if not payload:
        return {"enabled": False}
    payload.setdefault("enabled", False)
    payload.setdefault("config", {})
    return payload


def save_shadow_schedule_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")


def output_files_payload_for_names(output_dir: Path, names: Iterable[str]) -> dict[str, Any]:
    files: dict[str, Any] = {}
    for name in names:
        path = output_dir / name
        if not path.exists():
            files[name] = {"exists": False, "size": 0, "updated_at": None}
            continue
        stat = path.stat()
        files[name] = {
            "exists": True,
            "size": stat.st_size,
            "updated_at": int(stat.st_mtime * 1000),
        }
    return files


def apply_pump_strategy_paper_rows(
    rows: list[dict[str, Any]],
    *,
    output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR,
    catalog: Iterable[dict[str, Any]] = PUMP_STRATEGY_CATALOG,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / PUMP_STRATEGY_PAPER_STATE_FILE
    events_path = output_dir / PUMP_STRATEGY_PAPER_EVENTS_FILE
    csv_path = output_dir / PUMP_STRATEGY_PAPER_CSV_FILE
    state = load_strategy_paper_state(state_path)
    positions = list(state.get("positions") or [])
    strategies = list(catalog)
    rows_by_symbol = {str(row.get("symbol") or ""): row for row in rows if row.get("symbol")}
    scan_ts = max((to_int(row.get("ts_ms")) or 0 for row in rows), default=now_ms()) or now_ms()
    events: list[dict[str, Any]] = []

    for position in positions:
        if position.get("status") != "open":
            continue
        row = rows_by_symbol.get(str(position.get("symbol") or ""))
        if not row:
            position["last_missing_update_at_ms"] = scan_ts
            position["missing_update_count"] = int(position.get("missing_update_count") or 0) + 1
            continue
        events.extend(update_strategy_paper_position(position, row))

    for strategy in strategies:
        strategy_id = str(strategy.get("strategy_id") or "")
        active_count = sum(1 for item in positions if item.get("status") == "open" and item.get("strategy_id") == strategy_id)
        max_active = int(strategy.get("max_active_coins") or PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS)
        decisions = [classify_strategy_signal(strategy, row) for row in rows]
        decisions.sort(key=strategy_decision_sort_key)
        for decision in decisions:
            if decision.get("state") != "entry_ready":
                continue
            if active_count >= max_active:
                events.append(strategy_paper_event("paper_skip_no_slot", strategy_id=strategy_id, decision=decision, ts_ms=scan_ts))
                continue
            if strategy_open_position_exists(positions, strategy_id=strategy_id, decision=decision):
                continue
            position = open_strategy_paper_position(strategy, decision)
            positions.append(position)
            active_count += 1
            events.append({"event": "paper_open", "position": compact_strategy_paper_position(position), "ts_ms": position.get("opened_at_ms")})

    previous_summaries = state.get("strategy_summaries") if isinstance(state.get("strategy_summaries"), dict) else {}
    summaries = build_strategy_paper_summaries(positions, strategies)
    for strategy_id, summary in summaries.items():
        previous = previous_summaries.get(strategy_id) if isinstance(previous_summaries.get(strategy_id), dict) else {}
        summary["peak_topup_needed_usd"] = round(
            max(
                to_number(previous.get("peak_topup_needed_usd")),
                to_number(summary.get("peak_topup_needed_usd")),
                to_number(summary.get("current_topup_needed_usd")),
            ),
            6,
        )
    state.update(
        {
            "schema": "pump_short_strategy_paper_v1",
            "updated_at_ms": scan_ts,
            "positions": positions,
            "strategy_summaries": summaries,
        }
    )
    save_strategy_paper_state(state_path, state)
    write_strategy_paper_csv(csv_path, positions)
    for event in events:
        append_jsonl_file(events_path, event)
    return {
        "positions": len(positions),
        "open_positions": sum(1 for item in positions if item.get("status") == "open"),
        "closed_positions": sum(1 for item in positions if item.get("status") == "closed"),
        "events": len(events),
        "current_topup_usd": round(sum(to_number(item.get("current_topup_needed_usd")) for item in positions if item.get("status") == "open"), 6),
        "peak_topup_usd": round(sum(to_number(summary.get("peak_topup_needed_usd")) for summary in summaries.values()), 6),
        "strategy_summaries": summaries,
    }


def open_strategy_paper_position(strategy: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    tier = decision.get("tier") if isinstance(decision.get("tier"), dict) else {}
    ts_ms = to_int(decision.get("ts_ms")) or now_ms()
    current_price = to_number(decision.get("last_close"))
    strategy_id = str(strategy.get("strategy_id") or "")
    capital_usd = float(strategy.get("capital_usd") or PUMP_STRATEGY_MONITOR_CAPITAL_USD)
    max_active = int(strategy.get("max_active_coins") or PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS)
    leverage = float(strategy.get("leverage") or PUMP_STRATEGY_MONITOR_LEVERAGE)
    per_coin_capital = capital_usd / max(1, max_active)
    legs = build_strategy_paper_legs(
        entry_price=current_price,
        tier=tier,
        per_coin_capital=per_coin_capital,
        leverage=leverage,
        opened_at_ms=ts_ms,
    )
    paper_id = "|".join(
        [
            strategy_id,
            str(decision.get("symbol") or ""),
            str(decision.get("event_id") or ts_ms),
            str(tier.get("rule_slug") or ""),
        ]
    )
    position = {
        "paper_id": paper_id,
        "schema": "pump_short_strategy_paper_position_v1",
        "status": "open",
        "strategy_id": strategy_id,
        "strategy_name": strategy.get("name") or strategy_id,
        "symbol": decision.get("symbol"),
        "event_id": decision.get("event_id"),
        "opened_at_ms": ts_ms,
        "updated_at_ms": ts_ms,
        "closed_at_ms": None,
        "entry_price": current_price,
        "current_price": current_price,
        "avg_entry_price": current_price,
        "target_price": current_price * (1.0 - to_number(tier.get("tp_pct")) / 100.0) if current_price > 0 else None,
        "exit_reason": None,
        "capital_usd": capital_usd,
        "max_active_coins": max_active,
        "per_coin_capital_usd": per_coin_capital,
        "leverage": leverage,
        "tier": tier,
        "legs": legs,
        "remaining_weight": 1.0,
        "filled_steps": sum(1 for leg in legs if leg.get("filled")),
        "planned_steps": len(legs),
        "used_margin_usd": 0.0,
        "gross_notional_usd": 0.0,
        "current_pnl_pct": 0.0,
        "current_unrealized_pnl_usd": 0.0,
        "realized_pnl_usd": 0.0,
        "combined_pnl_usd": 0.0,
        "current_topup_needed_usd": 0.0,
        "peak_topup_needed_usd": 0.0,
        "mae_pct": 0.0,
        "mfe_pct": 0.0,
        "target_hits": [],
        "open_decision": decision,
        "last_decision": decision,
    }
    recompute_strategy_paper_position_metrics(position, current_price=current_price, now_ms_value=ts_ms)
    return position


def build_strategy_paper_legs(
    *,
    entry_price: float,
    tier: dict[str, Any],
    per_coin_capital: float,
    leverage: float,
    opened_at_ms: int,
) -> list[dict[str, Any]]:
    legs_count = max(1, int(tier.get("ladder_legs") or 1))
    step_pct = float(tier.get("ladder_step_pct") or 50.0)
    weights = [float(item) for item in (tier.get("leg_weights") or [])][:legs_count]
    if len(weights) < legs_count:
        weights.extend([1.0] * (legs_count - len(weights)))
    weight_sum = sum(weights) or 1.0
    legs: list[dict[str, Any]] = []
    for index in range(legs_count):
        trigger_price = entry_price * (1.0 + step_pct / 100.0 * index) if entry_price > 0 else 0.0
        margin = per_coin_capital * weights[index] / weight_sum
        notional = margin * leverage
        filled = index == 0
        legs.append(
            {
                "step": index + 1,
                "trigger_price": round(trigger_price, 10) if trigger_price else None,
                "entry_price": round(trigger_price, 10) if filled and trigger_price else None,
                "weight": weights[index],
                "margin_usd": round(margin, 6),
                "notional_usd": round(notional, 6),
                "filled": filled,
                "filled_at_ms": opened_at_ms if filled else None,
                "closed": False,
                "closed_at_ms": None,
                "realized_pnl_usd": 0.0,
            }
        )
    return legs


def update_strategy_paper_position(position: dict[str, Any], row: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    now_value = to_int(row.get("ts_ms")) or now_ms()
    current_price = to_number(row.get("last_close"))
    if current_price <= 0:
        return events
    position["updated_at_ms"] = now_value
    position["current_price"] = current_price
    position["last_decision"] = row
    for leg in position.get("legs") or []:
        if leg.get("filled") or leg.get("closed"):
            continue
        trigger = to_number(leg.get("trigger_price"))
        if trigger > 0 and current_price >= trigger:
            leg["filled"] = True
            leg["entry_price"] = round(trigger, 10)
            leg["filled_at_ms"] = now_value
            events.append(
                {
                    "event": "paper_add_leg",
                    "ts_ms": now_value,
                    "paper_id": position.get("paper_id"),
                    "strategy_id": position.get("strategy_id"),
                    "symbol": position.get("symbol"),
                    "step": leg.get("step"),
                    "entry_price": leg.get("entry_price"),
                    "margin_usd": leg.get("margin_usd"),
                    "notional_usd": leg.get("notional_usd"),
                }
            )
    recompute_strategy_paper_position_metrics(position, current_price=current_price, now_ms_value=now_value)
    previous_peak = to_number(position.get("peak_topup_needed_usd"))
    current_topup = to_number(position.get("current_topup_needed_usd"))
    if current_topup > previous_peak:
        position["peak_topup_needed_usd"] = round(current_topup, 6)
        events.append(
            {
                "event": "paper_topup_peak",
                "ts_ms": now_value,
                "paper_id": position.get("paper_id"),
                "strategy_id": position.get("strategy_id"),
                "symbol": position.get("symbol"),
                "topup_needed_usd": round(current_topup, 6),
            }
        )
    close_event = maybe_close_strategy_paper_position(position, current_price=current_price, now_ms_value=now_value)
    if close_event:
        events.append(close_event)
    return events


def recompute_strategy_paper_position_metrics(
    position: dict[str, Any],
    *,
    current_price: float,
    now_ms_value: int,
) -> None:
    open_legs = [leg for leg in position.get("legs") or [] if leg.get("filled") and not leg.get("closed")]
    used_margin = sum(to_number(leg.get("margin_usd")) for leg in open_legs)
    gross_notional = sum(to_number(leg.get("notional_usd")) for leg in open_legs)
    weighted_entry = sum(to_number(leg.get("entry_price")) * to_number(leg.get("notional_usd")) for leg in open_legs)
    avg_entry = weighted_entry / gross_notional if gross_notional > 0 else to_number(position.get("entry_price"))
    gross_pnl = sum(
        to_number(leg.get("notional_usd")) * short_pnl_pct(to_number(leg.get("entry_price")), current_price) / 100.0
        for leg in open_legs
    )
    fee_if_closed = gross_notional * PUMP_STRATEGY_PAPER_FEE_ROUNDTRIP_PCT / 100.0
    unrealized_net = gross_pnl - fee_if_closed
    pnl_pct = (gross_pnl / gross_notional * 100.0) if gross_notional > 0 else 0.0
    realized = to_number(position.get("realized_pnl_usd"))
    per_coin_capital = to_number(position.get("per_coin_capital_usd"))
    unrealized_loss = max(0.0, -unrealized_net)
    opened_at = to_int(position.get("opened_at_ms"))
    max_hold_h = to_int((position.get("tier") or {}).get("max_hold_h")) or 168
    time_in_trade_h = ((now_ms_value - opened_at) / 3_600_000.0) if opened_at and now_ms_value >= opened_at else 0.0
    hours_left = max(0.0, max_hold_h - time_in_trade_h)
    tp_pct = to_number((position.get("tier") or {}).get("tp_pct")) or 25.0
    target_price = avg_entry * (1.0 - tp_pct / 100.0) if avg_entry > 0 else None
    position.update(
        {
            "filled_steps": len(open_legs),
            "remaining_steps": max(0, len(position.get("legs") or []) - len(open_legs)),
            "avg_entry_price": round(avg_entry, 10) if avg_entry else None,
            "target_price": round(target_price, 10) if target_price else None,
            "used_margin_usd": round(used_margin, 6),
            "gross_notional_usd": round(gross_notional, 6),
            "current_pnl_pct": round(pnl_pct, 6),
            "current_unrealized_pnl_usd": round(unrealized_net, 6),
            "combined_pnl_usd": round(realized + unrealized_net, 6),
            "current_topup_needed_usd": round(max(0.0, unrealized_loss - per_coin_capital), 6),
            "mae_pct": round(max(to_number(position.get("mae_pct")), -pnl_pct), 6),
            "mfe_pct": round(max(to_number(position.get("mfe_pct")), pnl_pct), 6),
            "time_in_trade_h": round(time_in_trade_h, 3),
            "max_hold_h": max_hold_h,
            "hours_left_h": round(hours_left, 3),
        }
    )


def maybe_close_strategy_paper_position(
    position: dict[str, Any],
    *,
    current_price: float,
    now_ms_value: int,
) -> dict[str, Any] | None:
    if position.get("status") != "open":
        return None
    tier = position.get("tier") if isinstance(position.get("tier"), dict) else {}
    avg_entry = to_number(position.get("avg_entry_price"))
    gross_notional = to_number(position.get("gross_notional_usd"))
    if avg_entry <= 0 or gross_notional <= 0:
        return None
    tp_pct = to_number(tier.get("tp_pct")) or 25.0
    target_price = avg_entry * (1.0 - tp_pct / 100.0)
    reason = None
    fill_pnl_pct = short_pnl_pct(avg_entry, current_price)
    if current_price <= target_price:
        reason = f"target_{int(tp_pct)}"
        fill_pnl_pct = tp_pct
    else:
        opened_at = to_int(position.get("opened_at_ms")) or now_ms_value
        max_hold_h = to_int(tier.get("max_hold_h")) or 168
        if now_ms_value - opened_at >= max_hold_h * 3_600_000:
            reason = "time_stop"
    if not reason:
        return None
    net_pnl = gross_notional * (fill_pnl_pct - PUMP_STRATEGY_PAPER_FEE_ROUNDTRIP_PCT) / 100.0
    for leg in position.get("legs") or []:
        if leg.get("filled") and not leg.get("closed"):
            leg["closed"] = True
            leg["closed_at_ms"] = now_ms_value
    position.update(
        {
            "status": "closed",
            "closed_at_ms": now_ms_value,
            "exit_reason": reason,
            "realized_pnl_usd": round(to_number(position.get("realized_pnl_usd")) + net_pnl, 6),
            "current_unrealized_pnl_usd": 0.0,
            "combined_pnl_usd": round(to_number(position.get("realized_pnl_usd")) + net_pnl, 6),
            "current_topup_needed_usd": 0.0,
            "remaining_weight": 0.0,
            "target_hits": list(position.get("target_hits") or []) + [{"reason": reason, "ts_ms": now_ms_value, "net_pnl_usd": round(net_pnl, 6)}],
        }
    )
    return {
        "event": "paper_close",
        "ts_ms": now_ms_value,
        "paper_id": position.get("paper_id"),
        "strategy_id": position.get("strategy_id"),
        "symbol": position.get("symbol"),
        "reason": reason,
        "net_pnl_usd": round(net_pnl, 6),
        "gross_notional_usd": round(gross_notional, 6),
        "fill_pnl_pct": round(fill_pnl_pct, 6),
    }


def strategy_open_position_exists(
    positions: list[dict[str, Any]],
    *,
    strategy_id: str,
    decision: dict[str, Any],
) -> bool:
    symbol = str(decision.get("symbol") or "")
    event_id = str(decision.get("event_id") or "")
    rule_slug = str((decision.get("tier") or {}).get("rule_slug") or "")
    for position in positions:
        if position.get("status") != "open":
            continue
        if str(position.get("strategy_id") or "") != strategy_id:
            continue
        if str(position.get("symbol") or "") != symbol:
            continue
        if str(position.get("event_id") or "") == event_id and str((position.get("tier") or {}).get("rule_slug") or "") == rule_slug:
            return True
    return False


def build_strategy_paper_summaries(
    positions: list[dict[str, Any]],
    catalog: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for strategy in catalog:
        strategy_id = str(strategy.get("strategy_id") or "")
        capital = float(strategy.get("capital_usd") or PUMP_STRATEGY_MONITOR_CAPITAL_USD)
        items = [position for position in positions if position.get("strategy_id") == strategy_id]
        open_items = [position for position in items if position.get("status") == "open"]
        closed_items = [position for position in items if position.get("status") == "closed"]
        realized = sum(to_number(item.get("realized_pnl_usd")) for item in items)
        unrealized = sum(to_number(item.get("current_unrealized_pnl_usd")) for item in open_items)
        used_margin = sum(to_number(item.get("used_margin_usd")) for item in open_items)
        current_topup = sum(to_number(item.get("current_topup_needed_usd")) for item in open_items)
        peak_topup = max([to_number(item.get("peak_topup_needed_usd")) for item in items] + [0.0])
        max_active = int(strategy.get("max_active_coins") or PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS)
        summaries[strategy_id] = {
            "strategy_id": strategy_id,
            "initial_capital_usd": round(capital, 6),
            "equity_realized_usd": round(capital + realized, 6),
            "equity_mark_usd": round(capital + realized + unrealized, 6),
            "realized_pnl_usd": round(realized, 6),
            "unrealized_pnl_usd": round(unrealized, 6),
            "combined_pnl_usd": round(realized + unrealized, 6),
            "roi_mark_pct": round((realized + unrealized) / capital * 100.0, 6) if capital else 0.0,
            "positions": len(items),
            "open_positions": len(open_items),
            "closed_positions": len(closed_items),
            "free_slots": max(0, max_active - len(open_items)),
            "used_margin_usd": round(used_margin, 6),
            "current_topup_needed_usd": round(current_topup, 6),
            "peak_topup_needed_usd": round(peak_topup, 6),
            "needs_external_topup": current_topup > 0,
            "capital_sufficient_without_topup": current_topup <= 0,
        }
    return summaries


def read_strategy_paper_summary(
    *,
    output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR,
    limit: int = 200,
) -> dict[str, Any]:
    state = load_strategy_paper_state(output_dir / PUMP_STRATEGY_PAPER_STATE_FILE)
    positions = list(state.get("positions") or [])
    positions.sort(key=lambda item: int(float(item.get("updated_at_ms") or item.get("opened_at_ms") or 0)), reverse=True)
    return {
        "schema": state.get("schema") or "pump_short_strategy_paper_v1",
        "updated_at_ms": state.get("updated_at_ms"),
        "positions": positions[:limit],
        "open_positions": sum(1 for item in positions if item.get("status") == "open"),
        "closed_positions": sum(1 for item in positions if item.get("status") == "closed"),
        "strategy_summaries": state.get("strategy_summaries") or {},
        "events_latest": read_latest_jsonl(output_dir / PUMP_STRATEGY_PAPER_EVENTS_FILE, limit=50),
    }


def load_strategy_paper_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema": "pump_short_strategy_paper_v1", "positions": [], "updated_at_ms": None, "strategy_summaries": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema": "pump_short_strategy_paper_v1", "positions": [], "updated_at_ms": None, "strategy_summaries": {}}
    if not isinstance(payload, dict):
        return {"schema": "pump_short_strategy_paper_v1", "positions": [], "updated_at_ms": None, "strategy_summaries": {}}
    payload.setdefault("positions", [])
    payload.setdefault("strategy_summaries", {})
    return payload


def save_strategy_paper_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")


def write_strategy_paper_csv(path: Path, positions: list[dict[str, Any]]) -> None:
    rows = [flatten_strategy_paper_position(position) for position in positions]
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def flatten_strategy_paper_position(position: dict[str, Any]) -> dict[str, Any]:
    tier = position.get("tier") if isinstance(position.get("tier"), dict) else {}
    return {
        "paper_id": position.get("paper_id"),
        "strategy_id": position.get("strategy_id"),
        "symbol": position.get("symbol"),
        "status": position.get("status"),
        "event_id": position.get("event_id"),
        "opened_at_ms": position.get("opened_at_ms"),
        "updated_at_ms": position.get("updated_at_ms"),
        "closed_at_ms": position.get("closed_at_ms"),
        "entry_price": position.get("entry_price"),
        "current_price": position.get("current_price"),
        "avg_entry_price": position.get("avg_entry_price"),
        "target_price": position.get("target_price"),
        "rule_slug": tier.get("rule_slug"),
        "tp_pct": tier.get("tp_pct"),
        "max_hold_h": tier.get("max_hold_h"),
        "filled_steps": position.get("filled_steps"),
        "planned_steps": position.get("planned_steps"),
        "used_margin_usd": position.get("used_margin_usd"),
        "gross_notional_usd": position.get("gross_notional_usd"),
        "current_pnl_pct": position.get("current_pnl_pct"),
        "current_unrealized_pnl_usd": position.get("current_unrealized_pnl_usd"),
        "realized_pnl_usd": position.get("realized_pnl_usd"),
        "combined_pnl_usd": position.get("combined_pnl_usd"),
        "current_topup_needed_usd": position.get("current_topup_needed_usd"),
        "peak_topup_needed_usd": position.get("peak_topup_needed_usd"),
        "exit_reason": position.get("exit_reason"),
    }


def compact_strategy_paper_position(position: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_id": position.get("paper_id"),
        "strategy_id": position.get("strategy_id"),
        "symbol": position.get("symbol"),
        "event_id": position.get("event_id"),
        "status": position.get("status"),
        "opened_at_ms": position.get("opened_at_ms"),
        "entry_price": position.get("entry_price"),
        "tier": position.get("tier"),
        "capital_usd": position.get("capital_usd"),
    }


def strategy_paper_event(
    event: str,
    *,
    strategy_id: str,
    decision: dict[str, Any],
    ts_ms: int,
) -> dict[str, Any]:
    return {
        "event": event,
        "ts_ms": ts_ms,
        "strategy_id": strategy_id,
        "symbol": decision.get("symbol"),
        "event_id": decision.get("event_id"),
        "reason": decision.get("reason"),
        "state": decision.get("state"),
    }


def apply_pump_cycle_paper_rows(
    rows: list[dict[str, Any]],
    *,
    output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / PUMP_CYCLE_PAPER_STATE_FILE
    events_path = output_dir / PUMP_CYCLE_PAPER_EVENTS_FILE
    csv_path = output_dir / PUMP_CYCLE_PAPER_CSV_FILE
    state = load_cycle_paper_state(state_path)
    positions = list(state.get("positions") or [])
    rows_by_symbol = {str(row.get("symbol") or ""): row for row in rows if row.get("symbol")}
    scan_ts = max((to_int(row.get("ts_ms")) or 0 for row in rows), default=now_ms()) or now_ms()
    events: list[dict[str, Any]] = []

    for position in positions:
        if position.get("status") != "open":
            continue
        row = rows_by_symbol.get(str(position.get("symbol") or ""))
        if not row:
            position["last_missing_update_at_ms"] = scan_ts
            position["missing_update_count"] = int(position.get("missing_update_count") or 0) + 1
            continue
        events.extend(update_cycle_paper_position(position, row))

    open_positions = [position for position in positions if position.get("status") == "open"]
    short_decisions = [classify_cycle_short_signal(row) for row in rows]
    long_decisions = best_cycle_long_decisions(rows)
    candidate_paper = apply_pump_cycle_candidate_paper_rows(rows, output_dir=output_dir)
    candidate_shadow = attach_candidate_paper_to_shadow(build_cycle_candidate_shadow(rows), candidate_paper)
    decisions = sorted(
        [item for item in short_decisions + long_decisions if item.get("state") == "entry_ready"],
        key=cycle_decision_sort_key,
    )
    for decision in decisions:
        side = str(decision.get("side") or "")
        if cycle_open_position_exists(positions, decision=decision):
            continue
        skip_reason = cycle_capacity_block_reason(open_positions, decision)
        if skip_reason:
            event = cycle_paper_event("cycle_skip", decision=decision, ts_ms=scan_ts)
            event["reason"] = skip_reason
            events.append(event)
            continue
        position = open_cycle_paper_position(decision)
        positions.append(position)
        open_positions.append(position)
        events.append(
            {
                "event": "cycle_open",
                "ts_ms": position.get("opened_at_ms"),
                "paper_id": position.get("paper_id"),
                "side": position.get("side"),
                "track_id": position.get("track_id"),
                "symbol": position.get("symbol"),
                "event_id": position.get("event_id"),
                "position": compact_cycle_paper_position(position),
            }
        )

    previous_summary = state.get("cycle_summary") if isinstance(state.get("cycle_summary"), dict) else {}
    previous_skip_summary = state.get("skip_summary") if isinstance(state.get("skip_summary"), dict) else {}
    summary = build_cycle_paper_summary(positions)
    summary["peak_topup_needed_usd"] = round(
        max(
            to_number(previous_summary.get("peak_topup_needed_usd")),
            to_number(summary.get("peak_topup_needed_usd")),
            to_number(summary.get("current_topup_needed_usd")),
        ),
        6,
    )
    previous_peak_equity = to_number(previous_summary.get("peak_equity_mark_usd")) or PUMP_CYCLE_CAPITAL_USD
    summary["peak_equity_mark_usd"] = round(max(previous_peak_equity, to_number(summary.get("equity_mark_usd"))), 6)
    previous_drawdown = to_number(previous_summary.get("max_drawdown_usd"))
    current_drawdown = max(0.0, to_number(summary.get("peak_equity_mark_usd")) - to_number(summary.get("equity_mark_usd")))
    summary["max_drawdown_usd"] = round(max(previous_drawdown, current_drawdown), 6)
    summary["max_drawdown_pct"] = round(summary["max_drawdown_usd"] / PUMP_CYCLE_CAPITAL_USD * 100.0, 6)
    track_summaries = build_cycle_track_summaries(positions)
    skip_summary = update_cycle_skip_summary(previous_skip_summary, events)
    state.update(
        {
            "schema": "pump_cycle_paper_v1",
            "updated_at_ms": scan_ts,
            "positions": positions,
            "cycle_summary": summary,
            "track_summaries": track_summaries,
            "candidate_shadow": candidate_shadow,
            "candidate_paper": candidate_paper,
            "skip_summary": skip_summary,
            "config": {
                "capital_usd": PUMP_CYCLE_CAPITAL_USD,
                "total_slots": PUMP_CYCLE_TOTAL_SLOTS,
                "short_slots": PUMP_CYCLE_SHORT_SLOTS,
                "long_slots": PUMP_CYCLE_LONG_SLOTS,
                "slot_budget_usd": PUMP_CYCLE_SLOT_BUDGET_USD,
                "main_tracks": list(PUMP_CYCLE_MAIN_TRACKS),
                "candidate_tracks": list(PUMP_CYCLE_CANDIDATE_TRACKS),
                "candidate_mode": "shadow_paper_independent_slots",
            },
        }
    )
    save_cycle_paper_state(state_path, state)
    write_cycle_paper_csv(csv_path, positions)
    for event in events:
        append_jsonl_file(events_path, event)
    return {
        "positions": len(positions),
        "open_positions": summary.get("open_positions"),
        "closed_positions": summary.get("closed_positions"),
        "short_open_positions": summary.get("short_open_positions"),
        "long_open_positions": summary.get("long_open_positions"),
        "events": len(events),
        "equity_mark_usd": summary.get("equity_mark_usd"),
        "current_topup_usd": summary.get("current_topup_needed_usd"),
        "peak_topup_usd": summary.get("peak_topup_needed_usd"),
        "track_summaries": track_summaries,
        "candidate_shadow": candidate_shadow,
        "candidate_paper": candidate_paper,
        "skip_summary": skip_summary,
        "cycle_summary": summary,
    }


def apply_pump_cycle_candidate_paper_rows(
    rows: list[dict[str, Any]],
    *,
    output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / PUMP_CYCLE_CANDIDATE_PAPER_STATE_FILE
    events_path = output_dir / PUMP_CYCLE_CANDIDATE_PAPER_EVENTS_FILE
    csv_path = output_dir / PUMP_CYCLE_CANDIDATE_PAPER_CSV_FILE
    state = load_cycle_candidate_paper_state(state_path)
    positions = list(state.get("positions") or [])
    rows_by_symbol = {str(row.get("symbol") or ""): row for row in rows if row.get("symbol")}
    scan_ts = max((to_int(row.get("ts_ms")) or 0 for row in rows), default=now_ms()) or now_ms()
    events: list[dict[str, Any]] = []

    for position in positions:
        if position.get("status") != "open":
            continue
        row = rows_by_symbol.get(str(position.get("symbol") or ""))
        if not row:
            position["last_missing_update_at_ms"] = scan_ts
            position["missing_update_count"] = int(position.get("missing_update_count") or 0) + 1
            continue
        events.extend(update_cycle_paper_position(position, row))

    decisions = sorted(
        [item for item in cycle_candidate_decisions(rows) if item.get("state") == "entry_ready"],
        key=cycle_decision_sort_key,
    )
    open_positions = [position for position in positions if position.get("status") == "open"]
    for decision in decisions:
        if cycle_candidate_open_position_exists(positions, decision=decision):
            continue
        skip_reason = cycle_candidate_capacity_block_reason(open_positions, decision)
        if skip_reason:
            event = cycle_paper_event("candidate_skip", decision=decision, ts_ms=scan_ts)
            event["reason"] = skip_reason
            events.append(event)
            continue
        position = open_cycle_paper_position(decision)
        positions.append(position)
        open_positions.append(position)
        events.append(
            {
                "event": "candidate_open",
                "ts_ms": position.get("opened_at_ms"),
                "paper_id": position.get("paper_id"),
                "side": position.get("side"),
                "track_id": position.get("track_id"),
                "symbol": position.get("symbol"),
                "event_id": position.get("event_id"),
                "position": compact_cycle_paper_position(position),
            }
        )

    previous_summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    previous_skip_summary = state.get("skip_summary") if isinstance(state.get("skip_summary"), dict) else {}
    summary = build_cycle_candidate_paper_summary(positions)
    summary["peak_topup_needed_usd"] = round(
        max(
            to_number(previous_summary.get("peak_topup_needed_usd")),
            to_number(summary.get("peak_topup_needed_usd")),
            to_number(summary.get("current_topup_needed_usd")),
        ),
        6,
    )
    track_summaries = build_cycle_track_summaries(positions)
    skip_summary = update_cycle_skip_summary(previous_skip_summary, events)
    state.update(
        {
            "schema": "pump_cycle_candidate_paper_v1",
            "updated_at_ms": scan_ts,
            "positions": positions,
            "summary": summary,
            "track_summaries": track_summaries,
            "skip_summary": skip_summary,
            "config": {
                "capital_usd": PUMP_CYCLE_CAPITAL_USD,
                "short_slots_per_track": PUMP_CYCLE_CANDIDATE_SHORT_SLOTS,
                "long_slots_per_track": PUMP_CYCLE_CANDIDATE_LONG_SLOTS,
                "slot_budget_usd": PUMP_CYCLE_SLOT_BUDGET_USD,
                "tracks": list(PUMP_CYCLE_CANDIDATE_TRACKS),
                "mode": "shadow_paper_independent_slots",
            },
        }
    )
    save_cycle_candidate_paper_state(state_path, state)
    write_cycle_paper_csv(csv_path, positions)
    for event in events:
        append_jsonl_file(events_path, event)
    return {
        "schema": state.get("schema"),
        "updated_at_ms": state.get("updated_at_ms"),
        "config": state.get("config") or {},
        "summary": summary,
        "track_summaries": track_summaries,
        "skip_summary": skip_summary,
        "positions": positions,
        "open_positions": summary.get("open_positions"),
        "closed_positions": summary.get("closed_positions"),
        "events": len(events),
    }


def cycle_candidate_decisions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    for row in rows:
        decisions.extend(classify_cycle_candidate_short_signals(row))
        decisions.append(classify_cycle_long_signal(row, track_id="long_veto_core_midpremium", priority=50))
        decisions.append(classify_cycle_long_signal(row, track_id="long_funding_first_m10", priority=60))
    return decisions


def cycle_candidate_open_position_exists(positions: list[dict[str, Any]], *, decision: dict[str, Any]) -> bool:
    track_id = str(decision.get("track_id") or "")
    side = str(decision.get("side") or "")
    symbol = str(decision.get("symbol") or "")
    event_id = str(decision.get("event_id") or "")
    for position in positions:
        if str(position.get("track_id") or "") != track_id:
            continue
        if str(position.get("side") or "") != side:
            continue
        if str(position.get("symbol") or "") != symbol:
            continue
        if event_id and str(position.get("event_id") or "") == event_id:
            return True
    return False


def cycle_candidate_capacity_block_reason(open_positions: list[dict[str, Any]], decision: dict[str, Any]) -> str | None:
    side = str(decision.get("side") or "")
    track_id = str(decision.get("track_id") or "")
    side_count = sum(1 for item in open_positions if item.get("track_id") == track_id and item.get("side") == side)
    if side == "short" and side_count >= PUMP_CYCLE_CANDIDATE_SHORT_SLOTS:
        return "candidate_short_slots_full"
    if side == "long" and side_count >= PUMP_CYCLE_CANDIDATE_LONG_SLOTS:
        return "candidate_long_slots_full"
    return None


def build_cycle_candidate_paper_summary(positions: list[dict[str, Any]]) -> dict[str, Any]:
    open_items = [item for item in positions if item.get("status") == "open"]
    closed_items = [item for item in positions if item.get("status") == "closed"]
    realized = sum(to_number(item.get("realized_pnl_usd")) for item in positions)
    unrealized = sum(to_number(item.get("current_unrealized_pnl_usd")) for item in open_items)
    current_topup = sum(to_number(item.get("current_topup_needed_usd")) for item in open_items)
    peak_topup = max([to_number(item.get("peak_topup_needed_usd")) for item in positions] + [0.0])
    return {
        "initial_capital_usd": round(PUMP_CYCLE_CAPITAL_USD, 6),
        "slot_budget_usd": round(PUMP_CYCLE_SLOT_BUDGET_USD, 6),
        "positions": len(positions),
        "open_positions": len(open_items),
        "closed_positions": len(closed_items),
        "realized_pnl_usd": round(realized, 6),
        "unrealized_pnl_usd": round(unrealized, 6),
        "combined_pnl_usd": round(realized + unrealized, 6),
        "roi_on_initial_pct": round((realized + unrealized) / PUMP_CYCLE_CAPITAL_USD * 100.0, 6),
        "current_topup_needed_usd": round(current_topup, 6),
        "peak_topup_needed_usd": round(peak_topup, 6),
    }


def attach_candidate_paper_to_shadow(candidate_shadow: dict[str, Any], candidate_paper: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(candidate_shadow, dict):
        candidate_shadow = {}
    summaries = {
        str(row.get("track_id") or ""): row
        for row in candidate_paper.get("track_summaries") or []
        if isinstance(row, dict)
    }
    tracks: list[dict[str, Any]] = []
    for track in candidate_shadow.get("tracks") or []:
        if not isinstance(track, dict):
            continue
        summary = summaries.get(str(track.get("track_id") or ""), {})
        tracks.append({**track, "paper": summary})
    return {
        **candidate_shadow,
        "mode": "shadow_paper_independent_slots",
        "paper_summary": candidate_paper.get("summary") or {},
        "paper_track_summaries": candidate_paper.get("track_summaries") or [],
        "tracks": tracks,
    }


def classify_cycle_short_signal(row: dict[str, Any]) -> dict[str, Any]:
    strategy = {
        "strategy_id": "cycle_short_main_tiered",
        "name": "Cycle short main tiered",
        "funding_min_pct": -1.0,
        "oi_max_pct": 50.0,
        "long_ratio_min": 0.45,
        "long_ratio_max": 0.65,
        "tiers": (
            {"min_pump_pct": 0.0, "entry": "pb25", "rule_slug": "step50_legs5_equal_tp25_720"},
            {"min_pump_pct": 80.0, "entry": "pb20", "rule_slug": "step50_legs2_tapered_tp25_720"},
            {"min_pump_pct": 100.0, "entry": "pb20", "rule_slug": "step50_legs3_tapered_tp25_336"},
            {"min_pump_pct": 250.0, "entry": "pb20", "rule_slug": "step50_legs2_tapered_tp25_720"},
        ),
    }
    decision = classify_strategy_signal(strategy, row)
    decision.update({"side": "short", "track_id": "short_main_tiered", "priority": 40})
    return decision


def classify_cycle_candidate_short_signals(row: dict[str, Any]) -> list[dict[str, Any]]:
    specs = [
        (
            "short_super_250_shadow",
            20,
            {
                "strategy_id": "cycle_short_super_250_shadow",
                "funding_min_pct": -1.0,
                "oi_max_pct": 50.0,
                "long_ratio_min": 0.45,
                "long_ratio_max": 0.65,
                "tiers": (
                    {"min_pump_pct": 250.0, "entry": "pb20", "rule_slug": "step50_legs2_tapered_tp25_720"},
                ),
            },
        ),
        (
            "short_clean_p100_l3_shadow",
            30,
            {
                "strategy_id": "cycle_short_clean_p100_l3_shadow",
                "funding_min_pct": -1.0,
                "oi_max_pct": 50.0,
                "long_ratio_min": 0.45,
                "long_ratio_max": 0.65,
                "tiers": (
                    {"min_pump_pct": 100.0, "entry": "pb20", "rule_slug": "step50_legs3_tapered_tp25_336"},
                ),
            },
        ),
    ]
    decisions: list[dict[str, Any]] = []
    for track_id, priority, strategy in specs:
        decision = classify_strategy_signal(strategy, row)
        decision.update({"side": "short", "track_id": track_id, "priority": priority, "candidate_mode": "shadow_only"})
        decisions.append(decision)
    return decisions


def best_cycle_long_decisions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    for row in rows:
        candidates = [
            classify_cycle_long_signal(row, track_id="long_high_conf", priority=10),
            classify_cycle_long_signal(row, track_id="long_clean_oi", priority=20),
            classify_cycle_long_signal(row, track_id="long_broad", priority=30),
        ]
        ready = [item for item in candidates if item.get("state") == "entry_ready"]
        decisions.append(ready[0] if ready else candidates[0])
    return decisions


def classify_cycle_long_signal(row: dict[str, Any], *, track_id: str, priority: int) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "")
    ts_ms = to_int(row.get("ts_ms")) or now_ms()
    premium = to_optional_float(row.get("premium_latest_pct"))
    oi4 = to_optional_float(row.get("oi_change_4h_pct"))
    volume_z = to_optional_float(row.get("volume_z_24h"))
    hours_since_trigger = to_optional_float(row.get("hours_since_trigger"))
    source_status = str(row.get("status") or "")
    state = "waiting_pump"
    reason = "no_recent_pump"
    if source_status == "watch_slow_pump":
        state = "waiting_pump"
        reason = "research_only_slow_pump"
    elif source_status in {"no_data", "no_recent_pump"}:
        state = "waiting_pump"
        reason = source_status or "no_recent_pump"
    elif premium is None:
        state = "waiting_premium"
        reason = "missing_premium_index"
    elif hours_since_trigger is None or hours_since_trigger > 3.0:
        state = "waiting_timing"
        reason = "entry_window_expired"
    else:
        min_premium = -5.0
        max_premium = -1.0
        min_oi = 0.0
        min_volume_z: float | None = None
        if track_id == "long_clean_oi":
            min_oi = 10.0
        elif track_id == "long_high_conf":
            min_premium = -3.5
            max_premium = -1.2
            min_oi = 20.0
            min_volume_z = 1.0
        elif track_id == "long_veto_core_midpremium":
            min_premium = -3.5
            max_premium = -1.2
            min_oi = 10.0
            min_volume_z = 1.0
            if hours_since_trigger is not None and hours_since_trigger > 0.5:
                state = "waiting_timing"
                reason = "entry_window_expired"
                return build_cycle_long_decision_payload(
                    row,
                    symbol=symbol,
                    track_id=track_id,
                    priority=priority,
                    ts_ms=ts_ms,
                    state=state,
                    reason=reason,
                    source_status=source_status,
                )
        elif track_id == "long_funding_first_m10":
            funding_prev_24h = to_optional_float(row.get("funding_prev_24h_pct"))
            if funding_prev_24h is None or funding_prev_24h > -0.10:
                state = "waiting_funding"
                reason = "funding_not_negative_enough"
                return build_cycle_long_decision_payload(
                    row,
                    symbol=symbol,
                    track_id=track_id,
                    priority=priority,
                    ts_ms=ts_ms,
                    state=state,
                    reason=reason,
                    source_status=source_status,
                )
            min_oi = 0.0
        if premium < min_premium:
            state = "blocked_premium_toxic"
            reason = "premium_below_band"
        elif premium > max_premium:
            state = "waiting_premium"
            reason = "premium_not_discounted_enough"
        elif oi4 is None:
            state = "waiting_oi"
            reason = "missing_oi4"
        elif oi4 < min_oi:
            state = "waiting_oi"
            reason = "oi4_below_threshold"
        elif min_volume_z is not None and (volume_z is None or volume_z < min_volume_z):
            state = "waiting_volume"
            reason = "volume_z_below_threshold"
        else:
            state = "entry_ready"
            reason = "long_discount_conditions_met"
    return build_cycle_long_decision_payload(
        row,
        symbol=symbol,
        track_id=track_id,
        priority=priority,
        ts_ms=ts_ms,
        state=state,
        reason=reason,
        source_status=source_status,
    )


def build_cycle_long_decision_payload(
    row: dict[str, Any],
    *,
    symbol: str,
    track_id: str,
    priority: int,
    ts_ms: int,
    state: str,
    reason: str,
    source_status: str,
) -> dict[str, Any]:
    return {
        "side": "long",
        "track_id": track_id,
        "strategy_id": track_id,
        "symbol": symbol,
        "event_id": row.get("event_id"),
        "source_status": source_status,
        "state": state,
        "reason": reason,
        "priority": priority,
        "ts_ms": ts_ms,
        "last_close": row.get("last_close"),
        "hours_since_trigger": row.get("hours_since_trigger"),
        "premium_latest_pct": row.get("premium_latest_pct"),
        "premium_min_24h_pct": row.get("premium_min_24h_pct"),
        "premium_relief_1h_pct": row.get("premium_relief_1h_pct"),
        "oi_change_4h_pct": row.get("oi_change_4h_pct"),
        "oi_change_24h_pct": row.get("oi_change_24h_pct"),
        "volume_z_24h": row.get("volume_z_24h"),
        "trigger_pump_pct": row.get("trigger_pump_pct"),
    }


def build_cycle_candidate_shadow(rows: list[dict[str, Any]]) -> dict[str, Any]:
    decisions = cycle_candidate_decisions(rows)
    track_rows: list[dict[str, Any]] = []
    by_track: dict[str, list[dict[str, Any]]] = {}
    for decision in decisions:
        by_track.setdefault(str(decision.get("track_id") or ""), []).append(decision)
    for track_id, items in sorted(by_track.items()):
        ready = [item for item in items if item.get("state") == "entry_ready"]
        blocked = [item for item in items if str(item.get("state") or "").startswith("blocked")]
        watch = [item for item in items if item.get("state") != "entry_ready" and item not in blocked]
        track_rows.append(
            {
                "track_id": track_id,
                "mode": "candidate_shadow_no_slots",
                "total": len(items),
                "ready": len(ready),
                "watch": len(watch),
                "blocked": len(blocked),
                "top_ready": sorted(ready, key=cycle_decision_sort_key)[:8],
                "top_watch": sorted(watch, key=cycle_decision_sort_key)[:8],
            }
        )
    ready_all = sorted([item for item in decisions if item.get("state") == "entry_ready"], key=cycle_decision_sort_key)
    return {
        "schema": "pump_cycle_candidate_shadow_v1",
        "mode": "shadow_only_no_slots",
        "tracks": track_rows,
        "ready": ready_all[:30],
        "ready_count": len(ready_all),
        "total_decisions": len(decisions),
    }


def cycle_capacity_block_reason(open_positions: list[dict[str, Any]], decision: dict[str, Any]) -> str | None:
    side = str(decision.get("side") or "")
    symbol = str(decision.get("symbol") or "")
    if len(open_positions) >= PUMP_CYCLE_TOTAL_SLOTS:
        return "total_slots_full"
    side_count = sum(1 for item in open_positions if item.get("side") == side)
    if side == "short" and side_count >= PUMP_CYCLE_SHORT_SLOTS:
        return "short_slots_full"
    if side == "long" and side_count >= PUMP_CYCLE_LONG_SLOTS:
        return "long_slots_full"
    if any(item.get("symbol") == symbol and item.get("side") != side for item in open_positions):
        return "same_symbol_opposite_side_conflict"
    return None


def cycle_open_position_exists(positions: list[dict[str, Any]], *, decision: dict[str, Any]) -> bool:
    side = str(decision.get("side") or "")
    symbol = str(decision.get("symbol") or "")
    event_id = str(decision.get("event_id") or "")
    for position in positions:
        if str(position.get("side") or "") != side:
            continue
        if str(position.get("symbol") or "") != symbol:
            continue
        if event_id and str(position.get("event_id") or "") == event_id:
            return True
    return False


def open_cycle_paper_position(decision: dict[str, Any]) -> dict[str, Any]:
    side = str(decision.get("side") or "")
    ts_ms = to_int(decision.get("ts_ms")) or now_ms()
    current_price = to_number(decision.get("last_close"))
    track_id = str(decision.get("track_id") or side)
    paper_id = "|".join([track_id, str(decision.get("symbol") or ""), str(decision.get("event_id") or ts_ms)])
    if side == "short":
        tier = dict(decision.get("tier") or {})
        if not tier:
            tier = {
                "min_pump_pct": 100.0,
                "entry": "pb20",
                "rule_slug": "step50_legs3_tapered_tp25_336",
                "ladder_legs": 3,
                "ladder_step_pct": 50.0,
                "leg_weights": [3.0, 2.0, 1.0],
                "tp_pct": 25.0,
                "max_hold_h": 336,
            }
        legs = build_strategy_paper_legs(
            entry_price=current_price,
            tier=tier,
            per_coin_capital=PUMP_CYCLE_SLOT_BUDGET_USD,
            leverage=PUMP_CYCLE_SHORT_LEVERAGE,
            opened_at_ms=ts_ms,
        )
    else:
        tier = {
            "rule_slug": "long_tp30_sl25_hold72",
            "tp_pct": PUMP_CYCLE_LONG_TP_PCT,
            "stop_pct": PUMP_CYCLE_LONG_STOP_PCT,
            "max_hold_h": PUMP_CYCLE_LONG_MAX_HOLD_H,
        }
        legs = [
            {
                "step": 1,
                "trigger_price": current_price,
                "entry_price": current_price,
                "weight": 1.0,
                "margin_usd": round(PUMP_CYCLE_SLOT_BUDGET_USD, 6),
                "notional_usd": round(PUMP_CYCLE_SLOT_BUDGET_USD * PUMP_CYCLE_LONG_LEVERAGE, 6),
                "filled": True,
                "filled_at_ms": ts_ms,
                "closed": False,
                "closed_at_ms": None,
                "realized_pnl_usd": 0.0,
            }
        ]
    position = {
        "paper_id": paper_id,
        "schema": "pump_cycle_paper_position_v1",
        "status": "open",
        "side": side,
        "track_id": track_id,
        "symbol": decision.get("symbol"),
        "event_id": decision.get("event_id"),
        "opened_at_ms": ts_ms,
        "updated_at_ms": ts_ms,
        "closed_at_ms": None,
        "entry_price": current_price,
        "current_price": current_price,
        "avg_entry_price": current_price,
        "target_price": None,
        "stop_price": None,
        "exit_reason": None,
        "capital_usd": PUMP_CYCLE_CAPITAL_USD,
        "slot_budget_usd": PUMP_CYCLE_SLOT_BUDGET_USD,
        "leverage": PUMP_CYCLE_SHORT_LEVERAGE if side == "short" else PUMP_CYCLE_LONG_LEVERAGE,
        "tier": tier,
        "legs": legs,
        "filled_steps": 1,
        "planned_steps": len(legs),
        "used_margin_usd": 0.0,
        "gross_notional_usd": 0.0,
        "current_pnl_pct": 0.0,
        "current_unrealized_pnl_usd": 0.0,
        "realized_pnl_usd": 0.0,
        "combined_pnl_usd": 0.0,
        "current_topup_needed_usd": 0.0,
        "peak_topup_needed_usd": 0.0,
        "mae_pct": 0.0,
        "mfe_pct": 0.0,
        "open_decision": decision,
        "last_decision": decision,
    }
    recompute_cycle_paper_position_metrics(position, current_price=current_price, now_ms_value=ts_ms)
    return position


def update_cycle_paper_position(position: dict[str, Any], row: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    now_value = to_int(row.get("ts_ms")) or now_ms()
    current_price = to_number(row.get("last_close"))
    if current_price <= 0:
        return events
    position["updated_at_ms"] = now_value
    position["current_price"] = current_price
    position["last_decision"] = row
    if position.get("side") == "short":
        for leg in position.get("legs") or []:
            if leg.get("filled") or leg.get("closed"):
                continue
            trigger = to_number(leg.get("trigger_price"))
            if trigger > 0 and current_price >= trigger:
                leg["filled"] = True
                leg["entry_price"] = round(trigger, 10)
                leg["filled_at_ms"] = now_value
                events.append({"event": "cycle_add_short_leg", "ts_ms": now_value, "paper_id": position.get("paper_id"), "symbol": position.get("symbol"), "step": leg.get("step")})
    recompute_cycle_paper_position_metrics(position, current_price=current_price, now_ms_value=now_value)
    previous_peak = to_number(position.get("peak_topup_needed_usd"))
    current_topup = to_number(position.get("current_topup_needed_usd"))
    if current_topup > previous_peak:
        position["peak_topup_needed_usd"] = round(current_topup, 6)
        events.append({"event": "cycle_topup_peak", "ts_ms": now_value, "paper_id": position.get("paper_id"), "symbol": position.get("symbol"), "topup_needed_usd": round(current_topup, 6)})
    close_event = maybe_close_cycle_paper_position(position, current_price=current_price, now_ms_value=now_value)
    if close_event:
        events.append(close_event)
    return events


def aggregate_cycle_open_leg_metrics(
    open_legs: list[dict[str, Any]],
    *,
    side: str,
    current_price: float,
    fallback_entry: float,
) -> dict[str, float]:
    used_margin = sum(to_number(leg.get("margin_usd")) for leg in open_legs)
    gross_notional = sum(to_number(leg.get("notional_usd")) for leg in open_legs)
    total_quantity = 0.0
    gross_pnl = 0.0
    for leg in open_legs:
        entry_price = to_number(leg.get("entry_price"))
        notional = to_number(leg.get("notional_usd"))
        if entry_price <= 0 or notional <= 0:
            continue
        total_quantity += notional / entry_price
        leg_pnl_pct = short_pnl_pct(entry_price, current_price) if side == "short" else long_pnl_pct(entry_price, current_price)
        gross_pnl += notional * leg_pnl_pct / 100.0
    avg_entry = gross_notional / total_quantity if total_quantity > 0 else fallback_entry
    pnl_pct = gross_pnl / gross_notional * 100.0 if gross_notional > 0 else 0.0
    return {
        "used_margin": used_margin,
        "gross_notional": gross_notional,
        "avg_entry": avg_entry,
        "gross_pnl": gross_pnl,
        "pnl_pct": pnl_pct,
    }


def recompute_cycle_paper_position_metrics(position: dict[str, Any], *, current_price: float, now_ms_value: int) -> None:
    open_legs = [leg for leg in position.get("legs") or [] if leg.get("filled") and not leg.get("closed")]
    side = str(position.get("side") or "")
    metrics = aggregate_cycle_open_leg_metrics(
        open_legs,
        side=side,
        current_price=current_price,
        fallback_entry=to_number(position.get("entry_price")),
    )
    used_margin = metrics["used_margin"]
    gross_notional = metrics["gross_notional"]
    avg_entry = metrics["avg_entry"]
    gross_pnl = metrics["gross_pnl"]
    pnl_pct = metrics["pnl_pct"]
    fee_if_closed = gross_notional * PUMP_CYCLE_FEE_ROUNDTRIP_PCT / 100.0
    unrealized_net = gross_pnl - fee_if_closed
    opened_at = to_int(position.get("opened_at_ms"))
    max_hold_h = to_int((position.get("tier") or {}).get("max_hold_h")) or (336 if side == "short" else PUMP_CYCLE_LONG_MAX_HOLD_H)
    time_in_trade_h = ((now_ms_value - opened_at) / 3_600_000.0) if opened_at and now_ms_value >= opened_at else 0.0
    if side == "short":
        tp_pct = to_number((position.get("tier") or {}).get("tp_pct")) or 25.0
        target_price = avg_entry * (1.0 - tp_pct / 100.0) if avg_entry > 0 else None
        stop_price = None
        current_topup = max(0.0, max(0.0, -unrealized_net) - PUMP_CYCLE_SLOT_BUDGET_USD)
    else:
        target_price = avg_entry * (1.0 + PUMP_CYCLE_LONG_TP_PCT / 100.0) if avg_entry > 0 else None
        stop_price = avg_entry * (1.0 - PUMP_CYCLE_LONG_STOP_PCT / 100.0) if avg_entry > 0 else None
        current_topup = 0.0
    position.update(
        {
            "filled_steps": len(open_legs),
            "avg_entry_price": round(avg_entry, 10) if avg_entry else None,
            "target_price": round(target_price, 10) if target_price else None,
            "stop_price": round(stop_price, 10) if stop_price else None,
            "used_margin_usd": round(used_margin, 6),
            "gross_notional_usd": round(gross_notional, 6),
            "current_pnl_pct": round(pnl_pct, 6),
            "current_unrealized_pnl_usd": round(unrealized_net, 6),
            "combined_pnl_usd": round(to_number(position.get("realized_pnl_usd")) + unrealized_net, 6),
            "current_topup_needed_usd": round(current_topup, 6),
            "mae_pct": round(max(to_number(position.get("mae_pct")), -pnl_pct), 6),
            "mfe_pct": round(max(to_number(position.get("mfe_pct")), pnl_pct), 6),
            "time_in_trade_h": round(time_in_trade_h, 3),
            "max_hold_h": max_hold_h,
            "hours_left_h": round(max(0.0, max_hold_h - time_in_trade_h), 3),
        }
    )


def maybe_close_cycle_paper_position(position: dict[str, Any], *, current_price: float, now_ms_value: int) -> dict[str, Any] | None:
    if position.get("status") != "open":
        return None
    side = str(position.get("side") or "")
    open_legs = [leg for leg in position.get("legs") or [] if leg.get("filled") and not leg.get("closed")]
    metrics = aggregate_cycle_open_leg_metrics(
        open_legs,
        side=side,
        current_price=current_price,
        fallback_entry=to_number(position.get("avg_entry_price")) or to_number(position.get("entry_price")),
    )
    avg_entry = metrics["avg_entry"]
    gross_notional = metrics["gross_notional"]
    if avg_entry <= 0 or gross_notional <= 0:
        return None
    pnl_pct = metrics["pnl_pct"]
    reason = None
    if side == "short" and pnl_pct >= to_number((position.get("tier") or {}).get("tp_pct")):
        reason = "short_take_profit"
        pnl_pct = to_number((position.get("tier") or {}).get("tp_pct"))
    elif side == "long" and pnl_pct >= PUMP_CYCLE_LONG_TP_PCT:
        reason = "long_take_profit"
        pnl_pct = PUMP_CYCLE_LONG_TP_PCT
    elif side == "long" and pnl_pct <= -PUMP_CYCLE_LONG_STOP_PCT:
        reason = "long_stop_loss"
        pnl_pct = -PUMP_CYCLE_LONG_STOP_PCT
    else:
        opened_at = to_int(position.get("opened_at_ms")) or now_ms_value
        max_hold_h = to_int(position.get("max_hold_h")) or (336 if side == "short" else PUMP_CYCLE_LONG_MAX_HOLD_H)
        if now_ms_value - opened_at >= max_hold_h * 3_600_000:
            reason = "time_stop"
    if not reason:
        return None
    net_pnl = gross_notional * (pnl_pct - PUMP_CYCLE_FEE_ROUNDTRIP_PCT) / 100.0
    for leg in position.get("legs") or []:
        if leg.get("filled") and not leg.get("closed"):
            leg["closed"] = True
            leg["closed_at_ms"] = now_ms_value
    position.update(
        {
            "status": "closed",
            "closed_at_ms": now_ms_value,
            "exit_reason": reason,
            "realized_pnl_usd": round(to_number(position.get("realized_pnl_usd")) + net_pnl, 6),
            "current_unrealized_pnl_usd": 0.0,
            "combined_pnl_usd": round(to_number(position.get("realized_pnl_usd")) + net_pnl, 6),
            "current_topup_needed_usd": 0.0,
        }
    )
    return {"event": "cycle_close", "ts_ms": now_ms_value, "paper_id": position.get("paper_id"), "side": side, "symbol": position.get("symbol"), "reason": reason, "net_pnl_usd": round(net_pnl, 6)}


def build_cycle_paper_summary(positions: list[dict[str, Any]]) -> dict[str, Any]:
    open_items = [item for item in positions if item.get("status") == "open"]
    closed_items = [item for item in positions if item.get("status") == "closed"]
    short_open = [item for item in open_items if item.get("side") == "short"]
    long_open = [item for item in open_items if item.get("side") == "long"]
    realized = sum(to_number(item.get("realized_pnl_usd")) for item in positions)
    unrealized = sum(to_number(item.get("current_unrealized_pnl_usd")) for item in open_items)
    used_margin = sum(to_number(item.get("used_margin_usd")) for item in open_items)
    current_topup = sum(to_number(item.get("current_topup_needed_usd")) for item in short_open)
    peak_topup = max([to_number(item.get("peak_topup_needed_usd")) for item in positions] + [0.0])
    return {
        "initial_capital_usd": round(PUMP_CYCLE_CAPITAL_USD, 6),
        "slot_budget_usd": round(PUMP_CYCLE_SLOT_BUDGET_USD, 6),
        "equity_realized_usd": round(PUMP_CYCLE_CAPITAL_USD + realized, 6),
        "equity_mark_usd": round(PUMP_CYCLE_CAPITAL_USD + realized + unrealized, 6),
        "realized_pnl_usd": round(realized, 6),
        "unrealized_pnl_usd": round(unrealized, 6),
        "combined_pnl_usd": round(realized + unrealized, 6),
        "roi_mark_pct": round((realized + unrealized) / PUMP_CYCLE_CAPITAL_USD * 100.0, 6),
        "positions": len(positions),
        "open_positions": len(open_items),
        "closed_positions": len(closed_items),
        "short_open_positions": len(short_open),
        "long_open_positions": len(long_open),
        "free_total_slots": max(0, PUMP_CYCLE_TOTAL_SLOTS - len(open_items)),
        "free_short_slots": max(0, PUMP_CYCLE_SHORT_SLOTS - len(short_open)),
        "free_long_slots": max(0, PUMP_CYCLE_LONG_SLOTS - len(long_open)),
        "used_margin_usd": round(used_margin, 6),
        "current_topup_needed_usd": round(current_topup, 6),
        "peak_topup_needed_usd": round(peak_topup, 6),
    }


def build_cycle_track_summaries(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for position in positions:
        grouped.setdefault(str(position.get("track_id") or "unknown"), []).append(position)
    rows: list[dict[str, Any]] = []
    for track_id, items in sorted(grouped.items()):
        open_items = [item for item in items if item.get("status") == "open"]
        closed_items = [item for item in items if item.get("status") == "closed"]
        realized = sum(to_number(item.get("realized_pnl_usd")) for item in items)
        unrealized = sum(to_number(item.get("current_unrealized_pnl_usd")) for item in open_items)
        wins = sum(1 for item in closed_items if to_number(item.get("realized_pnl_usd")) > 0)
        rows.append(
            {
                "track_id": track_id,
                "side": str(items[0].get("side") or ""),
                "positions": len(items),
                "open_positions": len(open_items),
                "closed_positions": len(closed_items),
                "win_pct": round_optional((wins / len(closed_items) * 100.0) if closed_items else None, 6),
                "realized_pnl_usd": round(realized, 6),
                "unrealized_pnl_usd": round(unrealized, 6),
                "combined_pnl_usd": round(realized + unrealized, 6),
                "roi_on_initial_pct": round((realized + unrealized) / PUMP_CYCLE_CAPITAL_USD * 100.0, 6),
                "current_topup_needed_usd": round(sum(to_number(item.get("current_topup_needed_usd")) for item in open_items), 6),
                "peak_topup_needed_usd": round(max([to_number(item.get("peak_topup_needed_usd")) for item in items] + [0.0]), 6),
            }
        )
    rows.sort(key=lambda row: (to_number(row.get("combined_pnl_usd")), to_number(row.get("positions"))), reverse=True)
    return rows


def update_cycle_skip_summary(previous: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    by_reason = dict(previous.get("by_reason") or {}) if isinstance(previous, dict) else {}
    by_side = dict(previous.get("by_side") or {}) if isinstance(previous, dict) else {}
    total = int(previous.get("total") or 0) if isinstance(previous, dict) else 0
    for event in events:
        if event.get("event") != "cycle_skip":
            continue
        total += 1
        reason = str(event.get("reason") or "unknown")
        side = str(event.get("side") or "unknown")
        by_reason[reason] = int(by_reason.get(reason) or 0) + 1
        by_side[side] = int(by_side.get(side) or 0) + 1
    return {"total": total, "by_reason": by_reason, "by_side": by_side}


def read_cycle_paper_summary(*, output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR, limit: int = 200) -> dict[str, Any]:
    state = load_cycle_paper_state(output_dir / PUMP_CYCLE_PAPER_STATE_FILE)
    positions = list(state.get("positions") or [])
    positions.sort(key=lambda item: int(float(item.get("updated_at_ms") or item.get("opened_at_ms") or 0)), reverse=True)
    candidate_paper = read_cycle_candidate_paper_summary(output_dir=output_dir, limit=limit)
    candidate_shadow = attach_candidate_paper_to_shadow(state.get("candidate_shadow") or {}, candidate_paper)
    return {
        "schema": state.get("schema") or "pump_cycle_paper_v1",
        "updated_at_ms": state.get("updated_at_ms"),
        "config": state.get("config") or {},
        "summary": state.get("cycle_summary") or build_cycle_paper_summary(positions),
        "track_summaries": state.get("track_summaries") or build_cycle_track_summaries(positions),
        "candidate_shadow": candidate_shadow,
        "candidate_paper": candidate_paper,
        "skip_summary": state.get("skip_summary") or {},
        "positions": positions[:limit],
        "open_positions": sum(1 for item in positions if item.get("status") == "open"),
        "closed_positions": sum(1 for item in positions if item.get("status") == "closed"),
        "events_latest": read_latest_jsonl(output_dir / PUMP_CYCLE_PAPER_EVENTS_FILE, limit=50),
    }


def load_cycle_paper_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema": "pump_cycle_paper_v1", "positions": [], "updated_at_ms": None, "cycle_summary": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema": "pump_cycle_paper_v1", "positions": [], "updated_at_ms": None, "cycle_summary": {}}
    if not isinstance(payload, dict):
        return {"schema": "pump_cycle_paper_v1", "positions": [], "updated_at_ms": None, "cycle_summary": {}}
    payload.setdefault("positions", [])
    payload.setdefault("cycle_summary", {})
    return payload


def save_cycle_paper_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")


def read_cycle_candidate_paper_summary(*, output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR, limit: int = 200) -> dict[str, Any]:
    state = load_cycle_candidate_paper_state(output_dir / PUMP_CYCLE_CANDIDATE_PAPER_STATE_FILE)
    positions = list(state.get("positions") or [])
    positions.sort(key=lambda item: int(float(item.get("updated_at_ms") or item.get("opened_at_ms") or 0)), reverse=True)
    return {
        "schema": state.get("schema") or "pump_cycle_candidate_paper_v1",
        "updated_at_ms": state.get("updated_at_ms"),
        "config": state.get("config") or {},
        "summary": state.get("summary") or build_cycle_candidate_paper_summary(positions),
        "track_summaries": state.get("track_summaries") or build_cycle_track_summaries(positions),
        "skip_summary": state.get("skip_summary") or {},
        "positions": positions[:limit],
        "open_positions": sum(1 for item in positions if item.get("status") == "open"),
        "closed_positions": sum(1 for item in positions if item.get("status") == "closed"),
        "events_latest": read_latest_jsonl(output_dir / PUMP_CYCLE_CANDIDATE_PAPER_EVENTS_FILE, limit=50),
    }


def load_cycle_candidate_paper_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema": "pump_cycle_candidate_paper_v1", "positions": [], "updated_at_ms": None, "summary": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema": "pump_cycle_candidate_paper_v1", "positions": [], "updated_at_ms": None, "summary": {}}
    if not isinstance(payload, dict):
        return {"schema": "pump_cycle_candidate_paper_v1", "positions": [], "updated_at_ms": None, "summary": {}}
    payload.setdefault("positions", [])
    payload.setdefault("summary", {})
    return payload


def save_cycle_candidate_paper_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")


def write_cycle_paper_csv(path: Path, positions: list[dict[str, Any]]) -> None:
    rows = [flatten_cycle_paper_position(position) for position in positions]
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def flatten_cycle_paper_position(position: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_id": position.get("paper_id"),
        "side": position.get("side"),
        "track_id": position.get("track_id"),
        "symbol": position.get("symbol"),
        "status": position.get("status"),
        "event_id": position.get("event_id"),
        "opened_at_ms": position.get("opened_at_ms"),
        "updated_at_ms": position.get("updated_at_ms"),
        "closed_at_ms": position.get("closed_at_ms"),
        "entry_price": position.get("entry_price"),
        "current_price": position.get("current_price"),
        "avg_entry_price": position.get("avg_entry_price"),
        "target_price": position.get("target_price"),
        "stop_price": position.get("stop_price"),
        "filled_steps": position.get("filled_steps"),
        "planned_steps": position.get("planned_steps"),
        "used_margin_usd": position.get("used_margin_usd"),
        "gross_notional_usd": position.get("gross_notional_usd"),
        "current_pnl_pct": position.get("current_pnl_pct"),
        "current_unrealized_pnl_usd": position.get("current_unrealized_pnl_usd"),
        "realized_pnl_usd": position.get("realized_pnl_usd"),
        "combined_pnl_usd": position.get("combined_pnl_usd"),
        "current_topup_needed_usd": position.get("current_topup_needed_usd"),
        "peak_topup_needed_usd": position.get("peak_topup_needed_usd"),
        "exit_reason": position.get("exit_reason"),
    }


def compact_cycle_paper_position(position: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_id": position.get("paper_id"),
        "side": position.get("side"),
        "track_id": position.get("track_id"),
        "symbol": position.get("symbol"),
        "status": position.get("status"),
        "opened_at_ms": position.get("opened_at_ms"),
        "entry_price": position.get("entry_price"),
        "slot_budget_usd": position.get("slot_budget_usd"),
    }


def cycle_paper_event(event: str, *, decision: dict[str, Any], ts_ms: int) -> dict[str, Any]:
    return {
        "event": event,
        "ts_ms": ts_ms,
        "side": decision.get("side"),
        "track_id": decision.get("track_id"),
        "symbol": decision.get("symbol"),
        "event_id": decision.get("event_id"),
        "reason": decision.get("reason"),
        "state": decision.get("state"),
    }


def cycle_decision_sort_key(decision: dict[str, Any]) -> tuple[int, float, str]:
    return (
        int(decision.get("priority") or 99),
        -(to_optional_float(decision.get("trigger_pump_pct")) or 0.0),
        str(decision.get("symbol") or ""),
    )


def apply_pump_active_window_scan(
    rows: list[dict[str, Any]],
    *,
    output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR,
    sleep_sec: float = 0.1,
    max_symbols: int = PUMP_ACTIVE_WINDOW_MAX_SYMBOLS,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    latest_path = output_dir / PUMP_ACTIVE_WINDOW_LATEST_FILE
    csv_path = output_dir / PUMP_ACTIVE_WINDOW_CSV_FILE
    samples_path = output_dir / PUMP_ACTIVE_WINDOW_SAMPLES_FILE
    errors_path = output_dir / PUMP_ACTIVE_WINDOW_ERRORS_FILE
    selected = select_active_window_rows(rows, output_dir=output_dir, max_symbols=max_symbols)
    started = now_ms()
    collector = BybitPumpShortCollector(
        BybitCollectorConfig(
            output_dir=output_dir,
            sleep_sec=max(0.0, float(sleep_sec or 0.0)),
            timeout_sec=15.0,
            max_retries=2,
        )
    )
    summaries: list[dict[str, Any]] = []
    errors = 0
    for row in selected:
        symbol = normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        try:
            sample = collect_active_window_sample(collector, row)
            summary = build_active_window_summary(sample)
            summaries.append(summary)
            append_jsonl_file(samples_path, {"ts_ms": started, "summary": summary, "sample": sample})
        except Exception as exc:  # pylint: disable=broad-except
            errors += 1
            append_jsonl_file(
                errors_path,
                {
                    "ts_ms": now_ms(),
                    "symbol": symbol,
                    "event_id": row.get("event_id"),
                    "error": str(exc),
                },
            )
    payload = {
        "schema": "pump_active_window_v1",
        "updated_at_ms": now_ms(),
        "interval": PUMP_ACTIVE_WINDOW_INTERVAL,
        "selected_symbols": len(selected),
        "symbols": len(summaries),
        "errors": errors,
        "requests_made": collector.stats.requests_made,
        "rows": summaries,
        "config": {
            "interval": PUMP_ACTIVE_WINDOW_INTERVAL,
            "pre_hours": PUMP_ACTIVE_WINDOW_PRE_HOURS,
            "lookback_hours": PUMP_ACTIVE_WINDOW_LOOKBACK_HOURS,
            "max_symbols": max_symbols,
            "slow_watch_max_symbols": PUMP_ACTIVE_WINDOW_SLOW_WATCH_MAX_SYMBOLS,
        },
    }
    latest_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    write_active_window_csv(csv_path, summaries)
    return {
        "symbols": len(summaries),
        "selected_symbols": len(selected),
        "errors": errors,
        "requests_made": collector.stats.requests_made,
    }


def select_active_window_rows(
    rows: list[dict[str, Any]],
    *,
    output_dir: Path,
    max_symbols: int,
) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        status = str(row.get("status") or "")
        has_event = bool(row.get("trigger_ts") or row.get("event_id") or row.get("trigger_pump_pct"))
        if status != "no_recent_pump" or has_event:
            selected[symbol] = dict(row, active_source=status or "shadow")
    for position in open_paper_positions_for_active_window(output_dir):
        symbol = normalize_symbol(position.get("symbol"))
        if not symbol:
            continue
        if symbol in selected:
            selected[symbol]["active_open_paper"] = True
            selected[symbol]["active_source"] = str(
                position.get("track_id") or position.get("strategy_id") or position.get("side") or "paper"
            )
            continue
        selected[symbol] = {
            "symbol": symbol,
            "event_id": position.get("event_id"),
            "trigger_ts": position.get("opened_at_ms"),
            "ts_ms": position.get("updated_at_ms") or position.get("opened_at_ms"),
            "status": "open_paper_position",
            "active_source": str(position.get("track_id") or position.get("strategy_id") or position.get("side") or "paper"),
            "last_close": position.get("current_price"),
        }
    out = list(selected.values())
    out.sort(key=active_window_row_sort_key)
    limited: list[dict[str, Any]] = []
    slow_watch_count = 0
    for row in out:
        if row.get("status") == "watch_slow_pump" and not row.get("active_open_paper"):
            if slow_watch_count >= PUMP_ACTIVE_WINDOW_SLOW_WATCH_MAX_SYMBOLS:
                continue
            slow_watch_count += 1
        limited.append(row)
        if len(limited) >= max(0, int(max_symbols or 0)):
            break
    return limited


def open_paper_positions_for_active_window(output_dir: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for summary in (
        read_cycle_paper_summary(output_dir=output_dir, limit=500),
        read_strategy_paper_summary(output_dir=output_dir, limit=500),
    ):
        for position in summary.get("positions") or []:
            if position.get("status") == "open":
                out.append(position)
    return out


def active_window_row_sort_key(row: dict[str, Any]) -> tuple[int, float, str]:
    status = str(row.get("status") or "")
    if row.get("active_open_paper") or status == "entry_candidate" or status == "open_paper_position":
        priority = 0
    elif status.startswith("watch") and status != "watch_slow_pump":
        priority = 1
    elif status == "watch_slow_pump":
        priority = 2
    else:
        priority = 3
    strength = to_optional_float(row.get("trigger_pump_pct"))
    if strength is None:
        strength = to_optional_float(row.get("slow_pump_return_pct"))
    return (priority, -(strength or 0.0), str(row.get("symbol") or ""))


def collect_active_window_sample(collector: BybitPumpShortCollector, row: dict[str, Any]) -> dict[str, Any]:
    symbol = normalize_symbol(row.get("symbol"))
    if not symbol:
        raise ValueError("missing active-window symbol")
    end_ms = now_ms()
    is_slow_watch = str(row.get("status") or "") == "watch_slow_pump"
    trigger_ts = (
        to_int(row.get("trigger_ts"))
        or to_int(row.get("slow_pump_trigger_ts"))
        or to_int(row.get("ts_ms"))
        or end_ms
    )
    if is_slow_watch:
        start_ms = end_ms - PUMP_ACTIVE_WINDOW_LOOKBACK_HOURS * 3_600_000
    else:
        start_ms = min(
            trigger_ts - PUMP_ACTIVE_WINDOW_PRE_HOURS * 3_600_000,
            end_ms - PUMP_ACTIVE_WINDOW_LOOKBACK_HOURS * 3_600_000,
        )
    start_ms = max(0, start_ms)
    klines = collector.fetch_klines(symbol, interval=PUMP_ACTIVE_WINDOW_INTERVAL, start_ms=start_ms, end_ms=end_ms)
    premium = collector.fetch_price_klines(
        "/v5/market/premium-index-price-kline",
        symbol,
        interval=PUMP_ACTIVE_WINDOW_INTERVAL,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    mark = collector.fetch_price_klines(
        "/v5/market/mark-price-kline",
        symbol,
        interval=PUMP_ACTIVE_WINDOW_INTERVAL,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    index = collector.fetch_price_klines(
        "/v5/market/index-price-kline",
        symbol,
        interval=PUMP_ACTIVE_WINDOW_INTERVAL,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    oi = collector.fetch_open_interest(symbol, interval_time="5min", start_ms=start_ms, end_ms=end_ms)
    funding = collector.fetch_funding_history(symbol, start_ms=start_ms, end_ms=end_ms)
    return {
        "schema": "pump_active_window_sample_v1",
        "symbol": symbol,
        "row": row,
        "trigger_ts": trigger_ts,
        "start_ts": start_ms,
        "end_ts": end_ms,
        "interval": PUMP_ACTIVE_WINDOW_INTERVAL,
        "collection_mode": "rolling_24h_research_only" if is_slow_watch else "event_window",
        "series": {
            "klines": klines,
            "premium_index": premium,
            "mark_price": mark,
            "index_price": index,
            "open_interest": oi,
            "funding": funding,
        },
    }


def build_active_window_summary(sample: dict[str, Any]) -> dict[str, Any]:
    row = sample.get("row") if isinstance(sample.get("row"), dict) else {}
    series = sample.get("series") if isinstance(sample.get("series"), dict) else {}
    klines = series.get("klines") or []
    premium = series.get("premium_index") or []
    mark = series.get("mark_price") or []
    index = series.get("index_price") or []
    oi = series.get("open_interest") or []
    funding = series.get("funding") or []
    end_ts = to_int(sample.get("end_ts")) or now_ms()
    trigger_ts = to_int(sample.get("trigger_ts")) or end_ts
    last_close = active_latest_value(klines, "close")
    trigger_close = active_value_at_or_before(klines, trigger_ts, "close")
    if trigger_close is None:
        trigger_close = to_optional_float(row.get("slow_pump_trigger_close"))
    premium_latest = active_latest_value(premium, "close")
    premium_min_1h = active_min_between(premium, end_ts - 3_600_000, end_ts, "low")
    premium_min_4h = active_min_between(premium, end_ts - 4 * 3_600_000, end_ts, "low")
    premium_1h_ago = active_value_at_or_before(premium, end_ts - 3_600_000, "close")
    oi_latest = active_latest_value(oi, "open_interest")
    oi_1h_ago = active_value_at_or_before(oi, end_ts - 3_600_000, "open_interest")
    oi_4h_ago = active_value_at_or_before(oi, end_ts - 4 * 3_600_000, "open_interest")
    mark_latest = active_latest_value(mark, "close")
    index_latest = active_latest_value(index, "close")
    summary = {
        "ts_ms": end_ts,
        "symbol": sample.get("symbol"),
        "source_status": row.get("status"),
        "active_source": row.get("active_source"),
        "event_id": row.get("event_id"),
        "slow_pump_event_id": row.get("slow_pump_event_id"),
        "trigger_ts": trigger_ts,
        "hours_since_trigger": round_optional((end_ts - trigger_ts) / 3_600_000.0 if end_ts >= trigger_ts else None, 3),
        "trigger_pump_pct": round_optional(to_optional_float(row.get("trigger_pump_pct")), 6),
        "slow_pump_return_pct": round_optional(to_optional_float(row.get("slow_pump_return_pct")), 6),
        "slow_pump_window_h": to_int(row.get("slow_pump_window_h")),
        "slow_pump_stage": row.get("slow_pump_stage"),
        "slow_pump_pullback_from_high_pct": round_optional(
            to_optional_float(row.get("slow_pump_pullback_from_high_pct")),
            6,
        ),
        "research_mode": row.get("research_mode"),
        "active_open_paper": bool(row.get("active_open_paper")),
        "collection_mode": sample.get("collection_mode"),
        "shadow_pullback_from_high_pct": round_optional(to_optional_float(row.get("pullback_from_high_pct")), 6),
        "last_close_5m": round_optional(last_close, 10),
        "return_from_trigger_pct_5m": round_optional(pct_change_number(last_close, trigger_close), 6),
        "premium_latest_pct_5m": round_optional(scale_decimal_pct(premium_latest), 6),
        "premium_min_1h_pct_5m": round_optional(scale_decimal_pct(premium_min_1h), 6),
        "premium_min_4h_pct_5m": round_optional(scale_decimal_pct(premium_min_4h), 6),
        "premium_relief_1h_pct_5m": round_optional(scale_decimal_pct(active_point_change(premium_latest, premium_1h_ago)), 6),
        "oi_change_1h_pct_5m": round_optional(pct_change_number(oi_latest, oi_1h_ago), 6),
        "oi_change_4h_pct_5m": round_optional(pct_change_number(oi_latest, oi_4h_ago), 6),
        "volume_z_24h_5m": round_optional(active_latest_volume_z(klines, end_ts, lookback_rows=288), 6),
        "mark_index_basis_pct_5m": round_optional(mark_index_basis_pct(mark_latest, index_latest), 6),
        "funding_recent_pct": round_optional(scale_decimal_pct(sum_recent_funding(funding, end_ts - 24 * 3_600_000, end_ts)), 6),
        "klines_5m": len(klines),
        "premium_points_5m": len(premium),
        "oi_points_5m": len(oi),
        "funding_points": len(funding),
    }
    long_probe = classify_cycle_long_signal(
        {
            **row,
            "status": row.get("status") or "active_window",
            "last_close": summary["last_close_5m"],
            "premium_latest_pct": summary["premium_latest_pct_5m"],
            "oi_change_4h_pct": summary["oi_change_4h_pct_5m"],
            "volume_z_24h": summary["volume_z_24h_5m"],
            "hours_since_trigger": summary["hours_since_trigger"],
        },
        track_id="long_broad",
        priority=30,
    )
    summary["long_broad_state_5m"] = long_probe.get("state")
    summary["long_broad_reason_5m"] = long_probe.get("reason")
    return summary


def read_active_window_summary(*, output_dir: Path = DEFAULT_SHADOW_OUTPUT_DIR) -> dict[str, Any]:
    path = output_dir / PUMP_ACTIVE_WINDOW_LATEST_FILE
    if not path.exists():
        return {"schema": "pump_active_window_v1", "updated_at_ms": None, "rows": [], "symbols": 0, "errors": 0}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema": "pump_active_window_v1", "updated_at_ms": None, "rows": [], "symbols": 0, "errors": 0}
    return payload if isinstance(payload, dict) else {"schema": "pump_active_window_v1", "updated_at_ms": None, "rows": [], "symbols": 0, "errors": 0}


def write_active_window_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def active_latest_value(rows: list[dict[str, Any]], key: str) -> float | None:
    for row in sorted(rows or [], key=lambda item: to_int(item.get("ts_ms")) or 0, reverse=True):
        value = to_optional_float(row.get(key))
        if value is not None:
            return value
    return None


def active_value_at_or_before(rows: list[dict[str, Any]], ts_ms: int, key: str) -> float | None:
    out: float | None = None
    for row in sorted(rows or [], key=lambda item: to_int(item.get("ts_ms")) or 0):
        row_ts = to_int(row.get("ts_ms"))
        if row_ts is None or row_ts > ts_ms:
            continue
        value = to_optional_float(row.get(key))
        if value is not None:
            out = value
    return out


def active_min_between(rows: list[dict[str, Any]], start_ms: int, end_ms: int, key: str) -> float | None:
    values = [
        to_optional_float(row.get(key))
        for row in rows or []
        if (to_int(row.get("ts_ms")) or 0) >= start_ms and (to_int(row.get("ts_ms")) or 0) <= end_ms
    ]
    clean = [value for value in values if value is not None]
    return min(clean) if clean else None


def active_point_change(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None:
        return None
    return current - prior


def scale_decimal_pct(value: float | None) -> float | None:
    return value * 100.0 if value is not None else None


def pct_change_number(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None or prior == 0:
        return None
    return (current / prior - 1.0) * 100.0


def mark_index_basis_pct(mark_price: float | None, index_price: float | None) -> float | None:
    return pct_change_number(mark_price, index_price)


def sum_recent_funding(rows: list[dict[str, Any]], start_ms: int, end_ms: int) -> float | None:
    values = [
        to_optional_float(row.get("funding_rate"))
        for row in rows or []
        if (to_int(row.get("ts_ms")) or 0) >= start_ms and (to_int(row.get("ts_ms")) or 0) <= end_ms
    ]
    clean = [value for value in values if value is not None]
    return sum(clean) if clean else None


def active_latest_volume_z(rows: list[dict[str, Any]], ts_ms: int, *, lookback_rows: int) -> float | None:
    ordered = sorted(
        (row for row in rows or [] if (to_int(row.get("ts_ms")) or 0) <= ts_ms),
        key=lambda item: to_int(item.get("ts_ms")) or 0,
    )
    if len(ordered) < 3:
        return None
    current_volume = to_optional_float(ordered[-1].get("volume"))
    history = [to_optional_float(row.get("volume")) for row in ordered[-lookback_rows - 1 : -1]]
    clean = [value for value in history if value is not None]
    if current_volume is None or len(clean) < 3:
        return None
    mean = sum(clean) / len(clean)
    variance = sum((value - mean) ** 2 for value in clean) / len(clean)
    std = variance ** 0.5
    if std <= 0:
        return None
    return (current_volume - mean) / std


def build_pump_dashboard_state(
    shadow_status: dict[str, Any],
    schedule_status: dict[str, Any] | None = None,
    *,
    capital_usd: float = PUMP_DASHBOARD_CAPITAL_USD,
    max_active_coins: int = PUMP_DASHBOARD_MAX_ACTIVE_COINS,
    leverage: float = PUMP_DASHBOARD_LEVERAGE,
    ladder_legs: int = PUMP_DASHBOARD_LADDER_LEGS,
    ladder_step_pct: float = PUMP_DASHBOARD_LADDER_STEP_PCT,
) -> dict[str, Any]:
    schedule_status = schedule_status or {}
    paper = shadow_status.get("paper") if isinstance(shadow_status.get("paper"), dict) else {}
    positions = list(paper.get("positions") or [])
    open_positions = [position for position in positions if position.get("status") == "open"]
    closed_positions = [position for position in positions if position.get("status") == "closed"]
    per_coin_capital = capital_usd / max(1, max_active_coins)
    per_step_margin = per_coin_capital / max(1, ladder_legs)
    per_step_notional = per_step_margin * leverage

    enriched_open = [
        enrich_pump_dashboard_position(
            position,
            capital_usd=capital_usd,
            max_active_coins=max_active_coins,
            leverage=leverage,
            ladder_legs=ladder_legs,
            ladder_step_pct=ladder_step_pct,
        )
        for position in open_positions
    ]
    enriched_closed = [
        enrich_pump_dashboard_position(
            position,
            capital_usd=capital_usd,
            max_active_coins=max_active_coins,
            leverage=leverage,
            ladder_legs=ladder_legs,
            ladder_step_pct=ladder_step_pct,
        )
        for position in closed_positions[:20]
    ]
    rows = list(shadow_status.get("latest_rows") or [])
    candidates = [row for row in rows if row.get("status") == "entry_candidate"]
    watchlist = [row for row in rows if str(row.get("status") or "").startswith("watch")]
    blocked = [row for row in rows if str(row.get("status") or "").startswith("blocked")]

    used_margin = sum(to_number(position.get("used_margin_usd")) for position in enriched_open)
    current_topup = sum(to_number(position.get("current_topup_needed_usd")) for position in enriched_open)
    peak_topup = sum(to_number(position.get("peak_topup_needed_usd")) for position in enriched_open)
    unrealized_pnl = sum(to_number(position.get("current_unrealized_pnl_usd")) for position in enriched_open)
    realized_pnl = sum(to_number(position.get("realized_pnl_usd")) for position in enriched_open)
    active_count = len(enriched_open)
    free_slots = max(0, max_active_coins - active_count)
    usable_capital_left = capital_usd - used_margin - current_topup
    severity = "ok"
    if active_count > max_active_coins or current_topup > 0:
        severity = "danger"
    elif free_slots <= 0 or peak_topup > 0:
        severity = "warning"

    return {
        "schema": "pump_short_operator_dashboard_v1",
        "strategy": {
            "venue": "bybit",
            "mode": "shadow",
            "entry": "pb20",
            "funding_window_h": PUMP_DASHBOARD_FUNDING_WINDOW_H,
            "funding_min_pct": PUMP_DASHBOARD_FUNDING_MIN_PCT,
            "ladder_legs": ladder_legs,
            "ladder_step_pct": ladder_step_pct,
            "tp_pct": PUMP_DASHBOARD_TP_PCT,
            "max_hold_h": PUMP_DASHBOARD_MAX_HOLD_H,
            "leverage": leverage,
            "max_active_coins": max_active_coins,
        },
        "capital": {
            "initial_capital_usd": round(capital_usd, 6),
            "max_active_coins": max_active_coins,
            "active_open_positions": active_count,
            "free_slots": free_slots,
            "over_capacity": max(0, active_count - max_active_coins),
            "per_coin_capital_usd": round(per_coin_capital, 6),
            "per_step_margin_usd": round(per_step_margin, 6),
            "per_step_notional_usd": round(per_step_notional, 6),
            "planned_notional_per_coin_usd": round(per_step_notional * ladder_legs, 6),
            "used_margin_usd": round(used_margin, 6),
            "usable_capital_left_usd": round(usable_capital_left, 6),
            "current_topup_needed_usd": round(current_topup, 6),
            "peak_topup_needed_usd": round(peak_topup, 6),
            "current_unrealized_pnl_usd": round(unrealized_pnl, 6),
            "realized_pnl_usd": round(realized_pnl, 6),
            "severity": severity,
        },
        "shadow": {
            "status": shadow_status.get("status") or "idle",
            "started_at_ms": shadow_status.get("started_at_ms"),
            "updated_at_ms": shadow_status.get("updated_at_ms"),
            "finished_at_ms": shadow_status.get("finished_at_ms"),
            "last_event": shadow_status.get("last_event"),
            "last_error": shadow_status.get("last_error"),
            "metadata": shadow_status.get("metadata") or {},
            "files": shadow_status.get("files") or {},
        },
        "schedule": schedule_status,
        "positions": {
            "open": enriched_open,
            "recent_closed": enriched_closed,
            "open_count": len(enriched_open),
            "closed_count": len(closed_positions),
        },
        "signals": {
            "entry_candidates": candidates[:20],
            "watchlist": watchlist[:20],
            "blocked": blocked[:20],
            "latest_rows": rows[:50],
        },
    }


def build_pump_strategy_monitor_state(
    shadow_status: dict[str, Any],
    schedule_status: dict[str, Any] | None = None,
    *,
    catalog: Iterable[dict[str, Any]] = PUMP_STRATEGY_CATALOG,
) -> dict[str, Any]:
    schedule_status = schedule_status or {}
    rows = list(shadow_status.get("latest_rows") or [])
    paper = shadow_status.get("paper") if isinstance(shadow_status.get("paper"), dict) else {}
    legacy_positions = list(paper.get("positions") or [])
    strategy_paper = shadow_status.get("strategy_paper") if isinstance(shadow_status.get("strategy_paper"), dict) else {}
    positions = list(strategy_paper.get("positions") or [])
    strategy_summaries = strategy_paper.get("strategy_summaries") if isinstance(strategy_paper.get("strategy_summaries"), dict) else {}
    cycle_paper = shadow_status.get("cycle_paper") if isinstance(shadow_status.get("cycle_paper"), dict) else {}
    active_window = shadow_status.get("active_window") if isinstance(shadow_status.get("active_window"), dict) else {}
    slow_pump_watch_rows = [
        row
        for row in shadow_status.get("slow_pump_watch_rows") or []
        if isinstance(row, dict)
    ]
    legacy_open = [
        enrich_pump_dashboard_position(
            position,
            capital_usd=PUMP_DASHBOARD_CAPITAL_USD,
            max_active_coins=PUMP_DASHBOARD_MAX_ACTIVE_COINS,
            leverage=PUMP_DASHBOARD_LEVERAGE,
            ladder_legs=PUMP_DASHBOARD_LADDER_LEGS,
            ladder_step_pct=PUMP_DASHBOARD_LADDER_STEP_PCT,
        )
        for position in legacy_positions
        if position.get("status") == "open" and not position.get("strategy_id")
    ]
    legacy_closed = [
        enrich_pump_dashboard_position(
            position,
            capital_usd=PUMP_DASHBOARD_CAPITAL_USD,
            max_active_coins=PUMP_DASHBOARD_MAX_ACTIVE_COINS,
            leverage=PUMP_DASHBOARD_LEVERAGE,
            ladder_legs=PUMP_DASHBOARD_LADDER_LEGS,
            ladder_step_pct=PUMP_DASHBOARD_LADDER_STEP_PCT,
        )
        for position in legacy_positions
        if position.get("status") == "closed" and not position.get("strategy_id")
    ][:20]

    strategies: list[dict[str, Any]] = []
    for strategy in catalog:
        strategy_id = str(strategy.get("strategy_id") or "")
        strategy_positions = [
            position for position in positions
            if position.get("strategy_id") == strategy_id and position.get("status") == "open"
        ]
        decisions = [
            classify_strategy_signal(strategy, row)
            for row in rows
        ]
        decisions.sort(key=strategy_decision_sort_key)
        counts = count_strategy_decisions(decisions)
        active_count = len(strategy_positions)
        max_active = int(strategy.get("max_active_coins") or PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS)
        current_state = strategy_current_state(counts, active_count=active_count)
        capital = build_strategy_capital_payload(strategy, active_count=active_count)
        capital.update(strategy_summaries.get(strategy_id) or {})
        tiers = [enrich_strategy_tier(tier) for tier in strategy.get("tiers") or ()]
        strategies.append(
            {
                "strategy_id": strategy_id,
                "name": strategy.get("name") or strategy_id,
                "mode": strategy.get("mode") or "shadow",
                "notes": strategy.get("notes") or "",
                "current_state": current_state,
                "capital": capital,
                "filters": {
                    "funding_prev_24h_gt_pct": to_optional_float(strategy.get("funding_min_pct")),
                    "oi_change_24h_lte_pct": to_optional_float(strategy.get("oi_max_pct")),
                    "long_ratio_min": to_optional_float(strategy.get("long_ratio_min")),
                    "long_ratio_max": to_optional_float(strategy.get("long_ratio_max")),
                },
                "tiers": tiers,
                "positions": {
                    "open_count": active_count,
                    "free_slots": max(0, max_active - active_count),
                    "open": strategy_positions,
                    "recent_closed": [
                        position for position in positions
                        if position.get("strategy_id") == strategy_id and position.get("status") == "closed"
                    ][:20],
                },
                "signals": {
                    "counts": counts,
                    "ready": [item for item in decisions if item.get("state") == "entry_ready"][:12],
                    "watch": [
                        item for item in decisions
                        if str(item.get("state") or "").startswith("waiting")
                    ][:12],
                    "blocked": [
                        item for item in decisions
                        if str(item.get("state") or "").startswith("blocked")
                    ][:12],
                    "decisions": decisions[:30],
                },
            }
        )

    audit_key = strategy_monitor_audit_key(shadow_status, rows, strategies, paper)
    output_dir = Path(shadow_status.get("config", {}).get("output_dir") or DEFAULT_SHADOW_OUTPUT_DIR)
    return {
        "schema": "pump_short_strategy_monitor_v1",
        "shadow": {
            "status": shadow_status.get("status") or "idle",
            "started_at_ms": shadow_status.get("started_at_ms"),
            "updated_at_ms": shadow_status.get("updated_at_ms"),
            "finished_at_ms": shadow_status.get("finished_at_ms"),
            "last_event": shadow_status.get("last_event"),
            "last_error": shadow_status.get("last_error"),
            "metadata": shadow_status.get("metadata") or {},
            "rows_seen": len(rows),
            "files": shadow_status.get("files") or {},
        },
        "schedule": schedule_status,
        "strategies": strategies,
        "legacy_paper": {
            "schema": paper.get("schema") or "bybit_pump_short_paper_v1",
            "updated_at_ms": paper.get("updated_at_ms"),
            "open_count": len(legacy_open),
            "closed_count": int(paper.get("closed_positions") or 0),
            "open": legacy_open,
            "recent_closed": legacy_closed,
            "note": "Legacy paper positions do not have strategy_id yet; the new monitor logs per-strategy decisions separately.",
        },
        "strategy_paper": {
            "schema": strategy_paper.get("schema") or "pump_short_strategy_paper_v1",
            "updated_at_ms": strategy_paper.get("updated_at_ms"),
            "open_count": strategy_paper.get("open_positions") or 0,
            "closed_count": strategy_paper.get("closed_positions") or 0,
            "strategy_summaries": strategy_summaries,
            "events_latest": strategy_paper.get("events_latest") or [],
        },
        "slow_pump_watch": {
            "schema": "slow_pump_watch_v1",
            "mode": "research_only_no_trades",
            "recent_hours": SLOW_PUMP_WATCH_RECENT_HOURS,
            "configs": [
                {"window_h": window_h, "threshold_pct": threshold_pct}
                for window_h, threshold_pct in SLOW_PUMP_WATCH_CONFIGS
            ],
            "count": len(slow_pump_watch_rows),
            "rows": slow_pump_watch_rows,
        },
        "cycle_paper": {
            "schema": cycle_paper.get("schema") or "pump_cycle_paper_v1",
            "updated_at_ms": cycle_paper.get("updated_at_ms"),
            "config": cycle_paper.get("config") or {},
            "summary": cycle_paper.get("summary") or {},
            "track_summaries": cycle_paper.get("track_summaries") or [],
            "candidate_shadow": cycle_paper.get("candidate_shadow") or {},
            "candidate_paper": cycle_paper.get("candidate_paper") or {},
            "skip_summary": cycle_paper.get("skip_summary") or {},
            "open_count": cycle_paper.get("open_positions") or 0,
            "closed_count": cycle_paper.get("closed_positions") or 0,
            "positions": cycle_paper.get("positions") or [],
            "events_latest": cycle_paper.get("events_latest") or [],
        },
        "active_window": {
            "schema": active_window.get("schema") or "pump_active_window_v1",
            "updated_at_ms": active_window.get("updated_at_ms"),
            "interval": active_window.get("interval"),
            "selected_symbols": active_window.get("selected_symbols") or 0,
            "symbols": active_window.get("symbols") or 0,
            "errors": active_window.get("errors") or 0,
            "requests_made": active_window.get("requests_made") or 0,
            "config": active_window.get("config") or {},
            "rows": active_window.get("rows") or [],
        },
        "latest_errors": shadow_status.get("latest_errors") or [],
        "audit": {
            "key": audit_key,
            "file": str(output_dir / PUMP_STRATEGY_MONITOR_AUDIT_FILE),
            "latest": read_latest_jsonl(output_dir / PUMP_STRATEGY_MONITOR_AUDIT_FILE, limit=20),
        },
    }


def classify_strategy_signal(strategy: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    symbol = str(row.get("symbol") or "")
    pump_pct = to_optional_float(row.get("trigger_pump_pct"))
    if pump_pct is None:
        pump_pct = to_optional_float(row.get("high_from_trigger_pct"))
    tier = select_strategy_tier(strategy, pump_pct)
    enriched_tier = enrich_strategy_tier(tier) if tier else {}
    pullback = to_optional_float(row.get("pullback_from_high_pct"))
    funding = to_optional_float(row.get("funding_prev_24h_pct"))
    oi_change = to_optional_float(row.get("oi_change_24h_pct"))
    long_ratio = to_optional_float(row.get("long_ratio"))
    source_status = str(row.get("status") or "")
    state = "waiting_pump"
    reason = "no_recent_pump"

    if source_status == "watch_slow_pump":
        state = "waiting_pump"
        reason = "research_only_slow_pump"
    elif source_status in {"no_data", "no_recent_pump"}:
        state = "waiting_pump"
        reason = source_status or "no_recent_pump"
    elif tier is None:
        state = "waiting_pump"
        reason = "pump_strength_missing"
    elif pullback is None:
        state = "waiting_pullback"
        reason = "missing_pullback"
    elif pullback < float(enriched_tier.get("pullback_pct") or 0.0):
        state = "waiting_pullback"
        reason = f"pullback_lt_pb{int(float(enriched_tier.get('pullback_pct') or 0.0))}"
    else:
        funding_min = to_optional_float(strategy.get("funding_min_pct"))
        oi_max = to_optional_float(strategy.get("oi_max_pct"))
        lr_min = to_optional_float(strategy.get("long_ratio_min"))
        lr_max = to_optional_float(strategy.get("long_ratio_max"))
        if funding is not None and funding_min is not None and funding <= funding_min:
            state = "blocked_funding"
            reason = "funding_too_negative"
        elif oi_change is None:
            state = "waiting_oi"
            reason = "missing_oi"
        elif oi_max is not None and oi_change > oi_max:
            state = "blocked_oi"
            reason = "oi_change_above_max"
        elif long_ratio is None:
            state = "waiting_ratio"
            reason = "missing_long_ratio"
        elif lr_min is not None and lr_max is not None and not (lr_min <= long_ratio <= lr_max):
            state = "waiting_ratio"
            reason = "long_ratio_outside_band"
        else:
            state = "entry_ready"
            reason = "strategy_conditions_met"

    return {
        "strategy_id": strategy.get("strategy_id"),
        "symbol": symbol,
        "event_id": row.get("event_id"),
        "source_status": source_status,
        "source_reason": row.get("reason"),
        "state": state,
        "reason": reason,
        "ts_ms": row.get("ts_ms"),
        "pump_pct": round_optional(pump_pct, 3),
        "pullback_from_high_pct": round_optional(pullback, 3),
        "funding_prev_24h_pct": round_optional(funding, 6),
        "oi_change_24h_pct": round_optional(oi_change, 3),
        "long_ratio": round_optional(long_ratio, 6),
        "last_close": row.get("last_close"),
        "hours_since_trigger": row.get("hours_since_trigger"),
        "tier": enriched_tier,
    }


def select_strategy_tier(strategy: dict[str, Any], pump_pct: float | None) -> dict[str, Any] | None:
    if pump_pct is None:
        return None
    tiers = list(strategy.get("tiers") or [])
    if not tiers:
        return None
    eligible = [
        tier for tier in tiers
        if pump_pct >= float(tier.get("min_pump_pct") or 0.0)
    ]
    return max(eligible, key=lambda item: float(item.get("min_pump_pct") or 0.0)) if eligible else None


def enrich_strategy_tier(tier: dict[str, Any]) -> dict[str, Any]:
    slug = str(tier.get("rule_slug") or "")
    parsed = parse_strategy_rule_slug(slug)
    pullback = pullback_threshold_from_entry(str(tier.get("entry") or ""))
    return {
        **tier,
        "pullback_pct": pullback,
        "ladder_step_pct": parsed["ladder_step_pct"],
        "ladder_legs": parsed["ladder_legs"],
        "sizing": parsed["sizing"],
        "tp_pct": parsed["tp_pct"],
        "max_hold_h": parsed["max_hold_h"],
        "leg_weights": parsed["leg_weights"],
    }


def parse_strategy_rule_slug(slug: str) -> dict[str, Any]:
    match = re.search(r"step(\d+(?:\.\d+)?)_legs(\d+)_(equal|tapered)_tp(\d+(?:\.\d+)?)_(\d+)", slug)
    if not match:
        return {
            "ladder_step_pct": 50.0,
            "ladder_legs": 4,
            "sizing": "equal",
            "tp_pct": 25.0,
            "max_hold_h": 168,
            "leg_weights": [1.0, 1.0, 1.0, 1.0],
        }
    step_pct = float(match.group(1))
    legs = int(match.group(2))
    sizing = match.group(3)
    weights = [float(index + 1) for index in range(legs)] if sizing == "tapered" else [1.0 for _ in range(legs)]
    return {
        "ladder_step_pct": step_pct,
        "ladder_legs": legs,
        "sizing": sizing,
        "tp_pct": float(match.group(4)),
        "max_hold_h": int(match.group(5)),
        "leg_weights": weights,
    }


def pullback_threshold_from_entry(entry: str) -> float:
    match = re.search(r"pb(\d+(?:\.\d+)?)", entry)
    return float(match.group(1)) if match else 20.0


def build_strategy_capital_payload(strategy: dict[str, Any], *, active_count: int) -> dict[str, Any]:
    capital = float(strategy.get("capital_usd") or PUMP_STRATEGY_MONITOR_CAPITAL_USD)
    max_active = int(strategy.get("max_active_coins") or PUMP_STRATEGY_MONITOR_MAX_ACTIVE_COINS)
    leverage = float(strategy.get("leverage") or PUMP_STRATEGY_MONITOR_LEVERAGE)
    per_coin = capital / max(1, max_active)
    first_tier = enrich_strategy_tier(list(strategy.get("tiers") or [{}])[0])
    weights = [float(item) for item in first_tier.get("leg_weights") or [1.0]]
    weight_sum = sum(weights) or 1.0
    unit_margin = per_coin / weight_sum
    first_step_margin = unit_margin * weights[0]
    last_step_margin = unit_margin * weights[-1]
    return {
        "initial_capital_usd": round(capital, 6),
        "max_active_coins": max_active,
        "active_open_positions": active_count,
        "free_slots": max(0, max_active - active_count),
        "per_coin_capital_usd": round(per_coin, 6),
        "leverage": leverage,
        "first_tier_step_margin_usd": round(first_step_margin, 6),
        "first_tier_last_step_margin_usd": round(last_step_margin, 6),
        "first_tier_step_notional_usd": round(first_step_margin * leverage, 6),
        "planned_notional_per_coin_usd": round(per_coin * leverage, 6),
    }


def count_strategy_decisions(decisions: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "entry_ready": 0,
        "waiting_pullback": 0,
        "waiting_oi": 0,
        "waiting_ratio": 0,
        "waiting_pump": 0,
        "blocked": 0,
        "total": len(decisions),
    }
    for decision in decisions:
        state = str(decision.get("state") or "")
        if state in counts:
            counts[state] += 1
        elif state.startswith("blocked"):
            counts["blocked"] += 1
    return counts


def strategy_current_state(counts: dict[str, int], *, active_count: int) -> dict[str, Any]:
    if active_count > 0:
        return {"state": "waiting_take_profit", "label": "Position open; waiting TP or time-stop"}
    if counts.get("entry_ready"):
        return {"state": "entry_ready", "label": "Entry candidate ready"}
    if counts.get("waiting_pullback"):
        return {"state": "waiting_pullback", "label": "Pump found; waiting required pullback"}
    if counts.get("waiting_oi") or counts.get("waiting_ratio"):
        return {"state": "waiting_confirmation", "label": "Waiting OI or long-ratio confirmation"}
    return {"state": "waiting_pump", "label": "Waiting for a qualified pump"}


def strategy_decision_sort_key(decision: dict[str, Any]) -> tuple[int, float, float, str]:
    state_order = {
        "entry_ready": 0,
        "waiting_pullback": 1,
        "waiting_oi": 2,
        "waiting_ratio": 3,
        "blocked_funding": 4,
        "blocked_oi": 4,
        "waiting_pump": 5,
    }
    return (
        state_order.get(str(decision.get("state") or ""), 9),
        -to_number(decision.get("pump_pct")),
        -to_number(decision.get("pullback_from_high_pct")),
        str(decision.get("symbol") or ""),
    )


def strategy_monitor_audit_key(
    shadow_status: dict[str, Any],
    rows: list[dict[str, Any]],
    strategies: list[dict[str, Any]],
    paper: dict[str, Any],
) -> str:
    metadata = shadow_status.get("metadata") if isinstance(shadow_status.get("metadata"), dict) else {}
    scan_ts = metadata.get("ts_ms") or shadow_status.get("finished_at_ms") or shadow_status.get("updated_at_ms") or "idle"
    ready_parts = [
        f"{item.get('strategy_id')}:{(item.get('signals') or {}).get('counts', {}).get('entry_ready', 0)}"
        for item in strategies
    ]
    return "|".join(
        [
            str(scan_ts),
            str(len(rows)),
            str(paper.get("updated_at_ms") or ""),
            ",".join(ready_parts),
        ]
    )


def enrich_pump_dashboard_position(
    position: dict[str, Any],
    *,
    capital_usd: float,
    max_active_coins: int,
    leverage: float,
    ladder_legs: int,
    ladder_step_pct: float,
) -> dict[str, Any]:
    per_coin_capital = capital_usd / max(1, max_active_coins)
    per_step_margin = per_coin_capital / max(1, ladder_legs)
    per_step_notional = per_step_margin * leverage
    entry_price = to_number(position.get("entry_price"))
    current_price = to_number(position.get("current_price"))
    mae_pct = max(0.0, to_number(position.get("mae_pct")))
    filled_steps = filled_ladder_steps(mae_pct, ladder_legs=ladder_legs, ladder_step_pct=ladder_step_pct)
    ladder = build_ladder_rows(
        entry_price,
        current_price,
        filled_steps=filled_steps,
        ladder_legs=ladder_legs,
        ladder_step_pct=ladder_step_pct,
        per_step_margin=per_step_margin,
        per_step_notional=per_step_notional,
    )
    filled_prices = [to_number(row.get("price")) for row in ladder if row.get("filled")]
    avg_entry = sum(filled_prices) / len(filled_prices) if filled_prices else entry_price
    gross_notional = filled_steps * per_step_notional
    used_margin = filled_steps * per_step_margin
    current_pnl_pct = short_pnl_pct(avg_entry, current_price)
    current_unrealized_pnl = gross_notional * current_pnl_pct / 100.0
    current_unrealized_loss = max(0.0, -current_unrealized_pnl)
    current_topup = max(0.0, current_unrealized_loss - per_coin_capital)
    peak_loss = max(0.0, mae_pct / 100.0 * gross_notional)
    peak_topup = max(0.0, peak_loss - per_coin_capital)
    realized_pnl = per_step_notional * max(1, filled_steps) * to_number(position.get("realized_net_pct")) / 100.0
    opened_at = to_int(position.get("opened_at_ms"))
    updated_at = to_int(position.get("updated_at_ms"))
    time_in_trade_h = ((updated_at - opened_at) / 3_600_000.0) if opened_at and updated_at and updated_at >= opened_at else None
    max_hold_h = exit_max_hold_h(str(position.get("exit_strategy") or "")) or PUMP_DASHBOARD_MAX_HOLD_H
    hours_left = max(0.0, max_hold_h - time_in_trade_h) if time_in_trade_h is not None else None
    tp_pct = exit_tp_pct(str(position.get("exit_strategy") or "")) or PUMP_DASHBOARD_TP_PCT
    target_price = avg_entry * (1.0 - tp_pct / 100.0) if avg_entry > 0 else None
    last_snapshot = position.get("last_snapshot") if isinstance(position.get("last_snapshot"), dict) else {}
    return {
        **position,
        "capital_model": "1000_usd_3_slots_4_steps_3x",
        "per_coin_capital_usd": round(per_coin_capital, 6),
        "per_step_margin_usd": round(per_step_margin, 6),
        "per_step_notional_usd": round(per_step_notional, 6),
        "planned_steps": ladder_legs,
        "filled_steps": filled_steps,
        "remaining_steps": max(0, ladder_legs - filled_steps),
        "ladder": ladder,
        "avg_entry_price": round(avg_entry, 10) if avg_entry else None,
        "target_price": round(target_price, 10) if target_price else None,
        "tp_pct": tp_pct,
        "gross_notional_usd": round(gross_notional, 6),
        "used_margin_usd": round(used_margin, 6),
        "current_pnl_pct": round(current_pnl_pct, 6),
        "current_unrealized_pnl_usd": round(current_unrealized_pnl, 6),
        "current_unrealized_loss_usd": round(current_unrealized_loss, 6),
        "current_topup_needed_usd": round(current_topup, 6),
        "peak_loss_usd": round(peak_loss, 6),
        "peak_topup_needed_usd": round(peak_topup, 6),
        "realized_pnl_usd": round(realized_pnl, 6),
        "time_in_trade_h": round(time_in_trade_h, 3) if time_in_trade_h is not None else None,
        "max_hold_h": max_hold_h,
        "hours_left_h": round(hours_left, 3) if hours_left is not None else None,
        "funding_prev_24h_pct": to_optional_float(last_snapshot.get("funding_prev_24h_pct")),
        "oi_change_24h_pct": to_optional_float(last_snapshot.get("oi_change_24h_pct")),
        "pullback_from_high_pct": to_optional_float(last_snapshot.get("pullback_from_high_pct")),
        "requires_topup": current_topup > 0.0,
        "had_peak_topup": peak_topup > 0.0,
    }


def filled_ladder_steps(mae_pct: float, *, ladder_legs: int, ladder_step_pct: float) -> int:
    if ladder_legs <= 0:
        return 0
    steps = 1 + int(max(0.0, mae_pct) // max(0.000001, ladder_step_pct))
    return max(1, min(ladder_legs, steps))


def build_ladder_rows(
    entry_price: float,
    current_price: float,
    *,
    filled_steps: int,
    ladder_legs: int,
    ladder_step_pct: float,
    per_step_margin: float,
    per_step_notional: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index in range(ladder_legs):
        price = entry_price * (1.0 + ladder_step_pct / 100.0 * index) if entry_price > 0 else 0.0
        rows.append(
            {
                "step": index + 1,
                "price": round(price, 10) if price else None,
                "filled": index < filled_steps,
                "current_reached": bool(current_price and price and current_price >= price),
                "margin_usd": round(per_step_margin, 6),
                "notional_usd": round(per_step_notional, 6),
            }
        )
    return rows


def short_pnl_pct(entry_price: float, current_price: float) -> float:
    if entry_price <= 0 or current_price <= 0:
        return 0.0
    return (1.0 - current_price / entry_price) * 100.0


def long_pnl_pct(entry_price: float, current_price: float) -> float:
    if entry_price <= 0 or current_price <= 0:
        return 0.0
    return (current_price / entry_price - 1.0) * 100.0


def exit_tp_pct(exit_strategy: str) -> float | None:
    match = re.search(r"tp(\d+(?:\.\d+)?)", exit_strategy)
    return float(match.group(1)) if match else None


def exit_max_hold_h(exit_strategy: str) -> int | None:
    match = re.search(r"_(\d+)h?$", exit_strategy)
    return int(match.group(1)) if match else None


def to_number(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return out if out == out and out not in {float("inf"), float("-inf")} else 0.0


def to_optional_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and out not in {float("inf"), float("-inf")} else None


def to_int(value: Any) -> int | None:
    try:
        out = int(float(value))
    except (TypeError, ValueError):
        return None
    return out


def round_optional(value: float | None, digits: int) -> float | None:
    return round(value, digits) if value is not None else None


def append_jsonl_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")


def normalize_run_config(
    *,
    output_dir: str | None = None,
    lookback_days: int | None = None,
    sleep_sec: float | None = None,
    max_symbols: int | None = None,
    symbols: Iterable[str] | None = None,
    newest_first: bool | None = None,
    resume: bool | None = None,
) -> BybitPumpShortRunConfig:
    clean_symbols = [normalize_symbol(item) for item in symbols or [] if normalize_symbol(item)]
    return BybitPumpShortRunConfig(
        output_dir=Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR,
        lookback_days=min(max(int(lookback_days or 30), 1), 90),
        sleep_sec=min(max(float(sleep_sec if sleep_sec is not None else 0.8), 0.1), 10.0),
        max_symbols=max_symbols if max_symbols is None else max(1, int(max_symbols)),
        symbols=clean_symbols,
        newest_first=True if newest_first is None else bool(newest_first),
        resume=True if resume is None else bool(resume),
    )


def normalize_shadow_config(
    *,
    output_dir: str | None = None,
    lookback_days: int | None = None,
    sleep_sec: float | None = None,
    max_symbols: int | None = None,
    symbols: Iterable[str] | None = None,
    newest_first: bool | None = None,
    recent_event_hours: int | None = None,
) -> BybitPumpShortShadowConfig:
    clean_symbols = [normalize_symbol(item) for item in symbols or [] if normalize_symbol(item)]
    return BybitPumpShortShadowConfig(
        output_dir=Path(output_dir) if output_dir else DEFAULT_SHADOW_OUTPUT_DIR,
        lookback_days=min(max(int(lookback_days or 14), 2), 30),
        sleep_sec=min(max(float(sleep_sec if sleep_sec is not None else 0.8), 0.1), 10.0),
        max_symbols=max_symbols if max_symbols is None else max(1, int(max_symbols)),
        symbols=clean_symbols,
        newest_first=True if newest_first is None else bool(newest_first),
        recent_event_hours=min(max(int(recent_event_hours or 168), 24), 720),
    )


def normalize_shadow_schedule_config(
    *,
    output_dir: str | None = None,
    lookback_days: int | None = None,
    sleep_sec: float | None = None,
    max_symbols: int | None = None,
    symbols: Iterable[str] | None = None,
    newest_first: bool | None = None,
    recent_event_hours: int | None = None,
    interval_sec: int | None = None,
    run_immediately: bool | None = None,
    max_runs: int | None = None,
) -> BybitPumpShortShadowScheduleConfig:
    clean_symbols = [normalize_symbol(item) for item in symbols or [] if normalize_symbol(item)]
    return BybitPumpShortShadowScheduleConfig(
        output_dir=Path(output_dir) if output_dir else DEFAULT_SHADOW_OUTPUT_DIR,
        lookback_days=min(max(int(lookback_days or 14), 2), 30),
        sleep_sec=min(max(float(sleep_sec if sleep_sec is not None else 0.8), 0.1), 10.0),
        max_symbols=max_symbols if max_symbols is None else max(1, int(max_symbols)),
        symbols=clean_symbols,
        newest_first=True if newest_first is None else bool(newest_first),
        recent_event_hours=min(max(int(recent_event_hours or 168), 24), 720),
        interval_sec=min(max(int(interval_sec or 3600), 1), 86400),
        run_immediately=True if run_immediately is None else bool(run_immediately),
        max_runs=max_runs if max_runs is None else max(1, int(max_runs)),
    )
