"""
Scheduler module for automated scraping.

FIX 2026-01: Added ScanJobGuard and StuckScanMonitor for preventing silent death of scans.
"""
from .jobs import setup_scheduler, shutdown_scheduler, scheduler
from .scan_guard import ScanJobGuard, guarded_scan
from .stuck_monitor import start_stuck_monitor, stop_stuck_monitor, check_stuck_scans_now

__all__ = [
    "setup_scheduler",
    "shutdown_scheduler",
    "scheduler",
    # FIX 2026-01: Safety harness components
    "ScanJobGuard",
    "guarded_scan",
    "start_stuck_monitor",
    "stop_stuck_monitor",
    "check_stuck_scans_now",
]
