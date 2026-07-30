from __future__ import annotations

import os


# Must be set before test modules import webapp.app, whose production singleton
# otherwise restores the real Pump Live ledger and starts its recovery monitor.
os.environ.setdefault("FEEARB_TESTING", "1")
