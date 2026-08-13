from __future__ import annotations

import asyncio
from importlib import import_module
import unittest
from unittest.mock import AsyncMock, Mock, patch

webapp_app = import_module("webapp.app")


class AppLifespanTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_lifespan_owns_startup_and_orderly_shutdown(self) -> None:
        startup = AsyncMock(return_value=None)
        shutdown = AsyncMock(return_value=None)
        pump_shutdown = Mock(return_value=None)

        with (
            patch.object(webapp_app.service, "startup", startup),
            patch.object(webapp_app.service, "shutdown", shutdown),
            patch.object(webapp_app.bybit_pump_short_lab, "shutdown", pump_shutdown),
        ):
            async with webapp_app.app_lifespan(webapp_app.app):
                await asyncio.sleep(0)
                startup.assert_awaited_once_with()
                shutdown.assert_not_awaited()

        pump_shutdown.assert_called_once_with()
        shutdown.assert_awaited_once_with()

    async def test_lifespan_cancels_unfinished_startup_before_shutdown(self) -> None:
        startup_cancelled = asyncio.Event()

        async def _slow_startup() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                startup_cancelled.set()
                raise

        shutdown = AsyncMock(return_value=None)
        pump_shutdown = Mock(return_value=None)

        with (
            patch.object(webapp_app.service, "startup", _slow_startup),
            patch.object(webapp_app.service, "shutdown", shutdown),
            patch.object(webapp_app.bybit_pump_short_lab, "shutdown", pump_shutdown),
        ):
            async with webapp_app.app_lifespan(webapp_app.app):
                await asyncio.sleep(0)

        self.assertTrue(startup_cancelled.is_set())
        pump_shutdown.assert_called_once_with()
        shutdown.assert_awaited_once_with()
