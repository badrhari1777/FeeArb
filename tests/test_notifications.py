import unittest
from unittest.mock import AsyncMock

from utils.notifications import NotificationRouter


class NotificationRouterTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_fallback_channel_used_when_primary_skipped(self) -> None:
        router = NotificationRouter(primary_channel="pushbullet", fallback_channel="telegram")
        router._send_pushbullet_text_status = AsyncMock(return_value="skipped")  # type: ignore[attr-defined]
        router._send_telegram_text_status = AsyncMock(return_value="ok")  # type: ignore[attr-defined]

        status = await router.send_text_status("test message")

        self.assertEqual(status, "ok")
        router._send_pushbullet_text_status.assert_awaited_once()  # type: ignore[attr-defined]
        router._send_telegram_text_status.assert_awaited_once()  # type: ignore[attr-defined]

    async def test_http_error_bubbles_when_all_channels_fail(self) -> None:
        router = NotificationRouter(primary_channel="pushbullet", fallback_channel="telegram")
        router._send_pushbullet_text_status = AsyncMock(return_value="http_error")  # type: ignore[attr-defined]
        router._send_telegram_text_status = AsyncMock(return_value="skipped")  # type: ignore[attr-defined]

        status = await router.send_text_status("test message")

        self.assertEqual(status, "http_error")

    async def test_same_primary_and_fallback_collapses_to_single_channel(self) -> None:
        router = NotificationRouter(primary_channel="telegram", fallback_channel="telegram")
        router._send_telegram_text_status = AsyncMock(return_value="ok")  # type: ignore[attr-defined]

        status = await router.send_text_status("test message")

        self.assertEqual(status, "ok")
        router._send_telegram_text_status.assert_awaited_once()  # type: ignore[attr-defined]

    async def test_ntfy_primary_channel_is_supported(self) -> None:
        router = NotificationRouter(primary_channel="ntfy", fallback_channel="telegram")
        router._send_ntfy_text_status = AsyncMock(return_value="ok")  # type: ignore[attr-defined]
        router._send_telegram_text_status = AsyncMock(return_value="ok")  # type: ignore[attr-defined]

        status = await router.send_text_status("test message", title="FeeArb test")

        self.assertEqual(status, "ok")
        router._send_ntfy_text_status.assert_awaited_once()  # type: ignore[attr-defined]
        router._send_telegram_text_status.assert_not_awaited()  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
