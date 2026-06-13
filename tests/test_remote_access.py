from __future__ import annotations

import unittest

from webapp.remote_access import (
    has_valid_remote_token,
    is_cloudflare_request,
    is_public_proxy_request,
)


class RemoteAccessTestCase(unittest.TestCase):
    def test_cloudflare_request_detection(self) -> None:
        self.assertTrue(is_cloudflare_request({"cf-connecting-ip": "203.0.113.10"}))
        self.assertTrue(is_cloudflare_request({"cf-ray": "abc-FRA"}))
        self.assertFalse(is_cloudflare_request({"host": "127.0.0.1:8000"}))

    def test_remote_token_validation(self) -> None:
        self.assertTrue(
            has_valid_remote_token({"x-feearb-token": "secret"}, expected_token="secret")
        )
        self.assertFalse(
            has_valid_remote_token({"x-feearb-token": "wrong"}, expected_token="secret")
        )
        self.assertFalse(has_valid_remote_token({}, expected_token="secret"))

    def test_public_proxy_detection(self) -> None:
        self.assertTrue(
            is_public_proxy_request({"x-feearb-public-proxy": "tailscale-funnel"})
        )
        self.assertFalse(is_public_proxy_request({"x-feearb-public-proxy": "other"}))
        self.assertFalse(is_public_proxy_request({}))


if __name__ == "__main__":
    unittest.main()
