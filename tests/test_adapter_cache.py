from __future__ import annotations

import unittest

from exchanges import clear_adapter_cache, get_adapter_cached


class AdapterCacheTestCase(unittest.TestCase):
    def tearDown(self) -> None:
        clear_adapter_cache()

    def test_get_adapter_cached_returns_same_instance(self) -> None:
        one = get_adapter_cached("binance")
        two = get_adapter_cached("binance")
        self.assertIs(one, two)

    def test_clear_adapter_cache_resets_instance(self) -> None:
        one = get_adapter_cached("okx")
        clear_adapter_cache("okx")
        two = get_adapter_cached("okx")
        self.assertIsNot(one, two)


if __name__ == "__main__":
    unittest.main()
