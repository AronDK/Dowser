"""Extension deadlines and thread delivery when socket wakeups are unavailable."""

import asyncio
import time
import unittest
from asyncio.selector_events import BaseSelectorEventLoop
from unittest.mock import patch

from dowser.runtime import PersistentWorker, bounded_call


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_worker_does_not_wait_for_deadline_without_socket_wakeup(
        self,
    ):
        async def instant():
            return 42

        started = time.monotonic()
        with patch.object(BaseSelectorEventLoop, "_write_to_self", return_value=None):
            result = await bounded_call(instant, seconds=1)
        self.assertEqual(result, 42)
        self.assertLess(time.monotonic() - started, 0.5)

    async def test_idle_persistent_worker_receives_calls_without_socket_wakeup(self):
        async def instant():
            return "delivered"

        with patch.object(BaseSelectorEventLoop, "_write_to_self", return_value=None):
            worker = PersistentWorker("wake-test")
            try:
                await asyncio.sleep(0.1)
                started = time.monotonic()
                self.assertEqual(await worker.call(instant, seconds=1), "delivered")
                self.assertLess(time.monotonic() - started, 0.5)
                self.assertEqual(await worker.call(instant, seconds=None), "delivered")
            finally:
                worker.stop()
                worker.thread.join(0.5)
        self.assertFalse(worker.thread.is_alive())

    async def test_blocking_extensions_keep_deadlines_and_cancelled_owners_return(self):
        def blocking():
            time.sleep(0.3)

        with patch.object(BaseSelectorEventLoop, "_write_to_self", return_value=None):
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                await bounded_call(blocking, seconds=0.03)
            self.assertLess(time.monotonic() - started, 0.2)
            task = asyncio.create_task(bounded_call(blocking, seconds=5))
            await asyncio.sleep(0.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task


if __name__ == "__main__":
    unittest.main()
