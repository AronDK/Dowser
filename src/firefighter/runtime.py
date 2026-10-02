"""Bound extension calls even if an async implementation blocks or ignores cancel."""

import asyncio
import threading
from collections.abc import Callable


async def bounded_call(fn: Callable, *args, seconds: float):
    """Run in a daemon worker with its own loop, never await cancellation cleanup.

    A timed-out operation may still have external effects. Callers must stop the
    incident and record unknown execution; this is a deadline, not isolation.
    """
    if seconds <= 0:
        raise TimeoutError("incident deadline exhausted")
    owner = asyncio.get_running_loop()
    future = owner.create_future()
    worker: dict = {}

    def deliver(value=None, error=None):
        if future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(value)

    def notify(value=None, error=None):
        try:
            owner.call_soon_threadsafe(deliver, value, error)
        except RuntimeError:
            pass  # Owner already exited after a deadline/interruption.

    async def invoke():
        worker["loop"] = asyncio.get_running_loop()
        worker["task"] = asyncio.current_task()
        try:
            value = fn(*args)
            if hasattr(value, "__await__"):
                value = await value
            notify(value=value)
        except BaseException as error:
            notify(error=error)

    def run():
        asyncio.run(invoke())

    threading.Thread(target=run, daemon=True, name="firefighter-extension").start()
    try:
        done, _ = await asyncio.wait({future}, timeout=seconds)
        if not done:
            raise TimeoutError("extension call exceeded deadline")
        return future.result()
    finally:
        if not future.done():
            future.cancel()
            loop, task = worker.get("loop"), worker.get("task")
            if loop and task:
                try:
                    loop.call_soon_threadsafe(task.cancel)
                except RuntimeError:
                    pass
