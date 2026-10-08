"""Bound extension calls even if an async implementation blocks or ignores cancel."""

import asyncio
import inspect
import threading
import time
from collections.abc import Callable
from contextvars import ContextVar, copy_context

WAKE_INTERVAL = 0.01
call_deadline = ContextVar("dowser_call_deadline", default=None)


async def worker_result(future, seconds):
    """Bound socket-independent wakeups as well as the extension's deadline.

    Some restricted environments reject asyncio's cross-thread self-pipe send.
    Callbacks remain queued, so a short timer lets the owner receive them without
    waiting for the entire extension deadline. Normal socket wakeups remain fast.
    """
    loop = asyncio.get_running_loop()
    deadline = None if seconds is None else loop.time() + seconds
    while True:
        remaining = None if deadline is None else deadline - loop.time()
        if remaining is not None and remaining <= 0:
            raise TimeoutError("extension call exceeded deadline")
        done, _ = await asyncio.wait(
            {future},
            timeout=WAKE_INTERVAL
            if remaining is None
            else min(WAKE_INTERVAL, remaining),
        )
        if done:
            return future.result()


async def bounded_call(fn: Callable, *args, seconds: float):
    """Run in a daemon worker with its own loop, never await cancellation cleanup.

    A timed-out operation may still have external effects. Callers must stop the
    incident and record unknown execution; this is a deadline, not isolation.
    """
    if seconds <= 0:
        raise TimeoutError("incident deadline exhausted")
    owner = asyncio.get_running_loop()
    inherited = call_deadline.get()
    invocation_deadline = time.monotonic() + seconds
    if inherited is not None:
        invocation_deadline = min(inherited, invocation_deadline)
    invocation_context = copy_context()
    invocation_context.run(call_deadline.set, invocation_deadline)
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
        invocation_context.run(asyncio.run, invoke())

    threading.Thread(target=run, daemon=True, name="dowser-extension").start()
    try:
        return await worker_result(future, seconds)
    finally:
        if not future.done():
            future.cancel()
            loop, task = worker.get("loop"), worker.get("task")
            if loop and task:
                try:
                    loop.call_soon_threadsafe(task.cancel)
                except RuntimeError:
                    pass


class PersistentWorker:
    """One daemon loop for an intake component's entire resource lifetime.

    Source pulls and checkpoints can overlap on this loop. Owner-side deadlines
    remain effective even when extension code blocks or ignores cancellation.
    """

    def __init__(self, name: str):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self._run, daemon=True, name=f"dowser-{name}"
        )
        self.thread.start()

    def _run(self):
        asyncio.set_event_loop(self.loop)

        def heartbeat():
            self.loop.call_later(WAKE_INTERVAL, heartbeat)

        heartbeat()
        try:
            self.loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            if pending:
                self.loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()

    async def call(self, fn: Callable, *args, seconds: float | None):
        if seconds is not None and seconds <= 0:
            raise TimeoutError("extension deadline exhausted")

        async def invoke():
            # SystemExit/KeyboardInterrupt must not kill the worker loop and
            # strand an idle source pull. Deliver all failures to the owner.
            try:
                value = fn(*args)
                value = await value if inspect.isawaitable(value) else value
                return value, None
            except BaseException as error:
                return None, error

        future = asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(invoke(), self.loop)
        )
        try:
            value, error = await worker_result(future, seconds)
            if error is not None:
                raise error
            return value
        finally:
            if not future.done():
                future.cancel()

    def stop(self):
        try:
            self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError:
            pass


class WorkerComponent:
    """Route intake service calls, including dependency calls, to their owner."""

    interface_version = "1"

    def __init__(self, worker: PersistentWorker, seconds: float):
        self.worker = worker
        self.seconds = seconds
        self.component = None

    async def construct(self, fn, settings, context):
        async def create():
            value = fn(settings, context)
            self.component = await value if inspect.isawaitable(value) else value
            return self.component

        return await self.worker.call(create, seconds=15)

    async def call(self, name, *args, seconds):
        return await self.worker.call(
            getattr(self.component, name), *args, seconds=seconds
        )

    def __getattr__(self, name):
        async def invoke(*args):
            return await self.call(name, *args, seconds=self.seconds)

        return invoke

    async def aclose(self):
        async def close_owned():
            current = asyncio.current_task()
            pending = asyncio.all_tasks() - {current}
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if self.component is not None:
                await self.component.aclose()

        try:
            await self.worker.call(close_owned, seconds=2)
        finally:
            self.worker.stop()
