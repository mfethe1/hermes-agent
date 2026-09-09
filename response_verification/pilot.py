"""Single attempt, process-local pilot boundary; no production registration."""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import sys

from response_verification.core import evaluate_completion
from response_verification.admission import SnapshotAdmission, context_wire
from response_verification.pilot_wire import LIMIT, decode_envelope, decode_receipt
from response_verification.pilot_cleanup import (
    CLEANUP_TIMEOUT,
    CleanupStatus,
    cleanup_process,
    settle_owned,
)

COMPLETE = "COMPLETE: Verified admitted snapshot bytes only."
DENIED = "INCOMPLETE: Admitted snapshot could not be verified."


logger = logging.getLogger(__name__)


class PilotAttempt:
    def __init__(
        self,
        context,
        admission,
        *,
        timeout=2.0,
        command=None,
        cleanup_timeout=CLEANUP_TIMEOUT,
    ):
        for value, maximum in ((timeout, 60), (cleanup_timeout, CLEANUP_TIMEOUT)):
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or not 0 < value <= maximum
            ):
                raise ValueError("invalid bounded pilot timeout")
        if type(admission) is not SnapshotAdmission:
            raise ValueError("explicit snapshot admission required")
        admission.validate(context)
        self._admission = admission
        self._context = admission.context
        self.timeout = timeout
        self.cleanup_timeout = cleanup_timeout
        self.command = command or [
            sys.executable,
            "-I",
            str(Path(__file__).with_name("pilot_worker.py")),
            "verify",
        ]
        self.envelope = self.receipt = None
        self.cancelled = False
        self.drafts = bytearray()
        self.failed = False
        self.process_returncode = None
        self.terminal = False
        self._active_task = None
        self.cleanup = CleanupStatus()
        self._launch_task = self._cleanup_task = None

    async def _cleanup_process(self, proc):
        try:
            await cleanup_process(proc, self.cleanup, timeout=self.cleanup_timeout)
        finally:
            self.process_returncode = proc.returncode
            if not self.cleanup.verified:
                self.failed = True
                logger.warning(
                    "pilot cleanup incomplete: %s", json.dumps(asdict(self.cleanup))
                )

    def _late_launch(self, task):
        # A timed-out launch remains owned. A late handle gets bounded cleanup,
        # but can never restore this failed/cancelled attempt's eligibility.
        if task.cancelled():
            return
        try:
            proc = task.result()
        except Exception:
            return
        self._cleanup_task = asyncio.create_task(self._cleanup_process(proc))
        self._cleanup_task.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, value):
        # Any context replacement invalidates this attempt permanently, even on revert.
        self.failed = True
        self._context = value

    @property
    def admission(self):
        return self._admission

    def buffer(self, draft):
        if self.terminal:
            return
        if type(draft) is not str or len(draft) > LIMIT:
            self.failed = True
            return
        try:
            data = draft.encode()
        except UnicodeError:
            self.failed = True
            return
        if len(data) + len(self.drafts) > LIMIT:
            self.failed = True
            return
        self.drafts.extend(data)

    def close(self):
        self.terminal = True
        self.drafts.clear()

    def cancel(self):
        self.cancelled = True
        self.close()
        if self._active_task is not None and not self._active_task.done():
            self._active_task.cancel()

    async def cancel_and_join(self):
        active = self._active_task
        self.cancel()
        if active is not None and active is not asyncio.current_task():
            _, interrupted = await settle_owned(active)
            if not active.done():
                self.failed = True
            if interrupted:
                raise asyncio.CancelledError

    async def prepare(self, raw):
        if self.terminal:
            return
        if self._active_task is not None:
            raise ValueError("attempt already active")
        self._active_task = asyncio.current_task()
        proc = None
        launch = exchange_task = None
        try:
            self.envelope = decode_envelope(raw)
            self._admission.validate(self.context)
            job = json.dumps({
                "context": context_wire(self.context),
                "admission": self._admission.wire(),
                "envelope": raw.decode("utf-8"),
            }).encode()
            if len(job) > LIMIT:
                raise ValueError("job size")
            launch = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    *self.command,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                    start_new_session=True,
                )
            )
            self._launch_task = launch
            self.cleanup.child = "launch-pending"
            proc = await asyncio.wait_for(asyncio.shield(launch), self.timeout)

            async def exchange():
                proc.stdin.write(job)
                await proc.stdin.drain()
                proc.stdin.close()
                output = bytearray()
                while chunk := await proc.stdout.read(4096):
                    output.extend(chunk)
                    if len(output) > LIMIT:
                        raise ValueError("output size")
                if await proc.wait() != 0:
                    raise ValueError("verifier exit")
                return bytes(output)

            exchange_task = asyncio.create_task(exchange())
            output = await asyncio.wait_for(asyncio.shield(exchange_task), self.timeout)
            self.receipt = decode_receipt(output)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        except Exception:
            self.failed = True
        finally:
            # Terminal state and buffers do not depend on any interruptible await.
            self.close()
            interrupted = False
            try:
                if exchange_task is not None:
                    exchange_task.cancel()
                    result, interrupted = await settle_owned(exchange_task)
                    if isinstance(result, TimeoutError):
                        self.failed = True
                if launch is not None and proc is None:
                    result, cancelled = await settle_owned(
                        launch, cancel_on_timeout=False
                    )
                    interrupted |= cancelled
                    if not launch.done():
                        self.failed = True
                        self.cleanup.deadline_expired = True
                        launch.add_done_callback(self._late_launch)
                        logger.warning(
                            "pilot cleanup incomplete: %s",
                            json.dumps(asdict(self.cleanup)),
                        )
                    elif not isinstance(result, BaseException):
                        proc = result
                if proc is not None:
                    self._cleanup_task = asyncio.create_task(
                        self._cleanup_process(proc)
                    )
                    result, cancelled = await settle_owned(
                        self._cleanup_task, timeout=self.cleanup_timeout + 0.5
                    )
                    interrupted |= cancelled
                    if isinstance(result, BaseException):
                        self.failed = True
            finally:
                self._active_task = None
                self.close()
            if interrupted:
                self.cancelled = True
                raise asyncio.CancelledError

    def disposition(self):
        if (
            self.cancelled
            or self.failed
            or self.envelope is None
            or self.receipt is None
            or not self.cleanup.verified
        ):
            return DENIED
        try:
            self._admission.validate(self.context)
            current = replace(self.context, evaluated_at=datetime.now(timezone.utc))
            if evaluate_completion(current, self.envelope, (self.receipt,)).eligible:
                return COMPLETE
        except Exception:
            return DENIED
        return DENIED
