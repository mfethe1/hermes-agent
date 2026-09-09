"""Single attempt, process-local pilot boundary; no production registration."""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import sys

from response_verification.core import evaluate_completion
from response_verification.pilot_wire import LIMIT, decode_envelope, decode_receipt

COMPLETE = "COMPLETE: Verified isolated artifact state."
DENIED = "INCOMPLETE: Isolated artifact state could not be verified."


async def settle_owned(task):
    """Join retained work despite repeated caller cancellation; report interruption."""
    joined = asyncio.gather(task, return_exceptions=True)
    interrupted = False
    while True:
        try:
            return (await asyncio.shield(joined))[0], interrupted
        except asyncio.CancelledError:
            interrupted = True


class PilotAttempt:
    def __init__(self, context, artifact_path, *, timeout=2.0, command=None):
        self.context = context
        self.artifact_path = str(artifact_path)
        self.timeout = timeout
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
            context = asdict(self.context)
            context["evaluated_at"] = None
            context["receipt_max_age"] = self.context.receipt_max_age.total_seconds()
            context["trusted_issuers"] = sorted(self.context.trusted_issuers)
            job = json.dumps({
                "context": context,
                "artifact_path": self.artifact_path,
                "envelope": raw.decode(),
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
            proc = await asyncio.shield(launch)

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
                    _, interrupted = await settle_owned(exchange_task)
                if launch is not None and proc is None:
                    result, cancelled = await settle_owned(launch)
                    interrupted |= cancelled
                    if not isinstance(result, BaseException):
                        proc = result
                if proc is not None:

                    async def reap():
                        # Only the group created with start_new_session above.
                        try:
                            os.killpg(proc.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        await proc.wait()
                        self.process_returncode = proc.returncode

                    result, cancelled = await settle_owned(asyncio.create_task(reap()))
                    interrupted |= cancelled
                    if isinstance(result, BaseException):
                        raise result
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
        ):
            return DENIED
        try:
            current = replace(self.context, evaluated_at=datetime.now(timezone.utc))
            if evaluate_completion(current, self.envelope, (self.receipt,)).eligible:
                return COMPLETE
        except Exception:
            return DENIED
        return DENIED
