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

    def buffer(self, draft):
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

    def cancel(self):
        self.cancelled = True

    async def prepare(self, raw):
        proc = None
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
            proc = await asyncio.create_subprocess_exec(
                *self.command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )

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

            output = await asyncio.wait_for(exchange(), self.timeout)
            self.receipt = decode_receipt(output)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        except Exception:
            self.failed = True
        finally:
            if proc is not None:
                # Kill the whole POSIX process group, including collector descendants.
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await proc.wait()
                self.process_returncode = proc.returncode
            self.drafts.clear()

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
