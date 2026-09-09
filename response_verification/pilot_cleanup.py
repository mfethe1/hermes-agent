"""Bounded POSIX cleanup for the private session created by the local pilot.

Group absence and direct-child reaping are separate observations. Neither proves
that descendants did not detach into another session before cleanup.
"""

import asyncio
from dataclasses import dataclass, field
import os
import signal

CLEANUP_TIMEOUT = 2.0


async def settle_owned(task, *, timeout=CLEANUP_TIMEOUT, cancel_on_timeout=True):
    """Join until a fixed deadline despite repeated cancellation of the caller."""
    deadline = asyncio.get_running_loop().time() + timeout
    interrupted = False
    while not task.done():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            if cancel_on_timeout:
                task.cancel()
            # Retrieve a late exception without pretending that the task joined.
            task.add_done_callback(
                lambda done: None if done.cancelled() else done.exception()
            )
            return TimeoutError("owned task join deadline"), interrupted
        try:
            await asyncio.wait({task}, timeout=remaining)
        except asyncio.CancelledError:
            interrupted = True
    try:
        return task.result(), interrupted
    except BaseException as exc:
        return exc, interrupted


@dataclass
class CleanupStatus:
    pid: int | None = None
    pgid: int | None = None
    child: str = "not-started"
    group: str = "unknown"
    returncode: int | None = None
    detached_descendants: str = "untracked"
    deadline_expired: bool = False
    observations: list[dict] = field(default_factory=list)

    @property
    def verified(self):
        return (
            self.child == "reaped"
            and self.group == "absent"
            and not self.deadline_expired
            and not any(item.get("error") for item in self.observations)
        )

    def observe(self, operation, proc, *, error=None, **fields):
        # No argv, exception message, environment, admission or output bytes.
        item = dict(
            operation=operation,
            pid=proc.pid,
            pgid=self.pgid,
            returncode=proc.returncode,
            **fields,
        )
        if error is not None:
            item.update(error=type(error).__name__, errno=getattr(error, "errno", None))
        self.observations.append(item)


async def cleanup_process(proc, status, *, timeout=CLEANUP_TIMEOUT):
    deadline = asyncio.get_running_loop().time() + timeout
    status.pid = status.pgid = proc.pid
    status.child = "unreaped"
    leader_owned = False
    # Never derive destructive targets from model data, process listings or argv.
    # The live direct child is the leader of our start_new_session group. Once
    # reaped, a still-present numeric group is ambiguous: do not kill by stale ID.
    if proc.returncode is None:
        try:
            pgid, sid = os.getpgid(proc.pid), os.getsid(proc.pid)
            leader_owned = pgid == sid == proc.pid and pgid != os.getpgrp()
            if leader_owned:
                status.group = "present"
            status.observe(
                "identity",
                proc,
                observed_pgid=pgid,
                observed_sid=sid,
                parent_pgid=os.getpgrp(),
                leader_owned=leader_owned,
            )
        except OSError as exc:
            status.observe("identity", proc, error=exc)
    if leader_owned:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
            status.observe("killpg", proc, signal=int(signal.SIGKILL))
        except ProcessLookupError as exc:
            status.observe("killpg-absent", proc, errno=exc.errno)
        except OSError as exc:
            status.observe("killpg", proc, error=exc, signal=int(signal.SIGKILL))
            # The Process handle is still our direct child, not evidence about
            # the rest of its group. Try it once, even after group denial.
            if proc.returncode is None:
                try:
                    proc.kill()
                    status.observe("kill-child", proc, signal=int(signal.SIGKILL))
                except ProcessLookupError as gone:
                    status.observe("kill-child-absent", proc, errno=gone.errno)
                except OSError as denied:
                    status.observe(
                        "kill-child", proc, error=denied, signal=int(signal.SIGKILL)
                    )

    # proc.wait() can wait for inherited pipe EOF even after the child is reaped.
    # Poll the event-loop child watcher's returncode and group existence instead;
    # then call wait only after a returncode has actually been observed. Observe
    # the group after reaping, allowing a transient probe denial to become a
    # measured absence within the same deadline; retain that denial regardless.
    probe_error = False
    while True:
        status.returncode = proc.returncode
        status.child = "reaped" if proc.returncode is not None else "unreaped"
        if status.child == "reaped" and status.group != "absent":
            try:
                os.killpg(proc.pid, 0)
                status.group = "present"
            except ProcessLookupError:
                status.group = "absent"
            except OSError as exc:
                status.group = "unknown"
                if not probe_error:
                    status.observe("probe-group", proc, error=exc, signal=0)
                    probe_error = True
        if status.child == "reaped" and status.group == "absent":
            break
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            status.deadline_expired = True
            break
        await asyncio.sleep(min(0.01, remaining))
    if status.child == "reaped":
        # A fresh wait after the watcher set returncode does not depend on pipes.
        result, _ = await settle_owned(
            asyncio.create_task(proc.wait()),
            timeout=0.1,
        )
        if isinstance(result, BaseException):
            status.observe("wait", proc, error=result)
    status.observe(
        "final",
        proc,
        child=status.child,
        group=status.group,
        deadline_expired=status.deadline_expired,
    )
