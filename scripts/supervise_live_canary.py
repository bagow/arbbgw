#!/usr/bin/env python3
"""Signal-forwarding, atomic-output supervisor for a bounded live canary."""

import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys


def atomic_text(path: Path, value: str):
    temporary = path.with_name(path.name + ".write.tmp")
    temporary.write_text(value)
    os.replace(temporary, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--script", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--log", type=Path, required=True)
    args = parser.parse_args()

    script = args.script.resolve(strict=True)
    config = args.config.resolve(strict=True)
    result = args.result.resolve()
    log = args.log.resolve()
    run_dir = result.parent
    temporary = result.with_name(result.name + ".tmp")
    exit_path = result.with_name(result.name + ".exit")
    if result.exists() or temporary.exists() or exit_path.exists():
        parser.error("result, temporary result, and exit marker must not already exist")
    run_dir.mkdir(parents=True, exist_ok=True)

    child = None
    pending_signal = None

    def forward(signum, _frame):
        nonlocal pending_signal
        pending_signal = signum
        if child is not None and child.poll() is None:
            child.send_signal(signum)

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, forward)

    atomic_text(run_dir / "runner.pid", str(os.getpid()))
    with config.open("rb") as stdin, temporary.open("xb") as stdout, log.open("xb") as stderr:
        child = subprocess.Popen(
            [sys.executable, str(script)],
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            close_fds=True,
        )
        atomic_text(run_dir / "child.pid", str(child.pid))
        if pending_signal is not None and child.poll() is None:
            child.send_signal(pending_signal)
        return_code = child.wait()
        stdout.flush()
        os.fsync(stdout.fileno())

    if temporary.stat().st_size == 0:
        atomic_text(exit_path, str(return_code))
        raise SystemExit("canary child produced an empty result")
    os.replace(temporary, result)
    atomic_text(exit_path, str(return_code))
    raise SystemExit(return_code)


if __name__ == "__main__":
    main()
