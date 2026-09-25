"""Copy a tested core binary with compression and verify it before installation."""
import gzip
import hashlib
import os
from pathlib import Path
import re
import subprocess
import time


sha = os.environ["GITHUB_SHA"]
if not re.fullmatch(r"[0-9a-f]{40}", sha):
    raise RuntimeError("Invalid deployment build identity")

source = Path("target/release/core-node")
if not source.is_file():
    raise RuntimeError("Tested core artifact is missing")
expected_digest = hashlib.sha256(source.read_bytes()).hexdigest()

directory = Path(os.environ["RUNNER_TEMP"]) / "arb-core-deploy-ssh"
directory.mkdir(mode=0o700)
key = directory / "key"
archive = directory / "core-node.gz"
fd = os.open(key, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w") as stream:
    stream.write(os.environ["ARB_DEPLOY_KEY"] + "\n")
with source.open("rb") as input_stream, gzip.open(archive, "wb", compresslevel=9) as output_stream:
    while chunk := input_stream.read(1024 * 1024):
        output_stream.write(chunk)

destination = os.environ["ARB_DEPLOY_USER"] + "@" + os.environ["ARB_DEPLOY_HOST"]
remote_directory = "/tmp/arb-core-" + sha
remote_upload = remote_directory + "/core-node.gz.upload"
remote_binary = remote_directory + "/core-node"
remote_staged = remote_directory + "/core-node.staged"
archive_size = archive.stat().st_size
options = [
    "-i",
    str(key),
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=20",
    "-o",
    "ServerAliveInterval=10",
    "-o",
    "ServerAliveCountMax=6",
    "-o",
    "IPQoS=none",
    "-o",
    "StrictHostKeyChecking=accept-new",
    "-o",
    "UserKnownHostsFile=" + str(directory / "known_hosts"),
]

try:
    subprocess.run(
        ["ssh", *options, destination, "install -d -m 700 " + remote_directory],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    # Bound each SSH stream to a small chunk. Healthy connections finish a chunk
    # quickly; a stalled route loses only one short interval, and the next stream
    # resumes from the exact remote byte count. Keep two minutes of the Actions
    # step for digest verification and installation.
    transfer_deadline = time.monotonic() + 600
    chunk_size = 256 * 1024
    attempt = 0
    remote_size = 0
    last_error = "transfer did not run"
    while True:
        if time.monotonic() >= transfer_deadline:
            raise TimeoutError(
                "core transfer deadline reached at " + str(remote_size) + "/"
                + str(archive_size) + " bytes; last error: " + last_error
            )
        attempt += 1
        try:
            size_result = subprocess.run(
                [
                    "ssh",
                    *options,
                    destination,
                    "if [ -f " + remote_upload + " ]; then wc -c < "
                    + remote_upload + "; else printf 0; fi",
                ],
                capture_output=True,
                text=True,
                timeout=30,
                check=True,
            )
            remote_size = int(size_result.stdout.strip() or "0")
            if not 0 <= remote_size <= archive_size:
                subprocess.run(
                    ["ssh", *options, destination, "rm -f " + remote_upload],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True,
                )
                remote_size = 0
            if remote_size == archive_size:
                break
            if transfer_deadline - time.monotonic() < 60:
                raise TimeoutError(
                    "core transfer deadline reached at " + str(remote_size) + "/"
                    + str(archive_size) + " bytes; last error: " + last_error
                )
            print(
                "core transfer stream " + str(attempt) + ": resuming at "
                + str(remote_size) + "/" + str(archive_size) + " bytes",
                flush=True,
            )
            with archive.open("rb") as archive_stream:
                archive_stream.seek(remote_size)
                payload = archive_stream.read(min(chunk_size, archive_size - remote_size))
            subprocess.run(
                [
                    "ssh",
                    *options,
                    destination,
                    "timeout --signal=TERM --kill-after=5 45 sh -c 'cat >> "
                    + remote_upload + "'",
                ],
                input=payload,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                # The remote deadline prevents an orphaned writer from racing
                # the next resumable stream.
                timeout=55,
                check=True,
            )
        except (subprocess.SubprocessError, RuntimeError) as exc:
            last_error = type(exc).__name__
            print(
                "core transfer stream " + str(attempt) + " interrupted: " + last_error,
                flush=True,
            )
            time.sleep(2)
    subprocess.run(
        [
            "ssh",
            *options,
            destination,
            "gzip -dc " + remote_upload + " > " + remote_staged
            + " && printf '%s  %s\\n' " + expected_digest + " " + remote_staged
            + " | sha256sum -c -"
            + " && chmod 700 " + remote_staged
            + " && mv " + remote_staged + " " + remote_binary
            + " && rm -f " + remote_upload,
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    print("core transfer verified: " + expected_digest)
finally:
    key.unlink(missing_ok=True)
    archive.unlink(missing_ok=True)
