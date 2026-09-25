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
    last_error = "transfer did not run"
    for attempt in range(1, 5):
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
                timeout=60,
                check=True,
            )
            remote_size = int(size_result.stdout.strip() or "0")
            if not 0 <= remote_size <= archive_size:
                subprocess.run(
                    ["ssh", *options, destination, "rm -f " + remote_upload],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=True,
                )
                remote_size = 0
            print(
                "core transfer attempt " + str(attempt) + ": resuming at "
                + str(remote_size) + "/" + str(archive_size) + " bytes",
                flush=True,
            )

            # Preserve the verified TCP prefix after a timeout and resume from its
            # exact byte count. The runner-to-VPS route can be consistently slow;
            # restarting from byte zero made every otherwise healthy attempt fail.
            with archive.open("rb") as archive_stream:
                archive_stream.seek(remote_size)
                if remote_size < archive_size:
                    subprocess.run(
                        ["ssh", *options, destination, "cat >> " + remote_upload],
                        stdin=archive_stream,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        timeout=140,
                        check=True,
                    )
            size_result = subprocess.run(
                ["ssh", *options, destination, "wc -c < " + remote_upload],
                capture_output=True,
                text=True,
                timeout=60,
                check=True,
            )
            if int(size_result.stdout.strip()) != archive_size:
                raise RuntimeError("incomplete resumable transfer")
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
            break
        except (subprocess.SubprocessError, RuntimeError) as exc:
            last_error = type(exc).__name__
            print(
                "core transfer attempt " + str(attempt) + " interrupted: " + last_error,
                flush=True,
            )
            if attempt == 4:
                raise RuntimeError("core transfer failed after four attempts: " + last_error) from None
            time.sleep(5 * attempt)
finally:
    key.unlink(missing_ok=True)
    archive.unlink(missing_ok=True)
