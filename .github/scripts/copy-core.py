"""Copy a tested core binary with compression and verify it before installation."""
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
fd = os.open(key, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w") as stream:
    stream.write(os.environ["ARB_DEPLOY_KEY"] + "\n")

destination = os.environ["ARB_DEPLOY_USER"] + "@" + os.environ["ARB_DEPLOY_HOST"]
remote_directory = "/tmp/arb-core-" + sha
remote_upload = remote_directory + "/core-node.upload"
remote_binary = remote_directory + "/core-node"
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
    for attempt in range(1, 4):
        try:
            subprocess.run(
                ["scp", "-C", *options, str(source), destination + ":" + remote_upload],
                capture_output=True,
                text=True,
                timeout=300,
                check=True,
            )
            verify = subprocess.run(
                [
                    "ssh",
                    *options,
                    destination,
                    "sha256sum " + remote_upload + " && mv " + remote_upload + " " + remote_binary,
                ],
                capture_output=True,
                text=True,
                timeout=60,
                check=True,
            )
            remote_digest = verify.stdout.split()[0] if verify.stdout.split() else ""
            if remote_digest != expected_digest:
                raise RuntimeError("transferred core digest mismatch")
            print("core transfer verified: " + expected_digest)
            break
        except (subprocess.SubprocessError, RuntimeError) as exc:
            last_error = type(exc).__name__
            if attempt == 3:
                raise RuntimeError("core transfer failed after three attempts: " + last_error) from None
            time.sleep(5 * attempt)
finally:
    key.unlink(missing_ok=True)
