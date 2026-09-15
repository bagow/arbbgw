"""Run the candidate binary in disposable databases with no exchange credentials."""
import json
import os
from pathlib import Path
import resource
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request


def limits():
    os.nice(10)
    resource.setrlimit(resource.RLIMIT_AS, (768 * 1024 * 1024, 768 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def main():
    binary = Path(sys.argv[1]).resolve()
    expected_sha = sys.argv[2]
    if binary.name != "core-node" or not binary.is_file():
        raise RuntimeError("Missing candidate binary")
    if len(expected_sha) != 40 or any(c not in "0123456789abcdef" for c in expected_sha):
        raise RuntimeError("Invalid build identity")
    candidate_dir = Path("/home/opc/arb-candidates") / expected_sha
    candidate_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(binary, candidate_dir / "core-node.upload")
    os.replace(candidate_dir / "core-node.upload", candidate_dir / "core-node")
    with tempfile.TemporaryDirectory(prefix="arb-core-smoke-", dir="/tmp") as directory:
        root = Path(directory)
        # An empty local .env prevents dotenv from searching parent directories.
        (root / ".env").write_text("")
        source = Path("/home/opc/core-node/vps-api.db")
        with sqlite3.connect(source.as_uri()+"?mode=ro",uri=True,timeout=5) as live:
            with sqlite3.connect(root/"profiles.db") as copy:
                live.backup(copy)
                disabled = json.dumps(["gate","bybit","bitget","bingx","bitmex","ascendex","xt","toobit"])
                copy.execute("UPDATE trading_profiles SET is_active=0,pending_commands=NULL,disabled_exchanges=?",(disabled,))
                copy.execute("DELETE FROM server_cache")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1",0))
            port = probe.getsockname()[1]
        # Explicit allowlist: inherit no process environment or service secrets.
        environment = {
            "PATH":"/usr/local/bin:/usr/bin:/bin", "RUST_LOG":"error,core_node::collector::binance=info",
            "PROFILES_DB_PATH":str(root/"profiles.db"),"REAL_DB_PATH":str(root/"trades.db"),
            "PORT":str(port),"CORE_DIAGNOSTIC_PATH":str(root/"profiles.txt"),
        }
        binary.chmod(0o700)
        with (root/"process.log").open("w") as log:
            process = subprocess.Popen([str(binary)],cwd=root,env=environment,
                stdout=log,stderr=log,preexec_fn=limits)
            try:
                deadline = time.monotonic()+25
                health = None
                while time.monotonic()<deadline:
                    if process.poll() is not None:
                        raise RuntimeError("Candidate exited before health check: "+str(process.returncode)
                            +"; isolated log: "+(root/"process.log").read_text(errors="replace")[-2000:])
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health",timeout=2) as response:
                            health = json.load(response)
                        if health.get("healthy"):
                            break
                    except (OSError,ValueError):
                        pass
                    time.sleep(0.25)
                if not health or not health.get("healthy") or health.get("build_sha") != expected_sha:
                    raise RuntimeError("Candidate health/build identity not confirmed")
                samples = []
                observation_start = time.monotonic()
                while time.monotonic() - observation_start < 60:
                    if process.poll() is not None:
                        raise RuntimeError("Candidate exited during public market observation")
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as response:
                        health = json.load(response)
                    if not health.get("healthy") or health.get("build_sha") != expected_sha:
                        raise RuntimeError("Candidate lost health/build identity during observation")
                    fresh = {venue["exchange"]: venue["fresh_books"] for venue in health["venues"]}
                    samples.append({"elapsed_seconds": round(time.monotonic()-observation_start, 1),
                                    "fresh_books": fresh})
                    time.sleep(2)
                simultaneous = sum(all(sample["fresh_books"].get(ex, 0) > 0
                                       for ex in ("binance", "kucoin")) for sample in samples)
                with sqlite3.connect((root/"trades.db").as_uri()+"?mode=ro",uri=True) as db:
                    orders = db.execute("SELECT COUNT(*) FROM execution_orders").fetchone()[0]
                    trades = db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
                if orders or trades:
                    raise RuntimeError("Unexpected order/trade intent in credential-free smoke test")
                print(json.dumps({"ok":simultaneous >= 5,"health":health,"order_intents":orders,"trades":trades,
                    "exchange_credentials_provided":False,"production_replaced":False,
                    "public_collector_log":(root/"process.log").read_text(errors="replace")[-12000:],
                    "market_samples":samples,"simultaneous_fresh_samples":simultaneous,
                    "scope":"Public Binance/KuCoin data for 60 seconds; inactive profile; no profitability claim"}))
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)


if __name__ == "__main__":
    main()
