import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


SUPERVISOR = Path(__file__).with_name("supervise_live_canary.py")


class CanarySupervisorTests(unittest.TestCase):
    def test_writes_atomic_result_and_pid_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = root / "child.py"
            child.write_text(
                "import json,sys\n"
                "value=json.load(sys.stdin)\n"
                "print(json.dumps({'ok': True, 'value': value}), flush=True)\n"
            )
            config = root / "config.json"
            config.write_text('{"attempt": 49}')
            result = root / "result.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SUPERVISOR),
                    "--script",
                    str(child),
                    "--config",
                    str(config),
                    "--result",
                    str(result),
                    "--log",
                    str(root / "runner.log"),
                ],
                check=False,
                timeout=10,
            )
            self.assertEqual(completed.returncode, 0)
            self.assertEqual(json.loads(result.read_text())["value"]["attempt"], 49)
            self.assertEqual((root / "result.json.exit").read_text(), "0")
            self.assertTrue((root / "runner.pid").read_text().isdigit())
            self.assertTrue((root / "child.pid").read_text().isdigit())

    def test_forwards_sigterm_and_preserves_child_cleanup_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = root / "child.py"
            ready = root / "child.ready"
            child.write_text(
                "import json,pathlib,signal,time\n"
                "def stop(signum, frame):\n"
                " print(json.dumps({'ok': False, 'cleanup': True, 'signal': signum}), flush=True)\n"
                " raise SystemExit(7)\n"
                "signal.signal(signal.SIGTERM, stop)\n"
                f"pathlib.Path({str(ready)!r}).write_text('ready')\n"
                "while True: time.sleep(0.05)\n"
            )
            config = root / "config.json"
            config.write_text("{}")
            result = root / "result.json"
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(SUPERVISOR),
                    "--script",
                    str(child),
                    "--config",
                    str(config),
                    "--result",
                    str(result),
                    "--log",
                    str(root / "runner.log"),
                ]
            )
            deadline = time.monotonic() + 5
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(ready.exists())
            self.assertTrue((root / "child.pid").exists())
            process.send_signal(signal.SIGTERM)
            self.assertEqual(process.wait(timeout=10), 7)
            report = json.loads(result.read_text())
            self.assertTrue(report["cleanup"])
            self.assertEqual(report["signal"], signal.SIGTERM)
            self.assertEqual((root / "result.json.exit").read_text(), "7")


if __name__ == "__main__":
    unittest.main()
