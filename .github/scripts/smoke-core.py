"""Stage a tested artifact and run a credential-free, isolated server smoke test."""
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

directory = Path(os.environ["RUNNER_TEMP"]) / "arb-smoke-ssh"
directory.mkdir(mode=0o700)
key = directory / "key"
fd = os.open(key,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,"w") as stream:
    stream.write(os.environ["ARB_SMOKE_KEY"]+"\n")
options = ["-i",str(key),"-o","BatchMode=yes","-o","ConnectTimeout=15",
           "-o","StrictHostKeyChecking=accept-new","-o","UserKnownHostsFile="+str(directory/"known_hosts")]
destination = os.environ["ARB_SMOKE_USER"]+"@"+os.environ["ARB_SMOKE_HOST"]
remote_directory = None
try:
    created = subprocess.run(["ssh",*options,destination,"mktemp -d /tmp/arb-smoke.XXXXXXXX"],capture_output=True,text=True,timeout=30,check=True)
    remote_directory = created.stdout.strip()
    if not re.fullmatch(r"/tmp/arb-smoke\.[A-Za-z0-9]+",remote_directory):
        raise RuntimeError("Unexpected staging directory")
    binary = remote_directory+"/core-node"
    subprocess.run(["scp","-C",*options,"release/core-node",destination+":"+binary],capture_output=True,timeout=180,check=True)
    command = "python3 - "+shlex.quote(binary)+" "+shlex.quote(os.environ["GITHUB_SHA"])
    proc = subprocess.run(["ssh",*options,destination,command],input=Path("scripts/smoke_core_server.py").read_text(),
                          capture_output=True,text=True,timeout=120)
    if proc.returncode:
        # Isolated process has no secrets; retain its exception for diagnosis.
        raise RuntimeError("Candidate smoke failed: "+proc.stderr[-2000:])
    report = json.loads(proc.stdout)
    report.setdefault("ok", True)
    Path("smoke-result.json").write_text(json.dumps(report,indent=2))
    if not report["ok"]:
        raise RuntimeError("Candidate market-data check failed; see smoke-result.json")
except Exception as exc:
    if not Path("smoke-result.json").exists():
        Path("smoke-result.json").write_text(json.dumps({"ok":False,"error":str(exc)},indent=2))
    raise
finally:
    if remote_directory and re.fullmatch(r"/tmp/arb-smoke\.[A-Za-z0-9]+",remote_directory):
        command = "python3 -c "+shlex.quote("import shutil; shutil.rmtree("+repr(remote_directory)+")")
        subprocess.run(["ssh",*options,destination,command],capture_output=True,timeout=30)
    key.unlink(missing_ok=True)
