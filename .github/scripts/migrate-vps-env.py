"""Preserve an existing installation's credentials before replacing its service unit.
Run as root on the VPS. Never print environment values.
"""
import os
from pathlib import Path
import subprocess

output = Path('/etc/vps-api.env')
if output.exists():
    if output.is_symlink():
        raise SystemExit('Refusing a symlink for vps-api environment')
    output.chmod(0o600)
    raise SystemExit(0)
pid = subprocess.check_output(['systemctl', 'show', '-p', 'MainPID', '--value', 'vps-api'], text=True).strip()
if not pid.isdigit() or int(pid) <= 0:
    raise SystemExit('Provision /etc/vps-api.env before first deployment; no running service to migrate')
raw = Path(f'/proc/{pid}/environ').read_bytes()
variables = dict(item.split(b'=', 1) for item in raw.split(b'\0') if b'=' in item)
required = ['COLLECTOR_TOKEN', 'PASS_DIMA', 'PASS_SERGEI', 'PROFILES_DB_PATH', 'REAL_DB_PATH']
optional = ['INTERNAL_TOKEN', 'PORT', 'RUST_LOG']
lines = []
for key in required + optional:
    value = variables.get(key.encode())
    if value is None:
        if key in required:
            raise SystemExit(f'Required environment variable missing: {key}')
        continue
    text = value.decode('utf-8')
    if not text or '\n' in text or '\r' in text:
        raise SystemExit(f'Invalid environment value for {key}')
    escaped = text.replace('\\', '\\\\').replace('"', '\\"')
    lines.append(f'{key}="{escaped}"\n')
fd = os.open(str(output), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'w') as stream:
    stream.writelines(lines)
    stream.flush()
    os.fsync(stream.fileno())
print('Existing vps-api environment preserved in a root-only file')
