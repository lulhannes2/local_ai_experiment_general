# Preflight output per node

One file per node: `preflight-<hostname>.txt`, produced by [scripts/cluster/preflight.sh](../../scripts/cluster/preflight.sh).
Re-run it after every change (OTA update, network change, switch day) and commit the result, so the git history shows how each node changed over time.

**On the node itself:**
```bash
bash scripts/cluster/preflight.sh > docs/preflight/preflight-$(hostname).txt 2>&1
```

**Remotely from a machine that has this repo** (the script doesn't need to exist on the target; it asks for the password if no SSH key is set up):
```bash
ssh <user>@<host> 'bash -s' < scripts/cluster/preflight.sh > docs/preflight/preflight-<host>.txt 2>&1
# e.g.
ssh dgx43018@dgx43018 'bash -s' < scripts/cluster/preflight.sh > docs/preflight/preflight-dgx43018.txt 2>&1
```

| Node | Last run | Notes |
|---|---|---|
| dgx43018 | — | not yet collected (no key-based SSH from dgx43019) |
| dgx43019 | 2026-11-17 | 10 OK / 4 WARN: MTU 1500, old NVIDIA Sync direct-link config, mpirun missing |
| dgx43020 | — | |
| dgx43021 | — | |
