

---

# Local AI on DGX Spark: project status

| Workstream | Status | Docs |
|---|---|---|
| Single-node benchmarks & bottleneck analysis | ✅ done | [docs/HARDWARE_IMPACT_ANALYSIS.md](docs/HARDWARE_IMPACT_ANALYSIS.md), [docs/bottleneck_diagnostics.md](docs/bottleneck_diagnostics.md) |
| Cluster dgx43018–dgx43021 (4 nodes) | 🟡 preparing, waiting for switch | [docs/CLUSTER_SETUP_MANUAL.md](docs/CLUSTER_SETUP_MANUAL.md) |
| Demo | 🟡 in progress | [docs/DEMO_PRESENTATION.md](docs/DEMO_PRESENTATION.md) |
| Use cases (legacy code, annotation assistance, …) | ⬜ later | |

## Cluster: quick start for colleagues
1. Read [docs/CLUSTER_SETUP_MANUAL.md](docs/CLUSTER_SETUP_MANUAL.md) §1–2 (architecture + hardware quirks).
2. On each node run `bash scripts/cluster/preflight.sh | tee docs/preflight/preflight-$(hostname).txt` and commit the output.
3. Work through Phase A (§4) and tick off your node in the checklist (§10).

## Changelog
- **2026-11-17**: Cluster setup manual + preflight script written. dgx43019 preflight done: 200G link up (still the old direct cable), MTU 1500, OpenMPI missing.
