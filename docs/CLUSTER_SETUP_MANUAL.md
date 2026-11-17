# Manual: Clustering 4 DGX Sparks

| | |
|---|---|
| **Goal** | Connect 4 DGX Sparks into one cluster over a 200 GbE RoCE fabric, so they can run multi-node AI workloads (NCCL, vLLM/TRT-LLM, PyTorch distributed) |
| **Nodes** | `dgx43018`, `dgx43019`, `dgx43020`, `dgx43021` (5th Spark stays standalone) |
| **Status** | 🟡 Preparing. Waiting for the switch. Phase A can be done now, Phases B–D need the switch. |
| **Last updated** | 2026-11-17 |

> ⚠️ **Hostname check:** The user story says `dgx40318–dgx40321`, but the host where this was written is `dgx43019`. This manual assumes the nodes are **dgx43018–dgx43021**. Fix the names here if that's wrong.

---

## Contents
1. [Architecture](#1-architecture)
2. [Hardware facts you need to know](#2-hardware-facts-you-need-to-know)
3. [Switch & cabling requirements (what to order)](#3-switch--cabling-requirements)
4. [Phase A: Preparation (no switch needed)](#4-phase-a--preparation-no-switch-needed)
5. [Phase B: Switch day: network bring-up](#5-phase-b--switch-day-network-bring-up)
6. [Phase C: Validation (RDMA + NCCL)](#6-phase-c--validation)
7. [Phase D: First multi-node workload (smoke test)](#7-phase-d--first-multi-node-workload)
8. [Troubleshooting](#8-troubleshooting)
9. [Rollback](#9-rollback)
10. [Progress checklist per node](#10-progress-checklist-per-node)

---

## 1. Architecture

```
                         Office LAN 192.168.10.0/24  (management: SSH, internet, Tailscale)
        ┌──────────────────┬──────────────────┬──────────────────┐
        │ enP7s7           │ enP7s7           │ enP7s7           │ enP7s7
   ┌────┴─────┐       ┌────┴─────┐       ┌────┴─────┐       ┌────┴─────┐
   │ dgx43018 │       │ dgx43019 │       │ dgx43020 │       │ dgx43021 │
   │  GB10    │       │  GB10    │       │  GB10    │       │  GB10    │
   │ 128 GB   │       │ 128 GB   │       │ 128 GB   │       │ 128 GB   │
   └────┬─────┘       └────┬─────┘       └────┬─────┘       └────┬─────┘
        │ QSFP port 1      │                  │                  │
        │ 200 GbE          │                  │                  │
   ┌────┴──────────────────┴──────────────────┴──────────────────┴────┐
   │              200 GbE switch   (MTU 9216, RoCEv2)                  │
   └──────────────────────────────────────────────────────────────────┘
        Fabric A 10.100.10.0/24  (enp1s0f1np1)
        Fabric B 10.100.11.0/24  (enP2p1s0f1np1)
```

We use two separate networks:

| Network | Purpose | Interface | Subnet |
|---|---|---|---|
| **Management** | SSH, apt/OTA updates, docker pulls, Tailscale, dashboards | `enP7s7` (RJ45 10 GbE) | 192.168.10.0/24 (existing LAN) |
| **Fabric** | GPU-to-GPU traffic (NCCL/RDMA), model weight sync | QSFP port 1 (ConnectX-7) | 10.100.10.0/24 + 10.100.11.0/24 |

**Why two networks:** All-reduce traffic during tensor parallel inference is very latency-sensitive. Keeping it off the office LAN avoids slowdowns, and it also means a busy NCCL job can't make SSH unreachable.

### 1.1 IP plan

| Node | Mgmt IP (`enP7s7`) | Fabric A (`enp1s0f1np1`) | Fabric B (`enP2p1s0f1np1`) |
|---|---|---|---|
| dgx43018 | _TBD_ | 10.100.10.18 | 10.100.11.18 |
| dgx43019 | 192.168.10.234 | 10.100.10.19 | 10.100.11.19 |
| dgx43020 | _TBD_ | 10.100.10.20 | 10.100.11.20 |
| dgx43021 | _TBD_ | 10.100.10.21 | 10.100.11.21 |

The last octet of each IP matches the hostname. The fabric subnets are new on purpose, so they don't overlap with the current direct-link subnets (`10.100.192/193.x`, see §2.3).

> Ask IT for **DHCP reservations** (fixed IPs) for the management interfaces. Then fill in the table.

---

## 2. Hardware facts you need to know

### 2.1 Per node
- NVIDIA GB10 Grace Blackwell superchip, **128 GB unified memory** (CPU and GPU share it; `nvidia-smi` shows memory as N/A, which is expected)
- 1× ConnectX-7 NIC with **2 QSFP ports**, up to 200 GbE each
- 1× 10 GbE RJ45 (`enP7s7`), Wi-Fi, 4 TB NVMe
- DGX OS 7 (Ubuntu 24.04), driver 580.x, CUDA 13.0

### 2.2 The "twin interface" detail (important!)
On a Spark, each physical QSFP port shows up as **two** network interfaces and **two** RDMA devices. The ConnectX-7 is connected through two PCIe x4 links, and each link carries half of the bandwidth:

| Physical port | Interface (half 1) | Interface (half 2) | RDMA devices |
|---|---|---|---|
| Port 0 | `enp1s0f0np0` | `enP2p1s0f0np0` | `rocep1s0f0`, `roceP2p1s0f0` |
| **Port 1 (we use this)** | **`enp1s0f1np1`** | **`enP2p1s0f1np1`** | **`rocep1s0f1`, `roceP2p1s0f1`** |

To get the full ~200 Gb/s you have to use **both halves**. That's why each half gets its own IP subnet (Fabric A and B), and why NCCL is told to use both RDMA devices (§6.3). If you use only one half, you get about half the bandwidth.

Use the **same physical port on every node** (we chose port 1 because it's already cabled on dgx43019). Label the cables.

### 2.3 Current state (measured on dgx43019, 2026-11-17)
- Port 1 is **already directly cabled to another Spark** (peer MAC `4c:bb:47:ea:2a:cf`) at 200 Gb/s, set up by NVIDIA Sync:
  - `enp1s0f1np1` = `10.100.192.1/24`, `enP2p1s0f1np1` = `10.100.193.1/24`
  - Config file: `/etc/netplan/99-nvidia-sync-cluster.yaml`
- MTU on the fabric interfaces = **1500** (target: 9000)
- Software: DGX OS 7.6.0, OTA 26.09.3, kernel 7.0.0-1025-nvidia, driver 580.178.04, CUDA 13.0, Docker 29.6.2, nvidia-container-toolkit 1.20.2
- `ib_write_bw` (perftest) is installed. `mpirun` (OpenMPI) is **not** installed.
- Each node uses a different login user (`dgx43018`, `dgx43019`, …). See §4.3 for why that's a problem.

---

## 3. Switch & cabling requirements

What to check on the switch that is being delivered:

| Requirement | Why |
|---|---|
| ≥ 4 ports **200 GbE QSFP112** (or 400 GbE ports with a supported 2×200G breakout) | One port per Spark. QSFP56 at 200G also works if the switch/cable support it. |
| **RoCEv2** support: PFC (priority flow control) and/or ECN | RDMA performs badly on a lossy network. Packet drops cause big NCCL slowdowns. |
| **Jumbo frames**, MTU ≥ 9216 | Lets us use host MTU 9000 |
| Managed (CLI/web UI) | Needed for MTU/PFC config and for reading port counters |
| Spare port(s) | For the 5th Spark later, or an NFS/storage node |

**Cables:** QSFP112 (200G) DAC up to ~2 m if the switch is next to the Sparks, otherwise AOC. Make sure the cables are on the switch vendor's compatibility list.

> 📌 NVIDIA's official Spark playbooks cover a **2-node direct connection** in detail. Before you buy or configure anything, check the current NVIDIA documentation (build.nvidia.com/spark and the DGX Spark user guide) for multi-node/switch topologies and any list of validated switches.

---

## 4. Phase A: Preparation (no switch needed)

Do all of this now on **all 4 nodes**. Each step can be repeated safely.

### 4.1 Inventory and preflight
On every node:
```bash
cd ~/develop2   # or wherever this repo is cloned
bash scripts/cluster/preflight.sh | tee docs/preflight/preflight-$(hostname).txt
```
All outputs are collected in [docs/preflight/](preflight/). That folder's README also explains how to run the check remotely over SSH.
Compare the four outputs. **Software versions must be the same on all nodes** (DGX OS, OTA level, driver, CUDA). Different NCCL/driver versions are the most common cause of multi-node hangs.

### 4.2 Align software versions
On each node, apply the same OTA level through the DGX Dashboard (or `sudo apt update && sudo apt full-upgrade`), then reboot. Record the result in §10.

### 4.3 One shared cluster user
MPI, Ray and `mpirun` expect **the same username (and preferably the same UID) on every node**, with the same paths. Right now each Spark has its own user, so we create a dedicated user on all 4 nodes:

```bash
sudo groupadd -g 2000 cluster
sudo useradd -m -u 2000 -g 2000 -s /bin/bash -G docker,sudo cluster
sudo passwd cluster
```
UID/GID 2000 is the same everywhere, so files on shared storage (§4.7) get the right owners.

### 4.4 Hostnames and /etc/hosts
Add this block to `/etc/hosts` on **every** node (fill in the management IPs):
```
# --- DGX Spark cluster: management ---
192.168.10.xxx  dgx43018
192.168.10.234  dgx43019
192.168.10.xxx  dgx43020
192.168.10.xxx  dgx43021
# --- DGX Spark cluster: fabric A (used by MPI/NCCL bootstrap) ---
10.100.10.18  dgx43018-fab
10.100.10.19  dgx43019-fab
10.100.10.20  dgx43020-fab
10.100.10.21  dgx43021-fab
```
Note: `/etc/hosts` on dgx43019 also contains `127.0.0.1 spark-2965`. That's the factory name and it's harmless, but **never** map the node's own cluster hostname to `127.0.0.1`, because then MPI and Ray advertise loopback to the other nodes.

### 4.5 Passwordless SSH mesh (as user `cluster`)
On **dgx43018** (the "head" node):
```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
for h in dgx43018 dgx43019 dgx43020 dgx43021; do ssh-copy-id cluster@$h; done
# copy the same key to every node so any node can reach any node:
for h in dgx43019 dgx43020 dgx43021; do scp ~/.ssh/id_ed25519* cluster@$h:~/.ssh/; done
```
Test it. Each command must return without asking for a password:
```bash
for a in dgx43018 dgx43019 dgx43020 dgx43021; do
  for b in dgx43018 dgx43019 dgx43020 dgx43021; do
    ssh $a "ssh -o BatchMode=yes $b hostname" ; done; done
```
Best practice: after this works, set `PasswordAuthentication no` in `/etc/ssh/sshd_config` for the cluster. The current `~/.ssh/config` on dgx43019 has `StrictHostKeyChecking no`. That's fine for a lab, but don't copy it to production.

### 4.6 Install validation tools
On all nodes:
```bash
sudo apt install -y openmpi-bin libopenmpi-dev perftest infiniband-diags rdma-core ethtool iperf3 build-essential git
```
Build NCCL and nccl-tests. GB10 is compute capability **12.1** (`sm_121`):
```bash
mkdir -p ~/src && cd ~/src
git clone https://github.com/NVIDIA/nccl.git && cd nccl
make -j$(nproc) src.build NVCC_GENCODE="-gencode=arch=compute_121,code=sm_121"
cd ~/src && git clone https://github.com/NVIDIA/nccl-tests.git && cd nccl-tests
make -j$(nproc) MPI=1 MPI_HOME=/usr/lib/aarch64-linux-gnu/openmpi NCCL_HOME=$HOME/src/nccl/build
```
Use the **same path and the same commit** on every node, or build on one node and `rsync` to the others.

### 4.7 Model storage strategy
Every node needs a copy of the model weights. Options:

| Option | Pros | Cons |
|---|---|---|
| **Local copy on each node** (recommended) | Fastest loading, no single point of failure | 4× disk usage (4 TB per node, so usually fine) |
| NFS export from one node over the fabric | One copy to manage | Slower cold start, head node becomes a dependency |

Recommended: keep one "golden" HuggingFace cache on dgx43018 at `/data/models` and sync it over the fabric:
```bash
rsync -a --info=progress2 /data/models/ dgx43019-fab:/data/models/
```
Use the same path on every node, because the containers will mount it.

### 4.8 Pre-pull containers
On all nodes, pull the same image **tag** (never use `latest`, because the versions will drift apart), for example the NGC vLLM / TensorRT-LLM / PyTorch images for Spark (arm64). Write the exact tags in §10.

### 4.9 Firewall
If `ufw` is enabled, allow all traffic on the fabric subnets (NCCL, MPI and Ray use random high ports):
```bash
sudo ufw allow from 10.100.10.0/24
sudo ufw allow from 10.100.11.0/24
```

### 4.10 Single-node baselines
Before clustering, record single-node numbers (tokens/s, time-to-first-token, memory) for the demo models on **each** node. The existing benchmarks are in [HARDWARE_IMPACT_ANALYSIS.md](HARDWARE_IMPACT_ANALYSIS.md). The demo needs these to show "1 Spark vs 4 Sparks".

---

## 5. Phase B: Switch day: network bring-up

### 5.1 Configure the switch (before connecting the Sparks)
- Ports for the Sparks: speed **200G**, FEC **RS** (RS-FEC is standard for 200G PAM4). Try auto-negotiation first, and force the speed if the link doesn't come up.
- **MTU 9216** on all Spark ports
- All Spark ports in one VLAN (access/untagged)
- RoCE: enable **PFC on priority 3** + **ECN** (many switches have a "RoCE lossless" preset). Without this, NCCL still works but becomes unstable under load.
- Disable spanning-tree delays on these ports (edge/portfast)
- Save the config and export a backup to `docs/switch/` in this repo

### 5.2 Cabling
Connect **QSFP port 1** of each Spark to the switch. Record which switch port goes to which node:

| Node | Spark port | Switch port |
|---|---|---|
| dgx43018 | 1 | |
| dgx43019 | 1 | |
| dgx43020 | 1 | |
| dgx43021 | 1 | |

### 5.3 Remove the old direct-link config
On dgx43019 (and its current peer), keep a backup and remove the NVIDIA Sync config:
```bash
sudo cp /etc/netplan/99-nvidia-sync-cluster.yaml ~/backup-99-nvidia-sync-cluster.yaml
sudo rm /etc/netplan/99-nvidia-sync-cluster.yaml
```
Unplug the direct cable between the two Sparks.

### 5.4 Apply the fabric netplan
Create `/etc/netplan/60-cluster-fabric.yaml` on each node, replacing `NN` with the node number (18/19/20/21):
```yaml
network:
  version: 2
  ethernets:
    enp1s0f1np1:
      dhcp4: false
      dhcp6: false
      link-local: []
      mtu: 9000
      addresses: [10.100.10.NN/24]
    enP2p1s0f1np1:
      dhcp4: false
      dhcp6: false
      link-local: []
      mtu: 9000
      addresses: [10.100.11.NN/24]
```
No gateway: the fabric is isolated on purpose, so all internet and management traffic keeps going through `enP7s7`.

```bash
sudo chmod 600 /etc/netplan/60-cluster-fabric.yaml
sudo netplan try        # automatically rolls back after 120 s if you lose access
sudo netplan apply
```

### 5.5 Verify the links
On each node:
```bash
ethtool enp1s0f1np1   | grep -E "Speed|Link detected"   # expect 200000Mb/s, yes
ethtool enP2p1s0f1np1 | grep -E "Speed|Link detected"
ibdev2netdev                                            # rocep1s0f1 / roceP2p1s0f1 (Up)
# jumbo frames work end to end (8972 = 9000 - 28 bytes of IP/ICMP headers):
for n in 18 19 20 21; do ping -c2 -M do -s 8972 10.100.10.$n; ping -c2 -M do -s 8972 10.100.11.$n; done
bash scripts/cluster/preflight.sh dgx43018-fab dgx43019-fab dgx43020-fab dgx43021-fab
```
If the jumbo ping fails but a normal ping works, the switch MTU is wrong.

---

## 6. Phase C: Validation

Save all results under `docs/results/cluster/`. They form the "network" chapter of the demo.

### 6.1 RDMA bandwidth between pairs (perftest)
```bash
# on dgx43018 (server)
ib_write_bw -d rocep1s0f1 -x 3 --report_gbits -a
# on dgx43019 (client)
ib_write_bw -d rocep1s0f1 -x 3 --report_gbits -a 10.100.10.18
```
Repeat with `roceP2p1s0f1` / `10.100.11.18`. Each half should get close to its share of the line rate. Test every pair (6 pairs) so you can find a bad cable or switch port. `-x 3` selects the RoCEv2/IPv4 GID. Check with `show_gids` if it differs.

### 6.2 TCP sanity check (iperf3)
```bash
iperf3 -s                                    # on dgx43018
iperf3 -c 10.100.10.18 -P 8 -t 20            # on another node
```
TCP will be well below the RDMA numbers. This test only proves basic IP connectivity and MTU.

### 6.3 NCCL all-reduce across 4 nodes
Run as `cluster` from dgx43018:
```bash
export NCCL_HOME=$HOME/src/nccl/build
export LD_LIBRARY_PATH=$NCCL_HOME/lib:$LD_LIBRARY_PATH
mpirun -np 4 -H dgx43018-fab,dgx43019-fab,dgx43020-fab,dgx43021-fab \
  --mca btl_tcp_if_include enp1s0f1np1 --mca oob_tcp_if_include enp1s0f1np1 \
  -x LD_LIBRARY_PATH \
  -x NCCL_SOCKET_IFNAME=enp1s0f1np1 \
  -x NCCL_IB_HCA=rocep1s0f1,roceP2p1s0f1 \
  -x NCCL_IB_GID_INDEX=3 \
  -x NCCL_DEBUG=INFO \
  $HOME/src/nccl-tests/build/all_reduce_perf -b 8 -e 4G -f 2 -g 1
```
What to look for:
- The `NCCL INFO` lines must say **`NET/IB`** and list both `rocep1s0f1` and `roceP2p1s0f1`. If they say `NET/Socket`, RDMA isn't being used. See §8.
- Write down **busbw** at the large message sizes (≥ 256 MB) for 2, 3 and 4 nodes (`-np 2/3/4`). Seeing how it scales is a good thing to show in the demo.
- Also run `all_gather_perf` and `sendrecv_perf`.

Save these env vars as the cluster defaults, for example in `/etc/profile.d/nccl-cluster.sh` on all nodes, so later workloads (vLLM, PyTorch) use them automatically.

---

## 7. Phase D: First multi-node workload

This step is the bridge to the next user story. The idea:
1. Start a **Ray** cluster: `ray start --head --node-ip-address=10.100.10.18` on dgx43018, and `ray start --address=10.100.10.18:6379 --node-ip-address=10.100.10.NN` on the others. Run it inside the same vLLM container on every node, using `--network host --ipc host --gpus all`, the `/data/models` mount, and the NCCL env vars from §6.3.
2. Serve a model that **does not fit on one Spark** (more than ~110 GB of weights + KV cache), using `--tensor-parallel-size 4` or `--pipeline-parallel-size 4`. Compare TP and PP: over Ethernet, PP is often faster than TP because it sends less data between nodes.
3. Measure the same metrics as the single-node baseline (§4.10).

A detailed run book will be written once Phase C passes.

---

## 8. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Link down after cabling | Check FEC (RS) and speed on the switch port. Try forcing 200G. Swap the cable to rule it out. |
| Normal ping OK, `ping -M do -s 8972` fails | MTU mismatch. The switch port must be ≥ 9216 and the host 9000. |
| NCCL shows `NET/Socket` | Wrong `NCCL_IB_HCA` name, RDMA modules not loaded, or running in a container without `--network host` + `/dev/infiniband` access. |
| NCCL hangs at init | Firewall (§4.9), node hostname resolves to 127.0.0.1 (§4.4), or different NCCL versions on different nodes. |
| Low busbw, many retransmits | No PFC/ECN on the switch. Check the switch port pause/drop counters and `ethtool -S enp1s0f1np1 \| grep -i -E "discard\|pause\|out_of_buffer"`. |
| Only ~100 Gb/s | Only one twin interface is in use. Include **both** RDMA devices in `NCCL_IB_HCA`. |
| `mpirun` asks for a password | SSH mesh incomplete (§4.5), or not running as user `cluster`. |
| GPU memory shows "N/A" | Normal on GB10 (unified memory). Use `free -g` / DGX Dashboard instead. |

---

## 9. Rollback

To go back to the 2-node direct setup or to standalone nodes:
```bash
sudo rm /etc/netplan/60-cluster-fabric.yaml
sudo cp ~/backup-99-nvidia-sync-cluster.yaml /etc/netplan/99-nvidia-sync-cluster.yaml   # only on the old pair
sudo netplan apply
```
The management network is never changed by this manual, so remote access stays available throughout.

---

## 10. Progress checklist per node

Legend: ⬜ todo · 🟡 in progress · ✅ done · ❌ issue (add a note)

| Step | dgx43018 | dgx43019 | dgx43020 | dgx43021 |
|---|---|---|---|---|
| 4.1 Preflight output saved | ⬜ | ✅ | ⬜ | ⬜ |
| 4.2 Same OTA level (target: 26.09.3 / driver 580.178.04) | ⬜ | ✅ | ⬜ | ⬜ |
| 4.3 `cluster` user (uid 2000) | ⬜ | ⬜ | ⬜ | ⬜ |
| 4.4 /etc/hosts block | ⬜ | ⬜ | ⬜ | ⬜ |
| 4.5 SSH mesh verified | ⬜ | ⬜ | ⬜ | ⬜ |
| 4.6 OpenMPI + NCCL + nccl-tests built | ⬜ | ⬜ (perftest ✅, mpirun missing) | ⬜ | ⬜ |
| 4.7 `/data/models` synced | ⬜ | ⬜ | ⬜ | ⬜ |
| 4.8 Containers pulled (tags: ___ ) | ⬜ | ⬜ | ⬜ | ⬜ |
| 4.10 Single-node baseline | ⬜ | 🟡 (see HARDWARE_IMPACT_ANALYSIS) | ⬜ | ⬜ |
| 5.3 Old direct-link config removed | n/a? | ⬜ | ⬜ | ⬜ |
| 5.4 Fabric netplan applied | ⬜ | ⬜ | ⬜ | ⬜ |
| 5.5 200G link + jumbo ping OK | ⬜ | ⬜ | ⬜ | ⬜ |
| 6.1 ib_write_bw pairs | ⬜ | ⬜ | ⬜ | ⬜ |
| 6.3 NCCL 4-node all-reduce | ⬜ | ⬜ | ⬜ | ⬜ |

### Open questions
- [ ] Confirm the hostnames (dgx430xx vs dgx403xx) and the management IPs
- [ ] Which Spark is currently direct-cabled to dgx43019 (peer MAC `4c:bb:47:ea:2a:cf`, fabric IP 10.100.192.2)? Its SSH host key is **different from dgx43018's**, so it is **not** dgx43018. Is it dgx43020/21 or the 5th machine?
- [ ] Switch make/model and delivery date. Does it support RoCE (PFC/ECN) and 200G QSFP112?
- [ ] Which Spark becomes the head node (Ray head, model golden copy)? This manual assumes dgx43018.
