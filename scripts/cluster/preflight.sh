#!/usr/bin/env bash
# Read-only preflight check for a DGX Spark that will join the 4-node cluster.
# Usage:  bash scripts/cluster/preflight.sh [peer1 peer2 ...]
#   peers (optional) = hostnames/IPs to test passwordless SSH + ping against.
# Changes nothing on the system. Save output per node:  ... | tee preflight-$(hostname).txt

FABRIC_IFS=(enp1s0f1np1 enP2p1s0f1np1)   # both halves of QSFP port 1 (see manual §2.2)
PASS=0; WARN=0
ok()   { printf '  [ OK ] %s\n' "$*"; PASS=$((PASS+1)); }
warn() { printf '  [WARN] %s\n' "$*"; WARN=$((WARN+1)); }
hdr()  { printf '\n== %s ==\n' "$*"; }

hdr "Identity"
echo "  hostname : $(hostname)"
echo "  user     : $(id -un) (uid $(id -u))"
echo "  date     : $(date -Is)"

hdr "Software versions (must be identical on all 4 nodes)"
. /etc/os-release; echo "  OS          : $PRETTY_NAME"
echo "  kernel      : $(uname -r)"
echo "  dgx-release : $(dpkg-query -W -f='${Version}' dgx-release 2>/dev/null || echo n/a)"
echo "  OTA meta    : $(dpkg-query -W -f='${Version}' dgx-spark-ota-update-meta 2>/dev/null || echo n/a)"
echo "  driver      : $(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null || echo n/a)"
echo "  CUDA        : $(nvidia-smi 2>/dev/null | grep -o 'CUDA Version: [0-9.]*' | awk '{print $3}')"
echo "  docker      : $(docker --version 2>/dev/null || echo missing)"
echo "  nv-ctk      : $(dpkg-query -W -f='${Version}' nvidia-container-toolkit 2>/dev/null || echo missing)"
echo "  ofed/rdma   : $(dpkg-query -W -f='${Version}' rdma-core 2>/dev/null || echo n/a)"

hdr "GPU"
nvidia-smi -L >/dev/null 2>&1 && ok "$(nvidia-smi -L)" || warn "nvidia-smi failed"

hdr "Fabric NICs (ConnectX-7, port 1)"
for i in "${FABRIC_IFS[@]}"; do
  if [[ ! -e /sys/class/net/$i ]]; then warn "$i not present"; continue; fi
  state=$(cat /sys/class/net/$i/operstate); mtu=$(cat /sys/class/net/$i/mtu)
  speed=$(cat /sys/class/net/$i/speed 2>/dev/null); addr=$(ip -4 -br addr show "$i" | awk '{print $3}')
  echo "  $i: state=$state speed=${speed}Mb/s mtu=$mtu ipv4=${addr:-none}"
  [[ $state == up ]] && ok "$i link up" || warn "$i link down (expected until cabled to switch)"
  [[ $mtu -ge 9000 ]] && ok "$i MTU $mtu" || warn "$i MTU $mtu (target 9000)"
done
if command -v ibdev2netdev >/dev/null; then ibdev2netdev | sed 's/^/  /'; else warn "ibdev2netdev missing"; fi
ls /etc/netplan/ | sed 's/^/  netplan: /'
[[ -e /etc/netplan/99-nvidia-sync-cluster.yaml ]] && warn "old NVIDIA Sync direct-link config present (remove on switch day, manual §5.3)"

hdr "Container runtime"
id -nG | grep -qw docker && ok "user in docker group" || warn "user not in docker group"
docker info 2>/dev/null | grep -q nvidia && ok "nvidia runtime registered in docker" || warn "nvidia runtime not visible in 'docker info'"

hdr "Time sync"
[[ $(timedatectl show -p NTPSynchronized --value) == yes ]] && ok "clock NTP-synchronized" || warn "clock not synchronized"

hdr "Storage / memory"
df -h / | tail -1 | awk '{print "  root fs: size="$2" used="$3" free="$4}'
free -g | awk '/Mem:/ {print "  memory : total="$2"G used="$3"G available="$7"G"}'

hdr "Tools needed for validation"
for t in mpirun ib_write_bw git make nvcc; do
  command -v $t >/dev/null && ok "$t found" || warn "$t not found (see manual §4.6)"
done

if (( $# )); then
  hdr "Peers"
  for p in "$@"; do
    ping -c1 -W1 "$p" >/dev/null 2>&1 && ok "ping $p" || warn "ping $p failed"
    ssh -o BatchMode=yes -o ConnectTimeout=3 "$p" true 2>/dev/null && ok "passwordless ssh $p" || warn "passwordless ssh $p failed"
  done
fi

printf '\nSummary: %d OK, %d WARN\n' "$PASS" "$WARN"
