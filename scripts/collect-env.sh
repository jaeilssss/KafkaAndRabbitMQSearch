#!/usr/bin/env bash
# 실험 환경(계획서 §7)을 results/env.json 에 기록한다.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
if [[ "$(uname)" == "Darwin" ]]; then
  cpu="$(sysctl -n machdep.cpu.brand_string)"; cores="$(sysctl -n hw.ncpu)"; mem="$(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))GiB"
else
  cpu="$(lscpu | awk -F: '/Model name/{gsub(/^ +/,"",$2);print $2}')"; cores="$(nproc)"; mem="$(free -g | awk '/Mem:/{print $2"GiB"}')"
fi
jdk="$(java -version 2>&1 | head -1)"
docker_v="$(docker --version)"
compose_v="$(docker compose version --short)"
docker_mem="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo unknown)"
docker_cpus="$(docker info --format '{{.NCPU}}' 2>/dev/null || echo unknown)"
python3 - "$cpu" "$cores" "$mem" "$(uname -srm)" "$jdk" "$docker_v" "$compose_v" "$docker_cpus" "$docker_mem" <<'PY'
import json, sys, pathlib
keys = ["cpu","cores","memory","os","jdk","docker","dockerCompose","dockerCpus","dockerMemBytes"]
env = dict(zip(keys, sys.argv[1:]))
for line in pathlib.Path("docker/.env").read_text().splitlines():
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1); env[k] = v
pathlib.Path("results/env.json").write_text(json.dumps(env, indent=2, ensure_ascii=False) + "\n")
print(json.dumps(env, indent=2, ensure_ascii=False))
PY
