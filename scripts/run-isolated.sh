#!/usr/bin/env bash
# 브로커를 한 번에 하나씩 격리해서 실험을 실행한다 (브로커 간 CPU/디스크 간섭 제거).
# 각 브로커 구간: 다른 브로커 컨테이너 중지 -> 대상 브로커 기동/대기 -> 실험 실행. 끝나면 전체 스택을 복구한다.
# macOS 에서는 caffeinate 로 실행 중 잠들지 않게 한다.
# 사용: ./scripts/run-isolated.sh experiments/exp2-producer-scaling.yml --profile=full [--brokers=kafka,rabbitmq] [--force] [--dry-run]
set -euo pipefail
cd "$(dirname "$0")/.."
experiment="${1:?usage: run-isolated.sh <experiment.yml> --profile=<full|quick> [--brokers=kafka,rabbitmq] [options]}"
shift

brokers="kafka,rabbitmq"
dry_run=false
extra=()
for arg in "$@"; do
  case "$arg" in
    --brokers=*) brokers="${arg#--brokers=}" ;;
    --dry-run) dry_run=true; extra+=("$arg") ;;
    *) extra+=("$arg") ;;
  esac
done

compose=(docker compose -f docker/docker-compose.yml --profile single)
run=(./scripts/run-experiment.sh "$experiment")
if [[ "$(uname)" == "Darwin" ]] && command -v caffeinate >/dev/null; then
  run=(caffeinate -dims "${run[@]}")
fi

if $dry_run; then
  exec ./scripts/run-experiment.sh "$experiment" --brokers="$brokers" "${extra[@]}"
fi

restore() {
  echo ">> 전체 스택 복구"
  "${compose[@]}" up -d --wait --wait-timeout 180 || echo "!! 복구 실패: ./scripts/up.sh 로 수동 기동하세요"
}
trap restore EXIT

IFS=',' read -ra list <<< "$brokers"
for b in "${list[@]}"; do
  case "$b" in
    kafka)    stop=(rabbitmq);                start=(kafka kafka-exporter) ;;
    rabbitmq) stop=(kafka kafka-exporter);    start=(rabbitmq) ;;
    *) echo "unknown broker: $b" >&2; exit 2 ;;
  esac
  echo ">> [$b] 격리 실행: stop=${stop[*]}"
  "${compose[@]}" stop "${stop[@]}"
  "${compose[@]}" up -d --wait --wait-timeout 180 "${start[@]}" cadvisor prometheus
  "${run[@]}" --brokers="$b" "${extra[@]}"
done
