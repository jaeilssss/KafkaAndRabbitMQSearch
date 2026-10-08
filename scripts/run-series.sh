#!/usr/bin/env bash
# 여러 실험을 한 줄로 순서대로 실행한다 (자기 전에 한 번 실행하는 용도).
# 각 실험은 run-isolated.sh 로 브로커를 하나씩 격리해 돌리고, 끝난 포인트(DONE)는 건너뛰므로 끊겨도 같은 명령으로 이어 하면 된다.
# 안전장치: 실험 사이마다 호스트/Docker 디스크 여유와 전원(AC) 연결을 점검하고, 부족하면 남은 실험을 중단한다.
# 한 실험이 실패해도 다음 실험은 계속하고, 마지막에 성공/실패를 요약한다. 전체 로그는 logs/ 에 저장된다.
#
# 사용: ./scripts/run-series.sh <part1|part2|exp1.yml exp2.yml ...> [--profile=full|quick] [--dry-run] [--allow-battery]
#   part1 = Exp 7 -> Exp 2 -> 병렬성     (블로그 1편)
#   part2 = 내구성 -> 느린 소비자         (블로그 2편)
set -uo pipefail
cd "$(dirname "$0")/.."

part1=(experiments/exp7-message-size.yml experiments/exp2-producer-scaling.yml experiments/exp-parallelism.yml)
part2=(experiments/exp-durability.yml experiments/exp-slow-consumer.yml)
MIN_HOST_GB="${MIN_HOST_GB:-15}"       # 호스트 디스크 여유 하한
MIN_DOCKER_GB="${MIN_DOCKER_GB:-15}"     # Docker VM 디스크 여유 하한. Exp 2 의 총량 상한(약 12GB)보다 커야 한다 (Kafka 로그가 여기에 쌓인다)

experiments=(); profile="full"; dry_run=false; allow_battery=false; extra=()
for arg in "$@"; do
  case "$arg" in
    part1) experiments+=("${part1[@]}") ;;
    part2) experiments+=("${part2[@]}") ;;
    --profile=*) profile="${arg#--profile=}" ;;
    --dry-run) dry_run=true ;;
    --allow-battery) allow_battery=true ;;
    *.yml) experiments+=("$arg") ;;
    *) extra+=("$arg") ;;
  esac
done
[[ ${#experiments[@]} -gt 0 ]] || { echo "usage: run-series.sh <part1|part2|exp.yml ...> [--profile=full|quick] [--dry-run] [--allow-battery]" >&2; exit 2; }

if $dry_run; then
  for e in "${experiments[@]}"; do ./scripts/run-experiment.sh "$e" --profile="$profile" --dry-run 2>&1 | grep -E "^experiment=|total runs"; done
  exit 0
fi

mkdir -p logs
log="logs/series-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "$log") 2>&1
echo ">> 로그: $log"

# 안전 점검: 문제가 있으면 메시지를 출력하고 1 을 반환한다.
preflight() {
  local host_gb docker_gb
  host_gb="$(df -g / | awk 'NR==2{print $4}')"
  docker_gb="$(docker run --rm alpine df -BG / 2>/dev/null | awk 'NR==2{gsub("G","",$4);print $4}')"
  echo ">> 점검: 호스트 여유 ${host_gb}GB (하한 ${MIN_HOST_GB}), Docker 여유 ${docker_gb:-?}GB (하한 ${MIN_DOCKER_GB})"
  if [[ -n "$host_gb" && "$host_gb" -lt "$MIN_HOST_GB" ]]; then echo "!! 호스트 디스크 여유 부족"; return 1; fi
  if [[ -z "$docker_gb" ]]; then echo "!! Docker 디스크 여유를 확인할 수 없음 (Docker 가 실행 중인가?)"; return 1; fi
  if [[ "$docker_gb" -lt "$MIN_DOCKER_GB" ]]; then echo "!! Docker 디스크 여유 부족 (docker system prune 등으로 정리 필요)"; return 1; fi
  if [[ "$(uname)" == "Darwin" ]] && ! $allow_battery; then
    if ! pmset -g batt | head -1 | grep -q "AC Power"; then echo "!! 전원 어댑터가 연결돼 있지 않음 (--allow-battery 로 무시 가능)"; return 1; fi
  fi
  return 0
}

results=(); aborted=false
for e in "${experiments[@]}"; do
  name="$(basename "$e" .yml)"
  if $aborted; then results+=("SKIPPED  $name"); continue; fi
  echo; echo "=================== $name ($(date '+%H:%M:%S')) ==================="
  if ! preflight; then aborted=true; results+=("ABORTED  $name (안전 점검 실패)"); continue; fi
  if ./scripts/run-isolated.sh "$e" --profile="$profile" ${extra[@]+"${extra[@]}"}; then
    results+=("OK       $name")
  else
    results+=("FAILED   $name (exit $?)")
  fi
done

echo; echo "=================== 요약 ($(date '+%H:%M:%S')) ==================="
printf '%s\n' "${results[@]}"
echo "로그: $log"
printf '%s\n' "${results[@]}" | grep -qE "^(FAILED|ABORTED)" && exit 1 || exit 0
