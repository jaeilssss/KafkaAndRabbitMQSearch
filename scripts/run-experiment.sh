#!/usr/bin/env bash
# 사용: ./scripts/run-experiment.sh experiments/exp2-producer-scaling.yml --profile=quick [--brokers=kafka] [--force] [--dry-run]
#   --profile=<full|quick>  (필수)  full = 계획서 조건, quick = 개발/검증용
#   --brokers=kafka,rabbitmq        브로커 한정. 브로커 간 간섭을 줄이려면 한 번에 하나씩 실행하고 다른 브로커 컨테이너는 내려 둔다.
#   --force                         DONE 인 포인트도 다시 실행
#   --dry-run                       브로커 연결 없이 계획과 소요 시간 상한만 출력
set -euo pipefail
cd "$(dirname "$0")/.."
experiment="${1:?usage: run-experiment.sh <experiment.yml> --profile=<full|quick> [options]}"
shift
./gradlew -q :benchmark:bootJar
java -jar benchmark/build/libs/benchmark.jar --experiment="$experiment" "$@"
