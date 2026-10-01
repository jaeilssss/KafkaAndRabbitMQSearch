#!/usr/bin/env bash
# 사용: ./scripts/run-scenario.sh scenarios/smoke-kafka.yml [results-dir]
set -euo pipefail
cd "$(dirname "$0")/.."
scenario="${1:?usage: run-scenario.sh <scenario.yml> [results-dir]}"
results="${2:-results}"
./gradlew -q :benchmark:bootJar
java -jar benchmark/build/libs/benchmark.jar --scenario="$scenario" --results-dir="$results"
