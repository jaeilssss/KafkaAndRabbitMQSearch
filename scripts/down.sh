#!/usr/bin/env bash
# 스택 종료. 데이터(볼륨)까지 지우려면 ./scripts/down.sh -v
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker/docker-compose.yml --profile single down "$@"
