#!/usr/bin/env bash
# 단일 노드 스택 기동 후 모든 컨테이너가 healthy 가 될 때까지 대기한다.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker/docker-compose.yml --profile single up -d --wait --wait-timeout 180
docker compose -f docker/docker-compose.yml --profile single ps
echo "Grafana http://localhost:3000 (admin/admin) | Prometheus http://localhost:9090 | RabbitMQ UI http://localhost:15672 (guest/guest)"
