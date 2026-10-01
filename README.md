# Kafka vs RabbitMQ Performance Study

Kafka와 RabbitMQ의 처리량·지연·내구성·장애 복구 특성을 재현 가능하게 비교하기 위한 Spring Boot 기반 벤치마크 프로젝트.
연구 계획서: `kafka-rabbitmq-performance-study-plan.md` (Exp 1~10). 이 저장소의 현재 범위는 **환경·골격 세팅**이다.

## 구성

| 경로 | 역할 |
|------|------|
| `docker/` | Compose (`single` 프로파일), 이미지 태그 고정(`docker/.env`), RabbitMQ 플러그인 설정 |
| `monitoring/` | Prometheus 설정, Grafana 데이터소스/대시보드 프로비저닝 |
| `common/` | `Scenario`(YAML), `Payload`, `LatencyStats`(HdrHistogram), `MessagePublisher`/`MessageConsumerGroup` 추상화, 결과 저장 |
| `producer/` | `KafkaMessagePublisher`(Spring Kafka), `RabbitMessagePublisher`(Spring AMQP) |
| `consumer/` | `KafkaMessageConsumerGroup`, `RabbitMessageConsumerGroup` |
| `benchmark/` | Spring Boot 러너 (`benchmark.jar`), topic/queue 생성·정리, 집계 |
| `scenarios/` | 실험 조건 YAML |
| `scripts/` | `up.sh`, `down.sh`, `run-scenario.sh`, `collect-env.sh` |
| `results/`, `graphs/`, `paper/` | 산출물 |

## 사용법

요구사항: JDK 21+, Docker (Compose v2). 권장 Docker 자원: CPU 6+, 메모리 6GiB+ (컨테이너 limit 합계 약 5GiB).

```bash
./scripts/up.sh                                   # Kafka(KRaft) / RabbitMQ / Prometheus / Grafana / exporter 기동, healthy 대기
./scripts/collect-env.sh                          # results/env.json 에 환경·이미지 버전 기록
./scripts/run-scenario.sh scenarios/smoke-kafka.yml
./scripts/run-scenario.sh scenarios/smoke-rabbitmq.yml
./scripts/down.sh                                 # 종료 (-v 로 볼륨 삭제)
./gradlew build                                   # 빌드 + 단위 테스트
```

| UI | 주소 |
|----|------|
| Grafana (admin/admin) | http://localhost:3000 — Kafka / RabbitMQ / Containers 대시보드 |
| Prometheus | http://localhost:9090 |
| RabbitMQ Management (guest/guest) | http://localhost:15672 |
| cAdvisor | http://localhost:8081 |

## Scenario YAML

```yaml
name: exp5-prefetch-50
broker: rabbitmq            # kafka | rabbitmq
messageSizeBytes: 1024
messageCount: 1000000
producers: 4
consumers: 4
warmupSeconds: 120          # 이 구간에 보낸 메시지는 집계 제외
measureSeconds: 300         # 발행 최대 시간 (messageCount 도달 시 먼저 종료)
cooldownSeconds: 60         # 발행 종료 후 consumer drain 대기 최대 시간
consumerDelayMs: 0          # Exp 8 slow consumer
kafka:    { partitions: 1, replicationFactor: 1, acks: "1", lingerMs: 0, batchSizeBytes: 16384, compression: none }
rabbitmq: { queueType: classic, publisherConfirms: false, prefetch: 250 }
```

생략한 값은 기본값이 적용된다. Run 마다 topic/queue 를 새로 만들고 종료 시 삭제한다.
결과는 `results/<name>-<timestamp>.json`(환경 정보 포함)과 `results/summary.csv`(누적)에 저장된다.

## 부하 제어: 목표 rate · 반복 · 단계 상승

```yaml
targetRatePerSec: 5000          # 전체 producer 합계 msg/s (생략하면 무제한)
# 또는
rateSteps: [2000, 4000, 8000]   # step load. targetRatePerSec 와 동시 사용 불가
repetitions: 3                  # 각 step 을 3회 반복
pauseBetweenRunsSeconds: 5      # Run 사이 대기 (기본 5)
```

- rate 옵션을 쓰면 **시간 기반**(warmup + measure)으로 종료한다. `messageCount` 를 적지 않으면 상한이 없다.
- 결과 파일 (`results/`):
  - `<name>-<ts>.json` Run 마다 (rate 모드에서는 `targetRatePerSec`, `scheduleLagP99Ms`, `generatorProcessCpuLoad`, `generatorSaturated` 포함), `summary.csv` 누적
  - `<name>-aggregate-<ts>.json/.csv` step 별 mean / median / 표본 stddev (producer·consumer msg/s, P50/P95/P99, lost)
  - `<name>-sweep-<ts>.csv` (rateSteps 일 때) step 당 한 행: `targetRatePerSec, runs, consumerMsgPerSec, achievedRatio, p99Ms, lostTotal, generatorSaturated`
- 예시: `scenarios/rate-*.yml`(고정 5,000 msg/s), `scenarios/sweep-*.yml`(2K→4K, 2회 반복).

### 결과 해석

- **Coordinated omission 방지**: rate 모드의 latency 는 "실제로 보낸 시각"이 아니라 **계획 발행 시각** 기준이다. 브로커/클라이언트가 막혀 발행이 밀려도 그 지연이 latency 에 그대로 반영된다. 무제한 모드는 발행 직전 시각 기준(기존 방식).
- **achievedRatio** = 소비 처리량 ÷ 목표 rate. 1 보다 눈에 띄게 낮아지고 P99 가 급등하는 step 이 Saturation 후보 구간이다(판정은 사람이 한다).
- **generatorSaturated** = 측정 구간 schedule lag P99 > 10ms 이거나 러너 JVM 평균 CPU > 80%(전체 코어 대비). `true` 인 step 은 브로커 한계가 아니라 부하 생성기 한계일 수 있으니 결과를 그대로 믿지 말 것.

## 측정 방식과 주의점

- Latency = consumer 수신 시각 − producer 발행 직전 시각(같은 JVM 의 `System.nanoTime`). P50/P95/P99/P99.9 를 HdrHistogram 으로 계산.
- `msg/sec` 과 `MB/sec`(payload 기준)를 함께 기록한다.
- 짧은 Run(예: smoke)의 Kafka latency 는 consumer group 가입/rebalance 시간이 섞여 크게 나온다. 본 실험에서는 `warmupSeconds` 를 충분히 두어 제외한다.
- 로컬(macOS Docker Desktop) 결과는 프로덕션을 대표하지 않는다. Threats to Validity 에 `env.json` 을 인용한다.
- `kafka-exporter` 이미지는 셸이 없어 healthcheck 가 없다. Prometheus target 상태(`kafka` job UP)로 확인한다.

## 범위 밖 (후속 SPEC)

3노드 클러스터 프로파일(RF=3, quorum), 장애 주입 자동화, Exp 1~10 본 실험·그래프·논문.
