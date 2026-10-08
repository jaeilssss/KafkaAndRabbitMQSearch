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
| `scenarios/` | 단일 시나리오 YAML (smoke / rate / sweep) |
| `experiments/` | 실험 정의 YAML (Exp 1, Exp 2). 브로커 × 변수값으로 확장되어 자동 실행 |
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
cooldownSeconds: 60         # 발행 종료 후 consumer 가 남은 메시지를 소비하는 동안, 소비가 이 시간 동안 멈추면 중단
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

## 실험 실행 (Exp 1 · Exp 2)

실험 정의(`experiments/*.yml`)는 `base`(공통 Scenario 필드) + `vary`(바꿀 변수 1개와 값 목록) + `profiles`(실행 시간 조건)로 이루어지며,
정의된 브로커마다 변수값 하나가 **포인트**가 된다. 포인트는 프로파일의 `repetitions` 만큼 Run 을 반복해 집계한다.

| 실험 | 변수 | 포인트 수 | 종료 방식 |
|------|------|-----------|-----------|
| `exp1-baseline.yml` | `messageCount` 10K / 100K / 1M (producer 1, consumer 1, 1KB, partition 1 / queue 1) | 6 | 메시지 수 도달 (`warmupMessages` 로 앞부분 제외) |
| `exp2-producer-scaling.yml` | `producers` 1 / 2 / 4 / 8 / 16 / 32 | 12 | 시간 (warm-up + measure) |
| `exp7-message-size.yml` | 메시지 크기 100B / 1KB / 10KB / 100KB / 1MB (크기별 총 약 1GB) | 10 | 메시지 수 도달 |
| `exp-parallelism.yml` | consumer 1/2/4/8/16 × Kafka partition 1/3/12 (RabbitMQ 는 consumer 만) | 20 | 메시지 수 도달 |
| `exp-slow-consumer.yml` | `consumerDelayMs` 0 / 1 / 5 / 10 / 50 (발행 4,000 msg/s 고정) | 10 | 시간 + backlog 소비 완료까지 |
| `exp-durability.yml` | variant: Kafka `acks` 3종, RabbitMQ queue 종류 × confirm 방식 6종 | 9 | 시간 (warm-up + measure) |

각 YAML 상단 주석에 설계 결정과 한계가 적혀 있다. 소요 시간은 `--dry-run` 의 상한 추정이 최악 기준(시간 상한까지 도는 경우)이라 실제보다 훨씬 길게 나온다.

```bash
./scripts/run-experiment.sh experiments/exp2-producer-scaling.yml --profile=quick --dry-run   # 계획과 소요 시간 상한만 출력
./scripts/run-experiment.sh experiments/exp2-producer-scaling.yml --profile=quick             # 개발/검증용 (약 10분)
./scripts/run-experiment.sh experiments/exp2-producer-scaling.yml --profile=full              # 계획서 조건
./scripts/run-experiment.sh experiments/exp1-baseline.yml --profile=quick --brokers=kafka     # 브로커 한정
```

옵션: `--profile=<full|quick>`(필수), `--brokers=kafka,rabbitmq`, `--force`(DONE 포인트도 재실행), `--dry-run`, `--prometheus-url=http://localhost:9090`, `--results-dir=results`.

| 프로파일 | warm-up | measure | cool-down | 반복 | 용도 |
|----------|---------|---------|-----------|------|------|
| `full` | 120s | 300s | 60s | 3회 | 계획서 §24 조건 |
| `quick` | 5s | 15s | 5s | 1회 | 도구/설정 검증 (**결과 해석 금지**) |

- Exp 1 은 개수 기반이라 시간 warm-up 대신 `warmupMessages`(full: 10%, 최소 1,000개 / quick: 1,000개)를 쓰고, `measureSeconds: 600` 은 시간 **상한**(안전장치)일 뿐이다.
- **소요 시간(추정)**: Exp 2 full ≈ 36 Run, 약 5시간. Exp 1 full 은 개수 기반이라 이보다 훨씬 짧다. `--dry-run` 으로 확인한다. 발행이 소비보다 훨씬 빠른 과부하 포인트(예: Kafka producers=32, consumer 1)는 backlog 를 다 소비할 때까지 기다리므로 추정보다 길어질 수 있다.
- `lost` 는 발행 종료 후 소비가 `cooldownSeconds` 동안 멈췄을 때 남은 메시지 수다. 소비가 진행 중이면 backlog 를 끝까지 기다린다(최대 30분). 과부하로 쌓인 backlog 는 latency 에 그대로 반영된다.
- 중단 후 같은 명령을 다시 실행하면 `DONE` 파일이 있는 포인트는 건너뛴다.

### variant 실험 (브로커마다 바꿀 설정이 다를 때)

`vary` 의 변수 이름이 `variant` 이면 값은 이름 붙은 설정 묶음이다. `label` 이 결과 디렉터리 이름이 되고, `broker` 로 특정 브로커에만 적용하며, `set` 은 `base` 위에 깊은 병합으로 덮어쓴다.
예: `experiments/exp-durability.yml` (Kafka `acks` 3종, RabbitMQ queue 종류 × confirm 방식(없음 / 건당 동기 / 100건 배치) 6종 = 9포인트, 단일 브로커 한정).

```yaml
vary:
  variant:
    - { label: acks-all,       broker: kafka,    set: { kafka: { acks: "all" } } }
    - { label: quorum-confirm, broker: rabbitmq, set: { rabbitmq: { queueType: quorum, publisherConfirms: true } } }
```

### 결과 구조

```
results/<experiment>/<profile>/
├── experiment-meta.json            # 정의 SHA-256, 프로파일, git revision, 시작 시각, 환경, 떠 있던 mqt-* 컨테이너
├── experiment-summary.csv          # 포인트당 한 행 (그래프 입력): 처리량 mean/median/stddev, MB/s, P50/P95/P99, lostTotal, generatorSaturated, 자원 지표
└── <broker>__<변수>=<값>/
    ├── <name>-<ts>.json            # Run 마다 (resources 포함)
    ├── <name>-aggregate-<ts>.csv/.json
    ├── point-result.json           # 포인트 집계 (요약 CSV 의 원본)
    └── DONE                        # 완료 표시
```

### 자원 지표 (Prometheus)

각 Run 의 측정 구간에 대해 브로커 컨테이너(`mqt-kafka` / `mqt-rabbitmq`)의 CPU(평균·최대 코어), 메모리 working set 최대, 네트워크 RX/TX 평균 B/s, 디스크 write 평균 B/s 를 조회해 Run JSON 의 `resources` 에 저장한다
(요약 CSV 의 단위는 MB 또는 MB/s). Prometheus 에 연결할 수 없거나 값이 없으면 해당 필드는 null 이고 Run 은 실패하지 않는다.

- Docker Desktop(macOS) 의 cAdvisor 는 컨테이너 이름 라벨이 없어 `docker inspect` 로 얻은 컨테이너 ID(`id="/docker/<id>"`)로 선택한다.
- 같은 환경에서 cAdvisor 는 **컨테이너별 네트워크 지표를 노출하지 않는다**. 그래서 네트워크는 VM 전체 `eth0` 값으로 대체되며, 같은 VM 의 다른 컨테이너 트래픽이 포함될 수 있다(Linux 에서는 컨테이너별 값을 쓴다).
- 구간 종료 후 마지막 scrape(5초 간격)가 반영되도록 6초 기다린 뒤 조회하므로 Run 마다 6초가 더 걸린다. 측정 구간이 10초보다 짧아도 조회 윈도우는 최소 10초다.
- 디스크 write 는 환경에 따라 0 에 가깝거나 없을 수 있다.

### 시계열과 호스트 상태

포인트마다 아래 두 파일이 추가로 저장된다.

- `timeseries.csv` — 포인트 전체 구간(warm-up, cool-down, drain 포함)의 5초 간격 시계열. 브로커 CPU / 메모리 / 디스크 write / 네트워크, 그리고 Kafka 는 `consumerLag`, RabbitMQ 는 `queueDepth`. 값이 없는 시각은 빈 칸이다(consumer group 이 없는 구간의 lag 등). Prometheus 에 연결할 수 없으면 파일을 만들지 않는다.
- `host-state.csv` — 30초 간격 호스트 상태(macOS `pmset`): AC 연결 여부, 배터리 %, 열/성능 경고, CPU speed limit. 경고가 감지되면 로그에 WARN 이 남는다. **Apple Silicon 은 스로틀링 정도를 `pmset` 으로 완전히 노출하지 않으므로, 경고가 없다는 것이 스로틀링이 없었다는 증명은 아니다.** 결과가 의심스러우면 해당 포인트를 `--force` 로 다시 돌린다.

### 브로커 간 간섭 줄이기

두 브로커가 같은 Docker 호스트의 CPU / 디스크를 나눠 쓰므로, 비교용 본 실험은 **한 브로커씩** 실행하고 다른 브로커 컨테이너는 내려 두는 것을 권장한다.
러너는 컨테이너를 기동/중지하지 않으며, 실험 시작 시점에 떠 있던 `mqt-*` 컨테이너를 `experiment-meta.json` 에 기록한다.

한 줄로 자동화하려면 `run-isolated.sh` 를 쓴다. 브로커별로 다른 브로커 컨테이너를 중지하고 대상만 기동한 뒤 실험을 돌리고, 끝나면 전체 스택을 복구한다(macOS 에서는 `caffeinate` 로 잠들기 방지).

```bash
./scripts/run-isolated.sh experiments/exp2-producer-scaling.yml --profile=full                    # kafka -> rabbitmq 순서로 격리 실행
./scripts/run-isolated.sh experiments/exp2-producer-scaling.yml --profile=full --brokers=rabbitmq # 한 브로커만
```

수동으로 하려면:

```bash
docker compose -f docker/docker-compose.yml --profile single stop rabbitmq
./scripts/run-experiment.sh experiments/exp2-producer-scaling.yml --profile=full --brokers=kafka
docker compose -f docker/docker-compose.yml --profile single start rabbitmq && docker compose -f docker/docker-compose.yml --profile single stop kafka kafka-exporter
./scripts/run-experiment.sh experiments/exp2-producer-scaling.yml --profile=full --brokers=rabbitmq
```

## 측정 방식과 주의점

- Latency = consumer 수신 시각 − producer 발행 직전 시각(같은 JVM 의 `System.nanoTime`). P50/P95/P99/P99.9 를 HdrHistogram 으로 계산.
- `msg/sec` 과 `MB/sec`(payload 기준)를 함께 기록한다.
- `warmupMessages` 는 `warmupSeconds` 및 rate 제어(`targetRatePerSec` / `rateSteps`)와 함께 쓸 수 없다. 개수 기반 warm-up 이면 처리량 윈도우는 첫 측정 메시지 발행 시각부터 시작한다.
- 짧은 Run(예: smoke)의 Kafka latency 는 consumer group 가입/rebalance 시간이 섞여 크게 나온다. 본 실험에서는 `warmupSeconds` 를 충분히 두어 제외한다.
- 로컬(macOS Docker Desktop) 결과는 프로덕션을 대표하지 않는다. Threats to Validity 에 `env.json` 을 인용한다.
- `kafka-exporter` 이미지는 셸이 없어 healthcheck 가 없다. Prometheus target 상태(`kafka` job UP)로 확인한다.

## 범위 밖 (후속 SPEC)

3노드 클러스터 프로파일(RF=3, quorum), 장애 주입 자동화, Exp 1~10 본 실험·그래프·논문.
