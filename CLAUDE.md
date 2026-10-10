# Kafka vs RabbitMQ 성능 연구 — 작업 지침

연구 계획서: `~/Downloads/kafka-rabbitmq-performance-study-plan.md` (Exp 1~10). 실험 실행법은 `README.md` 참고.

## 논문 / 보고서 작성 규칙 (반드시 지킬 것)

- **실험마다 "왜 이런 결과가 나왔는가"를 반드시 쓴다.** 표와 그래프만 제시하고 끝내지 않는다. 결과의 원인을 각 시스템(Kafka / RabbitMQ)의 내부 아키텍처와 연결해 설명한다 (계획서 §2 연구 목적, §27 Discussion).
- 설명은 **데이터로 확인된 것**과 **가설(미검증)** 을 구분해서 표시한다.
- 보고서는 `paper/experiment-report-template.md` 형식을 따른다 (연구 계획서 §26 Result 작성 방법, §27 Discussion, §30 논문 구조 기준: 실험 장은 논문 6~13장에 들어간다). "왜 그런가" 항목이 비어 있으면 완성된 것이 아니다.
- 이상치를 이유로 재실행했다면 **첫 실행과 재실행을 모두 보고**한다 (선택적 재실행으로 보이지 않게). 첫 실행 원본은 `results/<exp>/archive/` 에 보존한다.

## 결과 데이터 취급

- `results/<experiment>/<profile>/` 의 `full` 결과는 `--force` 로 덮어쓰지 않는다. 다시 돌려야 하면 기존 폴더를 `archive/` 로 옮긴 뒤 실행한다.
- `quick` 프로파일 결과는 도구 검증용이다. 논문의 근거로 쓰지 않는다.
- 로컬(macOS Docker Desktop) 환경이므로 Threats to Validity 에 `experiment-meta.json` 의 환경 정보를 인용한다.

## 시리즈 구성 (블로그, 2026-10-08 확정)

이 연구는 학술 논문이 아니라 **블로그 시리즈**로 쓴다. 10개 실험을 한 편에 싣지 않고 나눈다.

| 편 | 제목안 | 실험 |
|----|--------|------|
| 1편 | 성능과 확장성: 부하를 올리면 누가 먼저 무너지는가 | Exp 1 Baseline, Exp 2 Producer Scaling, 병렬성(Exp 3+4: consumer × partition), Exp 7 Message Size |
| 2편 | 내구성과 느린 소비자: 안전을 높이면 얼마를 내야 하는가 | `exp-durability`(계획서 RQ6, 번호 없는 신규 실험), 느린 소비자와 backlog(Exp 8+9) |
| 3편(후보) | 튜닝과 장애 복구 | Exp 5 Prefetch, Exp 6 Batching, Exp 10 Failure Recovery(3노드 클러스터 필요) |

- 내구성 실험은 **단일 브로커(RF=1)** 범위다. Kafka `acks=all` 은 복제본이 없어 `acks=1` 과 비슷할 것으로 예상되며, 복제 효과는 한계로 명시한다. 3노드 클러스터 이후 재측정은 3편 이후 과제다.
- Exp 10 을 단일 노드에서 하면 재시작 복구만 잴 수 있다. failover 를 측정한 것처럼 쓰지 않는다.

## 디스크 보호 (중요)

- Docker VM 디스크 여유가 작다(2026-10-08 기준 약 22GB). Kafka 는 소비한 메시지도 토픽을 지울 때까지 로그에 남기므로, 시간 기반 + 무제한 속도 실험은 수십 GB 를 쓸 수 있다.
- 토픽에는 `retention.bytes`(기본 약 4GB, `kafka.retentionBytes` 로 변경)가 걸려 있다(`BrokerTopology`). consumer 가 그보다 더 뒤처지면 소비 전에 메시지가 지워져 `lost` 가 생긴다(Exp 2 1차 full 에서 Kafka producers 8/16/32 가 실제로 이렇게 오염됐다).
  **소비가 못 따라가는 실험은 `messageCount` 상한으로 총량을 제한하고, `kafka.retentionBytes` 를 그 총량보다 크게 올린다** (예: Exp 2 는 1,200만 건 ≈ 12GB, retentionBytes 16GB).
- 새 실험 정의를 만들 때: 예상 발행량(msg/s × 시간 × 메시지 크기)이 수 GB 를 넘으면 `messageCount` 상한을 둔다.
- 실험 시간은 계획서의 보수적 조건(warm-up 120s / measure 300s)을 그대로 따르지 않아도 된다. 반복(3회)을 유지하고 측정 창을 줄이되, 첫 full 결과에서 Run 간 stddev 를 확인한다. 줄였다면 글에 명시한다.
