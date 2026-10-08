# Kafka vs RabbitMQ 블로그 시리즈 개요

연구 계획서(`~/Downloads/kafka-rabbitmq-performance-study-plan.md`)의 실험 10개를 블로그 시리즈로 나눠 쓴다.
각 실험 장의 서식은 `paper/experiment-report-template.md`, 작성 규칙은 `CLAUDE.md` 를 따른다 (실험마다 **"왜 그런가"** 필수).

시리즈 공통 제목(안): **Kafka vs RabbitMQ: 메시징 시스템의 성능, 확장성, 내구성에 대한 실험적 비교**
공통 단서: 로컬 macOS Docker Desktop, 단일 브로커, 특정 버전(`results/*/experiment-meta.json` 참고) 결과다.

---

## 1편. 성능과 확장성: 부하를 올리면 누가 먼저 무너지는가

**독자가 얻어 가는 것**: 내 서비스에 Kafka 와 RabbitMQ 중 무엇이 맞는지, 어디까지 키울 수 있는지. "빠르다" 한 줄이 아니라 조건에 따라 승패가 갈리고, 그 이유가 구조(partition / queue, 디스크 로그 vs 메모리)에 있다는 것.

| 장 | 실험 | 배우는 것 | 상태 |
|----|------|-----------|------|
| 1 | Exp 1 Baseline (`experiments/exp1-baseline.yml`) | 튜닝 없는 기본 처리량·지연 차이 | **full 완료** (`results/exp1-baseline/full/`) |
| 2 | Exp 2 Producer Scaling (`experiments/exp2-producer-scaling.yml`) | 부하 증가 시 포화 지점(saturation point) | 정의 완료, **full 미실행** (약 5시간) |
| 3 | 병렬성 = Exp 3 Consumer Scaling + Exp 4 Partition Scaling | consumer 를 늘리면 빨라지는가, Kafka 는 partition 수가 상한인가 (H3) 정의 완료 (`experiments/exp-parallelism.yml`, 20포인트), quick 검증만 함, **full 미실행** |
| 4 | Exp 7 Message Size (`experiments/exp7-message-size.yml`) | msg/s 는 줄지만 MB/s 는 다르게 움직이는가 (H4) | 정의 완료 (10포인트), quick 검증만 함, **full 미실행** |

Exp 1 에서 지금까지 보인 것(중간 관찰, "왜"는 미검증):
- 1M 에서 Kafka 처리량이 RabbitMQ 의 약 1.65배, 반면 P99 는 Kafka 가 더 높았다.
- RabbitMQ 10K 는 Run 마다 처리량이 7배 이상 흔들렸고 재실행에서도 재현됐다. 첫 실행은 `results/exp1-baseline/archive/` 에 보존돼 있다. **두 실행 모두 보고한다.**
- Kafka 디스크 write 약 57MB/s vs RabbitMQ CPU 약 2코어 (1M 포인트).

---

## 2편. 내구성과 느린 소비자: 안전을 높이면 얼마를 내야 하는가

**독자가 얻어 가는 것**: 안전하게 만들수록 얼마나 느려지는지, 소비자가 못 따라올 때 무슨 일이 벌어지는지. 그리고 **공정한 비교란 무엇인가** (최대 성능 설정 vs 비슷한 안전 수준 설정을 나눠 보는 방법).

| 장 | 실험 | 배우는 것 | 상태 |
|----|------|-----------|------|
| 5 | 내구성 비교 `exp-durability` (`experiments/exp-durability.yml`, 계획서 RQ6 / H8) | 응답 대기·내구성 수준의 성능 비용 | 정의 완료, quick 검증만 함, **full 미실행** (27 Run, 상한 약 3시간 45분) |
| 6 | 느린 소비자와 backlog = Exp 8 Slow Consumer + Exp 9 Backpressure (H7) | backlog 가 쌓이는 모양, 자원 사용, 따라잡는 속도 정의 완료 (`experiments/exp-slow-consumer.yml`, 10포인트), quick 검증만 함, **full 미실행** |

내구성 실험 설계 메모:
- **Benchmark A (최대 성능)**: Kafka `acks-0` / `acks-1`, RabbitMQ `classic-noconfirm`.
- **Benchmark B (비슷한 안전 수준)**: Kafka `acks-all` ↔ RabbitMQ `quorum-confirm-batch`.
- RabbitMQ confirm 은 두 방식을 모두 잰다: `confirm-each`(건당 동기 대기, 최악 비용) / `confirm-batch`(100건 배치). 건당 동기 confirm 은 producer 1개일 때 왕복 시간이 처리량 상한이 되어(quick 에서 약 600~870 msg/s) Kafka 와 직접 비교하면 불공정하다. **배치 크기 100 은 임의로 정한 값이므로 글에 명시한다.**
- **단일 브로커(RF=1) 한계**: Kafka `acks=all` 은 복제본이 없어 `acks=1` 과 비슷할 것으로 예상된다. 복제(RF=3)와 quorum 의 다중 노드 복제 효과는 이 실험 범위가 아니다.

---

## 3편 후보. 튜닝과 장애 복구

| 실험 | 비고 |
|------|------|
| Exp 5 RabbitMQ Prefetch | 정의 파일 없음 |
| Exp 6 Kafka Producer Batching (`linger.ms`, `batch.size`) | 변수 2개, variant 로 설계 필요 |
| Exp 10 Failure Recovery | **3노드 클러스터 + 장애 주입 자동화 필요.** 단일 노드에서는 재시작 복구만 잴 수 있으므로 failover 를 측정한 것처럼 쓰지 않는다. |

---

## 진행 체크리스트

- [x] Exp 1 full
- [ ] Exp 2 full
- [x] 병렬성(Exp 3+4) 실험 정의 (실행은 아직)
- [ ] 병렬성(Exp 3+4) full 실행
- [x] Exp 7 실험 정의 (실행은 아직)
- [ ] Exp 7 full 실행
- [ ] `exp-durability` full
- [x] 느린 소비자 + backlog(Exp 8+9) 실험 정의 (실행은 아직)
- [ ] 느린 소비자 + backlog full 실행
- [ ] 보고서 템플릿을 블로그 형식으로 단순화 (첫 결과 정리 후)
- [ ] 편별 최종 제목 확정
