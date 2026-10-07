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
