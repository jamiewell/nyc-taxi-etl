# Spark 튜닝 실험 결과

`docs/spark_tuning_plan.md`의 8.1 템플릿 양식에 따라 실제 측정 결과를 기록한다. 계획 문서는 방법론만 담고, 실측 결과는 이 문서에 누적한다.

## 요약표

| ID | 실험 | 데이터 | 핵심 변경 | 전체 duration | 정합성 | 결정 |
|---|---|---|---|---:|---|---|
| EXP-00 | Baseline S | yellow 2011-05 (1개월) | 없음 (튜닝 요소 미적용) | 1,158.0초 | PASS | 기준값 확정 |
| EXP-01 | AQE off + Worker t2.medium | yellow 2011-05 (1개월, EXP-00과 동일) | `spark.sql.adaptive.enabled=false` + executor-memory 768MB→2560MB | 2,742.9초 (**+136.9%**) | PASS | **AQE off 기각**, 메모리 증설은 유지 |
| EXP-02 | AQE on(복원) + Worker t2.medium 유지 | yellow 2011-05 (1개월, EXP-00/01과 동일) | AQE 기본값 복원, executor-memory 2560MB 유지 (memory 단독 효과 분리) | **1,062.7초 (EXP-00 대비 -8.2%)** | PASS | **새 기준선(Baseline S v2)으로 채택** |
| EXP-03 | Baseline M (AQE 명시적 off) | yellow 2024-01~12 (12개월, 최초) | `spark.sql.adaptive.enabled=false` 명시 지정, t2.medium 유지 | 6,397.8초 (1h46m) | PASS (수정 후) | Baseline M 확정, 12개월 규모에서도 AQE off의 200-task 패턴 재확인 |
| EXP-04 | Column Pruning (계획서 EXP-01) | yellow 2011-05 (1개월, EXP-02와 동일) | Cleaned Layer 읽기 직후 `select_columns`로 필요 컬럼만 명시 선택 | 1,693.0초 (측정치, 해석은 본문 참고) | PASS | **채택 보류** — 0컬럼 pruning 확인, duration 차이는 인프라 노이즈로 판단 |

## EXP-00: Baseline S (튜닝 없음)

### 기본 정보

- 실행 ID / Spark Application ID: `app-20260819093713-0001`
- 실행 일시: 2026-08-19 09:37 ~ 09:56 UTC
- 코드 commit: `17de968`
- 실행 환경: AWS EC2 수동 Spark 클러스터 (`docs/aws_infra_setup.md`)
- Spark 버전: 3.5.3
- 데이터 범위: yellow 2011-05 (1개월)
- 입력 크기 / 파일 수 / row 수: 원본 소스 parquet 1개 (S3), 15,554,868 rows
- cluster 구성: t2.small 4대 (master 1 + worker 3)
- executor 수 / cores / memory: worker 3대 × 1 core / 768MB, driver 512MB (총 executor core 3개)

### 가설

없음 — 튜닝 요소를 하나도 적용하지 않은 순수 기준 실행. `spark.sql.optimizer.plannedWrite.enabled=false`만 이미 코드에 반영되어 있어 유지(사용자가 이전에 별도로 추가한 설정, 이번 실험의 튜닝 변수 아님).

### 변경 사항

- Baseline 설정: 프로젝트 기본 `spark-submit` 명령 그대로 (`docs/aws_infra_setup.md` "S3 데이터 처리용 표준 spark-submit 명령" 참고), `--taxi-types yellow --start-year-month 2011-05 --end-year-month 2011-05`
- 명시적으로 설정한 것: `spark.sql.sources.partitionOverwriteMode=dynamic`, `spark.sql.optimizer.plannedWrite.enabled=false` (둘 다 이번 실험 이전부터 있던 기존 기본값, 새로 추가 안 함)
- 명시적으로 설정하지 않은 것 (= Spark 기본값 사용, Environment API의 `sparkProperties`에 노출 안 됨): `spark.sql.shuffle.partitions`, `spark.sql.adaptive.enabled`, `spark.sql.autoBroadcastJoinThreshold`, `repartition`/`coalesce`, `cache`/`persist`, broadcast hint

> Spark UI Environment 탭(REST API)은 **명시적으로 override된 설정만** 노출한다. 위 항목들은 코드/CLI 어디에도 지정되지 않았으므로 Spark 3.5.3의 내장 기본값(공식 문서 기준 `shuffle.partitions=200`, `adaptive.enabled=true`, `autoBroadcastJoinThreshold=10MB`)이 그대로 적용된 것으로 추정하되, 실행 계획(SQL 탭)에서 `AdaptiveSparkPlan`/`AQEShuffleRead`/`BroadcastHashJoin` 노드가 실제로 관측되어 AQE와 broadcast join이 동작 중임은 직접 확인함 (아래 "Spark UI 관찰" 참고).

### 결과

| 지표 | 값 | 비고 |
|---|---:|---|
| 전체 duration | **1,158.0초** (19분 18초) | History Server 기준 |
| 최대 병목 stage | 666.8초 (stage 49) | 전체의 57.6% |
| 2위 병목 stage | 319.3초 (stage 76) | 전체의 27.6% |
| 상위 2개 stage 합산 | 986.1초 | 전체의 **85.1%** |
| stage 49 input | 1,991.1 MB | 17 tasks |
| stage 76 input / shuffle write | 817.5 MB / 448.4 MB | 24 tasks |
| 총 memory spill | **53,426 MB (52.2 GB)** | 소스 193MB짜리 job치고 매우 큼 |
| 총 disk spill | **11,482 MB (11.2 GB)** | |
| executor별 총 task 수 | 442 / 446 / 470 | 균등 분배, skew 없음 |
| executor별 GC time | 45.4 / 57.1 / 58.4초 | executor 총 실행시간(850~880초) 대비 5.3~6.8% |
| 실패/재시도 task | 0 | 안정적 |
| 출력 파일 (Raw) | 1개, 253.8 MB | |
| 출력 파일 (Cleaned) | 2개, 301.7 MB | |
| 출력 파일 (Fact, 누적) | 4개, 420.1 MB | **주의**: Fact는 지금까지 처리한 2011-01~05 전체 누적본, 이달만의 크기 아님 |
| fact_taxi_trip 총 row (누적) | 100,702,104 | 2011-01~05 누적 (Fact는 cumulative 설계) |

### 정합성 검증

- row count: 15,554,868 → 필터 후 15,140,156 (Pass rate 97.33%)
- Mart Count/Amount validation: 4개 Mart 전부 `PASSED` (로그 확인)
- schema/파티션별 건수: 이번 실행에서는 별도 재검증 안 함 (Baseline이라 기존 파이프라인의 내장 validation만 확인)
- 검증 결과: **PASS**

### Spark UI 관찰

**병목 stage의 정체 (SQL 탭 physical plan 대조)**

- stage 49 (666.8초, 최대 병목) — SQL id=17, 물리 계획: `Scan parquet → ColumnarToRow → Project → Project → InsertIntoHadoopFsRelationCommand`. **Sort/Exchange 노드 없음.** 즉 이 stage는 groupBy/join 셔플이 아니라 단순 read-transform-write이며, 사용자가 원래 의심했던 "`partitionBy`가 자동으로 유발하는 정렬"은 이 stage에서는 관측되지 않았다(`plannedWrite.enabled=false`가 실제로 sort 노드를 없앤 것으로 보이나, 이번 Baseline엔 비교 대상인 "true" 버전 실행이 없어 단정할 수 없음 — 별도 EXP로 검증 필요).
- 그럼에도 이 stage 하나에서 memory spill 28GB, disk spill 7.5GB가 발생. Sort 없이 read→write만 하는 stage에서 이 정도 spill이 나온다는 것은, **셔플/정렬이 아니라 순수 파일 처리(디코딩·버퍼링) 자체가 768MB executor 메모리에 비해 과도**하다는 뜻으로 해석됨.
- stage 76 (319.3초) — SQL id=23, 물리 계획에 `BroadcastExchange`, `BroadcastHashJoin`, `Exchange`, `AQEShuffleRead`, `ObjectHashAggregate`, `AdaptiveSparkPlan` 포함. Mart 생성(zone/vendor 집계 + dimension broadcast join) 단계로 추정. **AQE가 명시적 설정 없이도 기본값으로 동작 중임을 여기서 직접 확인.**

**전체 실행시간의 85%가 상위 2개 stage에 집중** — 나머지 300여 개 stage는 대부분 수 초~수십 초 수준. 즉 이번 파이프라인의 성능은 "여러 stage가 골고루 느린" 구조가 아니라 **소수의 특정 write/집계 stage에 병목이 몰려 있는** 구조.

**executor 활용도**: 3개 executor 모두 task 수(442~470)와 GC time이 고르게 분포 — executor 간 skew는 없음. 다만 **총 executor core가 3개뿐**이라 병렬성 자체가 근본적으로 제한적.

- event log 위치: `/home/ubuntu/spark-events/app-20260819093713-0001` (마스터 노드, `docs/known_issues_and_fixes.md` #10 참고 — stop/start에도 유지됨)

### 결론

- 결과: Baseline 확정 (튜닝 판정 대상 아님, 최초 기준값)
- 판단 근거: 정합성 PASS, 재현 가능한 조건(코드 커밋/클러스터 구성/데이터 범위)으로 기록 완료
- 다음 실험 후보 (관찰된 근거 기반, 우선순위순):
  1. **EXP-06 (Persist/Cache) 재검토**: stage 49의 input 1,991MB가 원본 소스 193MB의 약 10배 — `read_source_data`/`transform_to_cleaned` 등에서 동일 데이터를 여러 번 다시 읽거나 `count()` 액션이 반복 호출되는 것이 원인일 가능성. SQL 탭에서 동일 source의 반복 `FileScan` 여부를 직접 대조하는 검증이 필요.
  2. **executor memory 상향 조정**: 768MB executor에서 정렬/셔플 없는 단순 write조차 28GB spill이 나는 것은 명백한 메모리 압박 신호. t2.small의 물리적 한계(2GB RAM) 안에서 executor-memory를 얼마나 올릴 수 있는지, 혹은 인스턴스 타입 자체를 올려야 하는지 확인 필요 (EXP-03/EXP-05 전에 선행되어야 할 수 있음 — 계획서 우선순위에 없던 항목이지만 이번 Baseline에서 가장 두드러진 신호).
  3. **EXP-01 Column Pruning**: raw_to_cleaned가 전체 컬럼을 읽는지, 필요한 컬럼만 select하는지 실행 계획의 `ReadSchema`로 확인.
  4. `plannedWrite.enabled=false`의 실제 효과를 검증하는 A/B 비교 (true로 되돌린 버전과 동일 데이터로 재실행) — 사용자가 원래 제기한 가설을 Baseline만으로는 판정할 수 없으므로 별도 EXP로 분리 필요.
- 부작용/주의점: 이번 실행 도중 워커 노드 디스크가 84~85%까지 차서 `No space left on device`로 첫 시도가 실패했다 (누적된 이전 job들의 `pyspark/work/app-*` 디렉터리, job당 270MB × 누적 10개 이상). 정리 후 `spark.worker.cleanup.enabled=true`(interval 30분, appDataTtl 1시간)를 워커에 적용해 재발 방지 — 이후 모든 실험에 이 설정이 유지된 채로 진행됨. 이것도 일종의 "인프라가 실험 결과를 왜곡시킬 뻔한" 사례라 기록해둔다.

## EXP-01: AQE off + Worker t2.medium

### 기본 정보

- 실행 ID / Spark Application ID: `app-20260819114158-0002`
- 실행 일시: 2026-08-19 11:41 ~ 12:27 UTC
- 코드 commit: `17de968` (코드 변경 없음, 클러스터/실행 설정만 변경)
- 실행 환경: AWS EC2 수동 Spark 클러스터
- Spark 버전: 3.5.3
- 데이터 범위: yellow 2011-05 (1개월) — **EXP-00과 동일 데이터, 동일 코드**
- 입력 크기 / 파일 수 / row 수: EXP-00과 동일 (15,554,868 rows)
- cluster 구성: master t2.small 1대(변경 없음) + **worker 3대 t2.medium**(t2.small → 상향, 1 vCPU/2GB → 2 vCPU/4GB)
- executor 수 / cores / memory: worker 3대 × 1 core / **2560MB**(768MB → 상향), driver 512MB (총 executor core 3개, 변경 없음)

### 가설

1. executor-memory를 늘리면(768MB→2560MB) EXP-00에서 관측된 대량 spill(52GB)이 줄어 duration이 개선될 것이다.
2. AQE를 끄면 실행 조건이 더 예측 가능해지고, 이후 EXP들(shuffle partitions 등)의 개별 효과를 순수하게 측정할 수 있는 통제된 기준선이 될 것이다.

### 변경 사항

- Baseline 대비 변경: `--conf spark.sql.adaptive.enabled=false` 추가, `--executor-memory 768m` → `2560m`, 워커 인스턴스 t2.small → t2.medium
- 고정한 조건: 데이터 범위(2011-05), 코드 커밋, executor-cores(1), driver-memory(512m), `plannedWrite.enabled=false`, `partitionOverwriteMode=dynamic`, master 스펙

**⚠️ 방법론 위반 기록**: 계획서 3.1 "설정 하나의 효과를 확인할 때는 다른 주요 설정을 동시에 변경하지 않는다"를 이번 실험은 지키지 않았다 — executor 메모리와 AQE를 **동시에** 바꿨다. 사용자가 두 변경을 한 번에 적용해달라고 명시적으로 요청해 그대로 진행했고, 그 결과 두 변화가 duration에 각각 얼마나 기여했는지는 이 실험만으로 분리할 수 없다. 아래 결론에서 최대한 stage 단위 증거로 원인을 구분했지만, 엄밀한 개별 검증은 후속 EXP(메모리만 변경 / AQE만 변경)로 남겨둔다.

### 결과

| 지표 | EXP-00 (기준) | EXP-01 | 증감 | 비고 |
|---|---:|---:|---:|---|
| 전체 duration | 1,158.0초 | **2,742.9초** | **+136.9%** (악화) | |
| 총 memory spill | 53,426 MB | 23,936 MB | **-55.2%** (개선) | |
| 총 disk spill | 11,482 MB | 6,367 MB | **-44.5%** (개선) | |
| executor별 GC time | 45.4~58.4초 | 6.6~7.3초 | **-87%** (개선) | |
| executor maxMemory | 278 MB | 1,422 MB | +411% | 의도한 증설 반영됨 |
| 전체 task 수 | 3,299개 | **11,846개** | **+259%** (악화) | |
| numTasks≥100인 stage 수 | 0개 | **52개** | | AQE coalescing 소실의 직접 증거 |
| 그 52개 stage의 총 실행시간 | - | 1,052.1초 | | 전체 duration의 38.4% |
| 실패/재시도 task | 0 | 0 | - | 둘 다 안정적 |

### 정합성 검증

- Mart Count/Amount validation: 4개 Mart 전부 `PASSED`
- 검증 결과: **PASS**

### Spark UI 관찰

- **가설 1(메모리 증설)은 확인됨**: memory spill -55%, disk spill -45%, GC time -87%. 768MB→2560MB 증설이 의도대로 스필/GC 압박을 줄였다.
- **가설 2(AQE off가 통제된 기준선을 만들 것)는 기각됨, 역효과 발생**: EXP-00(AQE on)은 `numTasks≥100`인 stage가 **0개**였다 — AQE의 partition coalescing이 이 데이터 규모(1개월, ~200MB급)에 맞춰 shuffle partition을 자동으로 줄여줬기 때문. AQE를 끄자 `spark.sql.shuffle.partitions` 기본값 **200**이 그대로 적용되어 shuffle이 있는 stage마다 200개 task가 생겼고(52개 stage, 총 10,300 task), 워커 3코어로 이걸 순차 처리하느라 스케줄링/직렬화 오버헤드만 누적됐다. 전체 task 수가 3,299 → 11,846개(+259%)로 폭증한 게 직접적인 증거.
- 즉 duration 악화(+136.9%)는 메모리 증설의 개선분을 AQE off의 역효과가 훨씬 크게 상쇄하고도 남은 결과로 해석된다. 메모리 단독 효과만 봤다면 EXP-00보다 빨라졌을 가능성이 높다(후속 EXP로 확인 필요).
- event log 위치: `/home/ubuntu/spark-events/app-20260819114158-0002`

### 결론

- 결과: **AQE off는 기각.** executor-memory 증설(t2.medium)은 spill/GC 개선 효과가 뚜렷하므로 **유지**.
- 판단 근거: task 수 폭증(+259%)과 `numTasks≥100` stage 신규 발생(0→52개)이 AQE coalescing 소실과 시점·규모 모두 일치. duration 악화의 주된 원인으로 판단.
- 적용 범위: 앞으로의 실험은 **AQE는 다시 켠 상태**를 통제 조건으로 되돌리고, 메모리 증설(t2.medium, executor-memory 2560m)만 유지한 채 진행한다. `docs/aws_infra_setup.md` 표준 명령에서 `spark.sql.adaptive.enabled=false` 제거 필요 (아직 미반영, TODO).
- 부작용/주의점: 이번 실험이 방법론(변수 1개씩 변경)을 어긴 채 두 변수를 동시에 바꿔서, 정량적 기여도 분리는 안 됨. 정성적으로는 stage/task 지표가 AQE 쪽 원인을 명확히 가리킨다.
- 다음 실험:
  1. **AQE 다시 켜고 t2.medium만 유지한 상태로 Baseline 재측정** (EXP-02) — 메모리 증설의 순수 효과를 EXP-00과 비교해 분리.
  2. EXP-07(AQE on/off 비교)은 이미 사실상 이번 실험에서 강한 신호를 얻었으므로, 우선순위를 계획서 순서보다 앞당길 가치가 있음.
  3. EXP-00 결론에서 제기했던 "input이 소스의 10배"(반복 read 의심) 여부는 이번 실행에서도 stage 35(구 stage 49와 동일 위치, input 1,986MB)로 재현되어 여전히 유효한 의문 — Persist/Cache 실험(EXP-06)으로 이어서 확인.

## EXP-02: AQE 복원 + Worker t2.medium 유지 (메모리 단독 효과 분리)

### 기본 정보

- 실행 ID / Spark Application ID: `app-20260820060407-0000`
- 실행 일시: 2026-08-20 06:04 ~ 06:22 UTC
- 코드 commit: `d307a3e` (코드 변경 없음, 실행 설정만 변경)
- 실행 환경: AWS EC2 수동 Spark 클러스터
- Spark 버전: 3.5.3
- 데이터 범위: yellow 2011-05 (1개월) — EXP-00, EXP-01과 동일 데이터/코드
- 입력 크기 / 파일 수 / row 수: EXP-00과 동일 (15,554,868 rows)
- cluster 구성: master t2.small 1대 + worker 3대 t2.medium (EXP-01과 동일, 변경 없음)
- executor 수 / cores / memory: worker 3대 × 1 core / 2560MB (EXP-01과 동일), driver 512MB

### 가설

EXP-01에서 두 변수(메모리 증설 + AQE off)를 동시에 바꿔 개별 효과를 분리하지 못했다. AQE만 원래대로 되돌리고 메모리 증설은 유지하면, "메모리 증설 단독 효과"가 EXP-00 대비 순수하게 드러날 것이다 — EXP-01 결론에서 예측한 대로 EXP-00보다 빨라질 것으로 예상.

### 변경 사항

- EXP-01 대비 변경: `spark.sql.adaptive.enabled=false` 제거 (AQE 기본값 = on으로 복원)
- EXP-01에서 유지: executor-memory 2560m, worker t2.medium, `spark.worker.cleanup` 설정
- EXP-00과 고정 조건 동일: 데이터 범위, 코드, executor-cores(1), driver-memory(512m), `plannedWrite.enabled=false`, `partitionOverwriteMode=dynamic`

이번에는 변수 하나(AQE)만 EXP-01에서 되돌린 것이므로, EXP-01과 비교하면 "AQE 복원 단독 효과", EXP-00과 비교하면 "메모리 증설 단독 효과"(AQE는 두 실행 모두 on)를 각각 분리해서 볼 수 있다.

### 결과 (3-way 비교)

| 지표 | EXP-00 (t2.small, AQE on) | EXP-01 (t2.medium, AQE off) | EXP-02 (t2.medium, AQE on) | EXP-02 vs EXP-00 |
|---|---:|---:|---:|---:|
| 전체 duration | 1,158.0초 | 2,742.9초 | **1,062.7초** | **-8.2%** |
| 총 memory spill | 53,426 MB | 23,936 MB | 23,936 MB | -55.2% |
| 총 disk spill | 11,482 MB | 6,367 MB | 6,366 MB | -44.6% |
| executor별 GC time | 45.4~58.4초 | 6.6~7.3초 | **8.5~9.5초** | -83% |
| executor maxMemory | 278 MB | 1,422 MB | 1,422 MB | +411% |
| 전체 task 수 | 3,299개 | 11,846개 | **3,299개** | 0% (동일) |
| numTasks≥100인 stage 수 | 0개 | 52개 | **0개** | 0% (동일) |
| 최대 병목 stage(stage 49류) | 666.8초 | 588.2초(stage 35) | 595.9초 | -10.6% |
| 실패/재시도 task | 0 | 0 | 0 | - |

### 정합성 검증

- row count: 15,554,868 → 필터 후 15,140,156 (Pass rate 97.33%) — **EXP-00과 완전 동일**
- fact_taxi_trip 누적 row: 100,702,104 — EXP-00과 완전 동일 (같은 2011-05 파티션을 dynamic overwrite로 재생성했으므로 당연한 결과)
- Mart Count/Amount validation: 4개 Mart 전부 `PASSED`
- 검증 결과: **PASS**

### Spark UI 관찰

- **가설 확인됨**: AQE를 되돌리자 task 수(3,299개)와 `numTasks≥100` stage 수(0개)가 EXP-00과 정확히 일치 — AQE의 partition coalescing이 정상적으로 복원되어 EXP-01의 task 폭증(11,846개)이 사라졌다.
- 그 상태에서 memory spill(-55%), disk spill(-45%), GC time(-83%)은 EXP-01과 거의 동일하게 유지 — 메모리 증설 효과가 AQE 상태와 무관하게 그대로 살아있다는 뜻.
- 결과적으로 **duration이 EXP-00 대비 8.2% 개선**됐다. EXP-01 대비로는 **61.3% 개선**(2,742.9초 → 1,062.7초) — 이 차이는 거의 전적으로 AQE 복원 덕분으로 귀속 가능(메모리 조건은 EXP-01/EXP-02 사이에 변화 없음).
- 최대 병목 stage(read→write, Sort/Exchange 없음)는 여전히 존재하고 여전히 spill이 크다(19,456MB) — 메모리를 3배 이상 늘렸는데도 이 stage의 spill이 크게 안 줄어든 건, 이 stage의 메모리 압박이 executor 전체 메모리보다 **단일 task/파티션 단위의 처리량**에서 비롯된 것일 가능성을 시사한다. EXP-00 결론에서 제기했던 "반복 read 의심"과 함께 Persist/Cache, Column Pruning 실험에서 더 파봐야 함.
- event log 위치: `/home/ubuntu/spark-events/app-20260820060407-0000`

### 결론

- 결과: **채택 — 이 조건(t2.medium worker + executor-memory 2560m + AQE on)을 새 기준선(Baseline S v2)으로 삼는다.**
- 판단 근거: EXP-00 대비 duration 개선(-8.2%), spill/GC 대폭 개선, task 분포는 EXP-00과 동일(AQE 정상 동작), 정합성 완전 일치.
- 적용 범위: `docs/aws_infra_setup.md` 표준 명령에서 `spark.sql.adaptive.enabled=false`를 제거해 AQE 기본값(on) 상태로 되돌린다 (TODO, 아직 미반영).
- 부작용/주의점: t2.medium은 t2.small 대비 시간당 비용이 더 높다(온디맨드 기준 약 4배) — 실험 목적상 유지하되, 상시 운영 전환 시 비용 재검토 필요.
- 다음 실험: 이 조건(worker t2.medium, AQE on, executor-memory 2560m)을 고정 조건으로 삼아 계획서 순서대로 EXP-03(shuffle partitions)부터 진행. 최대 병목 stage의 "반복 read 의심"은 EXP-06(Persist/Cache)에서 우선 검증.

## EXP-03: Baseline M (12개월, yellow 2024-01~12, AQE 명시적 off)

### 기본 정보

- 실행 ID / Spark Application ID: `app-20260820140321-0002` (성공 실행; 최초 시도 `app-20260820080503-0001`는 데이터 무결성 버그로 재실행)
- 실행 일시: 2026-08-20 14:03 ~ 15:49 UTC
- 코드 commit: `e82042f` 이후 (이번 실험 중 `jobs/main.py`, `jobs/raw_to_cleaned.py`, `jobs/save_raw_layer.py` 수정, 아직 미커밋)
- 실행 환경: AWS EC2 수동 Spark 클러스터, master t2.small + worker 3대 t2.medium
- Spark 버전: 3.5.3
- 데이터 범위: yellow 2024-01 ~ 2024-12 (12개월) — 이 프로젝트 최초의 다개월(월 수 두 자릿수) 배치
- 입력 크기 / 파일 수 / row 수: 소스 parquet 12개(월 50~64MB, 총 ~693MB), 총 41,169,720 rows
- executor 수 / cores / memory: worker 3대 × 1 core / 2560MB, driver 512MB

### 가설

없음(정식 실험 가설이 아니라 정기 규모 확장 Baseline). 다만 사용자가 "AQE를 명시적으로 끄고" 요청했으므로, EXP-01에서 1개월 규모로 확인된 "AQE off의 200-task 패턴"이 12개월 규모에서도 동일하게 나타나는지가 관찰 포인트.

### 사전 발견 및 조치: 데이터 무결성 버그 (최초 시도 실패)

**증상**: 첫 시도(`app-20260820080503-0001`)에서 Raw/Cleaned/Fact까지는 로그상 전부 "COMPLETE"였으나, S3에 저장된 실제 결과를 대조해보니 `year=2024/month=1`~`month=11` 파티션이 각각 6~7KB(원본 대비 사실상 0에 가까움)였고, 가장 마지막에 처리된 `month=12`만 정상 크기(72MB)였다.

**원인**: `main.py`의 STEP 1이 12개월을 순차 반복하며 매번 `save_to_raw_layer`로 **같은 raw_layer_path에 대해 dynamic partition overwrite**를 수행했는데, TLC 원본 월별 parquet 파일은 100% 그 달로만 채워져 있지 않고 극소수(파일당 10~40건 수준)의 경계 노이즈 행(예: `2024-02` 파일 안에 실제 `pickup_datetime`이 1월인 행)을 포함한다. dynamic overwrite는 "그 실행에서 실제로 쓰인 파티션"을 교체하므로, `2024-02`를 처리하는 시점에 그 파일 속 소수의 1월 행이 `year=2024/month=1` 파티션에 dynamic overwrite로 쓰이면서, 직전에 이미 써둔 진짜 1월 데이터(수백만 건)를 그 몇 건짜리 파티션으로 **완전히 덮어썼다**. 이 패턴이 2월→3월→...→12월까지 연쇄적으로 반복되어, 결국 가장 나중에 쓰인 12월만 온전히 남고 나머지는 전부 소실됨.

부수적으로 같은 시도에서 `vendor_id` 컬럼도 `#8-9`와 같은 클래스의 물리 타입 불일치(2024-12 파일만 INT32, 나머지는 bigint)가 있어 Fact 빌드 단계에서 크래시가 났었음 — 이건 이번 실행에서 함께 고쳤음(아래 조치 2).

**조치 1 (파티션 덮어쓰기 충돌 해결)**: `jobs/save_raw_layer.py`에 `save_month_to_raw_layer()`를 신설. 기존처럼 `year(pickup_col)`/`month(pickup_col)`로 파생한 동적 파티션에 쓰는 대신, **소스 파일의 지정된(nominal) 연/월을 명시적 경로(`.../year=Y/month=M`)로 직접 지정해서 씀** (`df.write.mode("overwrite").parquet(explicit_path)`, `partitionBy` 미사용). 이렇게 하면 한 달 처리 시 그 달의 디렉터리만 건드리므로 다른 달과 절대 충돌하지 않고, 필터링도 하지 않으므로(모든 행 보존) "Raw 데이터는 수정하지 않는다" 원칙도 그대로 유지됨. `main.py` STEP 1 루프가 이 함수를 쓰도록 변경.

**조치 2 (vendor_id 등 정수 컬럼 타입 불일치)**: `#9`의 `_round_double()`과 동일한 패턴으로 `_cast_long()` 헬퍼를 추가, `vendor_id`/`pickup_location_id`/`dropoff_location_id`/`ratecode_id`/`payment_type`/`passenger_count`에 전부 적용해 `LongType`으로 고정.

**검증**: 로컬에서 "1월 파일(5건, 전부 1월)" + "2월 파일(6건 = 2월 5건 + 1월 경계 노이즈 1건)"을 합성해 재현 — 수정 전 코드였다면 1월 파티션이 노이즈 1건으로 덮어써졌을 상황을, 수정 후에는 1월 5건 그대로 유지 / 2월 6건(노이즈 포함, 소실 없음) 확인. 이후 클러스터에서 12개월 재실행 → 전 달이 58~75MB로 고르게 채워진 것으로 최종 확인(아래 결과 참고).

### 결과

| 지표 | 값 | 비고 |
|---|---:|---|
| 전체 duration | **6,397.8초 (1시간 46분 38초)** | 12개월, 41.2M rows |
| 최대 병목 stage | 1,173.0초 (stage 186, 200 tasks) | AQE off로 인한 정적 shuffle.partitions=200 stage |
| 2위 병목 stage | 848.1초 (stage 167, 27 tasks, input 2,796MB) | read→write 계열, EXP-00/01/02의 "반복 read 의심" stage와 동일 계열 |
| numTasks≥100인 stage 수 | **52개** (EXP-01/02의 1개월 실험과 정확히 동일한 개수) | AQE off 시 stage/task **개수**는 데이터量과 무관하게 고정, task당 처理量만 늘어남 |
| 그 52개 stage의 총 실행시간 | 2,368.2초 | 전체의 37.0% |
| 총 memory spill | 21,888 MB | |
| 총 disk spill | 5,869 MB | |
| executor별 총 task 수 | 3,899 / 3,938 / 3,951 (총 12,795) | 균등 분배, skew 없음 |
| executor별 GC time | 10.7~12.3초 | executor 총 실행시간(2,023~2,060초) 대비 0.5~0.6% — 매우 낮음 |
| 실패/재시도 task | 0 | 안정적 |
| 소스 rows | 41,169,720 (12개월 합) | |
| fact_taxi_trip 누적 rows | 136,329,160 | 2011년치 4개월 + 2024년치 12개월 누적 |
| raw 파티션 크기 (월별) | 58.2~75.0 MiB, 12개월 균등 | 버그 수정 후 정상 분포 확인 |

### 정합성 검증

- Mart Count/Amount validation: 4개 Mart 전부 `PASSED` (월별 monthly validation 포함)
- raw layer 12개 파티션 크기 직접 대조: 전부 정상 범위(58~75MiB), 이상치 없음
- 검증 결과: **PASS**

### Spark UI 관찰

- **EXP-01의 "AQE off → task 폭증" 패턴이 12개월 규모에서도 정확히 재현됨**: `numTasks≥100` stage 52개, 그 stage들의 총 task 수 10,300개 — **EXP-01(1개월)과 완전히 같은 숫자**. 즉 AQE off일 때 shuffle stage/task **개수**는 정적 `spark.sql.shuffle.partitions=200` 설정에만 좌우되고 데이터 크기와는 무관하며, 데이터가 늘어나면 그만큼 **task당 처리량**만 커진다(1개월 대비 12배 데이터를 같은 수의 task가 나눠 처리).
- GC time 비중(0.5~0.6%)이 1개월 실험(EXP-02, ~1%)보다도 낮음 — executor-memory 2560MB가 12개월 규모 처理에도 여유가 있다는 뜻.
- 여전히 최대 병목은 read→write 계열 stage(1,173초, 848초 두 개가 상위) — EXP-00부터 계속 제기된 "반복 read/과도한 buffering 의심"이 이번에도 재현됨. Column Pruning(EXP-04)과 Persist/Cache(EXP-06)로 이어서 확인 필요.
- event log 위치: `/home/ubuntu/spark-events/app-20260820140321-0002`

### 결론

- 결과: Baseline M 확정 (12개월 규모의 정식 기준값)
- 판단 근거: 정합성 PASS, 버그 수정 후 데이터 무결성 직접 검증 완료
- 부작용/주의점(중요): 이번에 고친 **파티션 덮어쓰기 충돌 버그**는 지금까지의 모든 실험(EXP-00~02, 1~4개월 규모)에서는 우연히 재현되지 않았을 뿐, **다개월을 한 번에 처리하는 모든 시나리오에 잠재된 위험**이었다. 월 수가 많아지고 연속된 달을 처리할수록 재현 확률이 올라간다 — 이번 12개월 처리가 처음으로 확실히 걸린 사례. `docs/known_issues_and_fixes.md` #12에 별도 기록.
- 다음 실험: 이 조건(12개월, AQE off, t2.medium)을 그대로 두고 EXP-04(Column Pruning)부터 이어가거나, 혹은 AQE on으로 되돌린 "진짜" Baseline M(EXP-02처럼 memory 단독 효과 재확인)을 먼저 잡을지는 다음 지시에 따름.

## EXP-04: Column Pruning (계획서 EXP-01, `exp01-column-pruning` 브랜치)

> 계획서(`docs/spark_tuning_plan.md`)의 실험 번호는 "EXP-01"이지만, 이 결과 문서에서는 이미 다른 실험이 EXP-01(AQE off + t2.medium)을 선점하고 있어 **EXP-04**로 기록한다. 코드/브랜치명은 사용자 요청대로 `exp01-column-pruning`을 유지.

### 기본 정보

- 브랜치: `exp01-column-pruning` (base: `main` `0753b5a`) — **main은 전혀 수정하지 않음**
- 커밋: `ea5f739`(초기 구현), `5ac8b6f`(대소문자 매칭 버그 수정)
- 실행 ID / Spark Application ID: `app-20260821042041-0001` (성공; 최초 시도 `app-20260821041701-0000`는 컬럼명 대소문자 버그로 실패, 78초 만에 크래시)
- 실행 일시: 2026-08-21 04:20 ~ 04:48 UTC
- 실행 환경: AWS EC2, master t2.small + worker 3대 t2.medium (EXP-02와 동일 스펙)
- 데이터 범위: yellow 2011-05 (1개월) — **EXP-02와 동일**
- executor 수 / cores / memory: worker 3대 × 1 core / 2560MB, driver 512MB, **AQE는 기본값(on)** — EXP-02와 동일 조건

### 가설

계획서 EXP-01 원문 가설: "ETL에 필요한 컬럼만 select하면 Parquet에서 읽는 데이터와 메모리 사용량이 감소한다."

### 실험 범위 (사용자 지정)

이번 실험은 **raw→cleaned 전처리·저장 구간에만** 한정한다 — Raw Layer 저장(원본 그대로 보존), Fact/Dim, Mart는 손대지 않음. 최초 베이스라인(main)의 `_transform_yellow`는 이미 필요한 출력 컬럼을 `.select()`로 명시하고 있었으므로, 이번 실험의 실질적 변경은 "그 select 시점을 Cleaned 변환 마지막 단계에서 **Raw 읽기 직후**로 앞당기는 것"이다 — `raw_to_cleaned.read_raw_data()`에 `select_columns` 파라미터를 추가해, `.count()`/`.filter()` 등 어떤 액션보다도 먼저 컬럼 projection이 걸리도록 함.

### 구현

- `jobs/raw_to_cleaned.py`: 타입별 실제 필요 원본 컬럼 목록(`REQUIRED_SOURCE_COLUMNS`) 정의. `read_raw_data(spark, input_path, select_columns=None)` — 지정 시 읽기 직후 `.select()` 수행, `df.columns`에 실제로 없는 컬럼은 조용히 스킵.
- `jobs/main.py`: Cleaned Layer 읽기 호출부(범위 모드/단일 모드 둘 다)에 `select_columns=REQUIRED_SOURCE_COLUMNS.get(taxi_type)` 전달.

### 개발 중 발견한 버그 (실험 자체와는 별개)

**1차 시도 실패** (`app-20260821041701-0000`, 78초): `Airport_fee` 컬럼을 찾을 수 없다는 에러. 원인은 2011-05 원본 파일의 해당 컬럼명이 `airport_fee`(소문자)였는데, `select_columns` 매칭을 Python `in` 연산자(대소문자 구분)로 했기 때문에 `Airport_fee`(대문자, 코드에 하드코딩된 표기)와 매칭이 안 되어 그 컬럼이 조용히 drop됐고, 이후 `_transform_yellow`가 `col("Airport_fee")`를 참조하는 지점에서 실패. Spark SQL 자체는 기본적으로 컬럼명을 대소문자 구분 없이 resolve하는데, 내가 추가한 Python 레벨 필터링만 그 규칙을 안 따른 게 원인. `df.columns`와 대소문자 무시 매칭을 하도록 수정(`5ac8b6f`)해서 해결 — 로컬 재현 검증 후 재배포.

### 결과

| 지표 | EXP-02 (기준, pruning 없음) | EXP-04 (pruning 적용) | 비고 |
|---|---:|---:|---|
| 컬럼 pruning 결과 | 해당 없음 | **19/19 유지, 0개 drop** | 이 taxi_type/월엔 애초에 불필요한 컬럼이 없었음 |
| 전체 duration | 1,062.7초 | 1,693.0초 (+59.3%) | **아래 "해석" 참고 — 실제 코드 효과로 보지 않음** |
| 최대 병목 stage | 595.9초 | 838.7초 | |
| 총 memory spill | 23,936 MB | 24,320 MB | 거의 동일 (오차범위) |
| 총 disk spill | 6,366 MB | 6,427 MB | 거의 동일 |
| executor별 GC time | 8.5~9.5초 | 8.5~10.0초 | 거의 동일 |
| 실패/재시도 task | 0 | 0 | |
| raw 파티션 크기 | (EXP-02 기록 없음) | 241.5 MiB | 정상 범위 (기존 실행들과 비슷한 수준) |
| cleaned 파티션 크기 | (EXP-02 기록 없음) | 286.9 MiB | 정상 범위 |

### 해석 — duration 차이를 pruning 효과로 보지 않는 이유

1. **메커니즘적으로 pruning이 아무 일도 하지 않았다**: `keeping 19/19 columns, dropping []` — DataFrame이 EXP-02와 완전히 동일한 스키마로 처리됐다. 코드가 다른데 결과 DataFrame이 같다면, duration 차이의 원인은 코드가 아니라 다른 곳에 있다고 봐야 한다.
2. **spill/GC/실패 task 등 메모리·안정성 지표는 EXP-02와 오차범위 내로 거의 동일**하다 — 유일하게 크게 벌어진 지표가 duration(그리고 그와 연동된 stage runtime)뿐이라는 것은, 계산량이 아니라 **처리 속도 자체의 변동**(예: 클러스터 콜드스타트, t2 버스터블 인스턴스의 CPU 크레딧/호스트 변동성)을 가리킨다.
3. 계획서 3.2절이 "각 조건 3회 이상 반복 측정, 중앙값 사용"을 요구하는 것도 정확히 이런 이유다. 이번 실험은 EXP-02와 마찬가지로 **단일 실행**이라, 두 실행 사이의 차이를 통계적으로 유의미하다고 주장할 근거가 없다.

### 정합성 검증

- Mart Count/Amount validation: 4개 Mart 전부 `PASSED`
- raw/cleaned 파티션 크기 S3 직접 대조: 정상 범위, 이상치 없음
- 검증 결과: **PASS**

### 결론

- 결과: **채택 보류(inconclusive)** — 이 데이터 규모/타입에서는 pruning할 컬럼이 원천적으로 없어(0/19 drop) 실질적으로 검증 불가능한 조건이었다. "효과가 있다/없다" 어느 쪽도 이 실험만으로는 결론 낼 수 없다.
- 판단 근거: 계획서가 이미 예견한 케이스("row 기반 포맷이나 실제로 모든 컬럼을 사용하는 경우 효과가 작을 수 있다")에 정확히 해당. `_transform_yellow`가 원본 19개 컬럼을 전부 참조하므로, 이 taxi_type에 한해서는 raw→cleaned 구간에서 pruning으로 얻을 이득이 구조적으로 없다.
- 적용 범위: main 브랜치에는 병합하지 않음 (사용자 지시). 코드/실험 기록 모두 `exp01-column-pruning` 브랜치에만 존재.
- 부수 성과: 대소문자 구분 없는 컬럼 매칭 버그를 실제 데이터로 잡아냄 — 향후 다른 실험에서 컬럼명을 문자열로 다룰 때 참고할 만한 사례로 `docs/known_issues_and_fixes.md`에 별도 기록할 가치가 있음(아직 미기록).
- 다음 실험: 컬럼이 실제로 남아도는 taxi_type(예: fhv — 원본 7개 컬럼 중 일부만 씀)이나 다른 레이어(Fact→Mart 구간, Mart가 fact_taxi_trip의 30개 컬럼 중 일부만 쓰는지 확인)에서 재시도하면 pruning 효과를 실제로 관찰할 여지가 있음. 또는 이번 결과의 duration 차이가 진짜 노이즈인지 확인하려면 동일 조건으로 반복 측정(3회+) 필요.

