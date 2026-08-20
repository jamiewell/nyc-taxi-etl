# Spark 튜닝 실험 결과

`docs/spark_tuning_plan.md`의 8.1 템플릿 양식에 따라 실제 측정 결과를 기록한다. 계획 문서는 방법론만 담고, 실측 결과는 이 문서에 누적한다.

## 요약표

| ID | 실험 | 데이터 | 핵심 변경 | 전체 duration | 정합성 | 결정 |
|---|---|---|---|---:|---|---|
| EXP-00 | Baseline S | yellow 2011-05 (1개월) | 없음 (튜닝 요소 미적용) | 1,158.0초 | PASS | 기준값 확정 |
| EXP-01 | AQE off + Worker t2.medium | yellow 2011-05 (1개월, EXP-00과 동일) | `spark.sql.adaptive.enabled=false` + executor-memory 768MB→2560MB | 2,742.9초 (**+136.9%**) | PASS | **AQE off 기각**, 메모리 증설은 유지 |
| EXP-02 | AQE on(복원) + Worker t2.medium 유지 | yellow 2011-05 (1개월, EXP-00/01과 동일) | AQE 기본값 복원, executor-memory 2560MB 유지 (memory 단독 효과 분리) | **1,062.7초 (EXP-00 대비 -8.2%)** | PASS | **새 기준선(Baseline S v2)으로 채택** |

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

