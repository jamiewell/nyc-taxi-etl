# Spark 튜닝 계획

## 1. 문서 목적

이 문서는 NYC Taxi 데이터를 PySpark로 처리하는 과정에서 성능 병목을 재현하고, Spark UI와 실행 결과를 근거로 개선 효과를 검증하기 위한 실험 계획서다.

대상 파이프라인은 다음과 같다.

```text
Raw → Cleaned → Fact → Mart
```

튜닝은 설정값을 무조건 크게 하거나 실행 시간을 한 번 줄이는 작업이 아니다. 동일한 데이터와 실행 환경에서 기준 실행(Baseline)을 측정하고, 한 번에 하나의 주요 변수를 변경한 뒤 처리 시간, 셔플, spill, 파일 수, 비용을 비교한다.

## 2. 튜닝 목표

- 필요한 데이터만 읽어 Parquet scan과 S3 I/O를 줄인다.
- `groupBy`, `join`, `repartition`에서 발생하는 불필요한 셔플을 줄인다.
- task 간 처리량 편차와 데이터 skew를 찾아 stage의 긴 꼬리(long tail)를 줄인다.
- executor의 메모리 부족, disk spill, 과도한 GC를 줄인다.
- Fact를 여러 Mart에서 사용할 때 반복 scan과 반복 계산의 비용을 비교한다.
- 출력 파일의 개수와 크기를 조절하여 small file 문제를 완화한다.
- AWS EC2 수동 Spark 클러스터에서 모든 기준 실행과 튜닝 실험을 수행한다.
- 실행 시간뿐 아니라 안정성, 인프라 비용, 코드 복잡도까지 함께 평가한다.

### 우선 관찰할 Job

1. `raw_to_cleaned.py`: Parquet scan, 필터, 파생 컬럼 생성
2. `cleaned_to_fact.py`: 컬럼 표준화와 지표 계산
3. `build_mart_month_hour_zone_trip_metrics.py`: 지역 기준 집계와 dimension join
4. `build_mart_month_hour_vendor_trip_metrics.py`: vendor 기준 집계

## 3. 실험 원칙

### 3.1 통제 조건

각 비교 실험에서는 다음 조건을 고정한다.

- 입력 경로, 데이터 기간, 입력 파일 집합
- 코드 버전과 Python/Spark 버전
- 클러스터 구성과 executor 수
- executor core와 memory
- 실행 모드와 배포 모드
- 출력 포맷과 compression codec
- 동시 실행 중인 다른 작업
- cold run 또는 warm run 여부

설정 하나의 효과를 확인할 때는 다른 주요 설정을 동시에 변경하지 않는다. 여러 설정을 함께 적용한 최종 실험은 개별 효과를 확인한 후 별도로 수행한다.

### 3.2 반복 측정

- 각 조건은 가능하면 3회 이상 실행한다.
- 첫 실행이 캐시, 이미지 다운로드, JVM 준비 등의 영향을 받으면 별도로 표시한다.
- 대표값은 중앙값을 사용하고 최솟값과 최댓값도 기록한다.
- 데이터 정합성 검증이 실패한 실행은 성능 비교에서 제외하고 원인을 기록한다.

### 3.3 정합성 검증

튜닝 전후에 다음 값이 같아야 한다.

- 입력 및 출력 row count
- 주요 집계의 합계와 평균
- null 및 중복 건수
- 파티션별 row count
- schema와 주요 컬럼의 데이터 타입

부동소수점 집계는 사전에 정한 허용 오차 안에서 비교한다.

## 4. Baseline 측정

### 4.1 데이터 범위

| 구분 | 데이터 범위 | 목적 |
|---|---|---|
| Baseline S | 1개월 | EC2 클러스터 동작 확인과 Spark UI 학습 |
| Baseline M | 12개월 | EC2 클러스터의 셔플, 메모리, S3 I/O 병목 관찰 |
| Baseline L | 다년치 | EC2 환경의 확장성, 안정성, 비용 측정 |

실제 사용한 연월, 원본 파일 수와 전체 크기는 실행 기록에 명시한다.

### 4.2 기준 설정

첫 Baseline은 명시적인 튜닝을 최소화한다.

- `spark.sql.shuffle.partitions`: Spark 기본값 또는 프로젝트 기본값
- AQE: 현재 환경의 기본값을 기록하고 고정
- `repartition`/`coalesce`: 명시적으로 추가하지 않음
- broadcast hint: 사용하지 않음
- `cache`/`persist`: 사용하지 않음
- 출력 파일 수를 위한 강제 repartition: 사용하지 않음

Spark 버전에 따라 기본 설정이 다를 수 있으므로 추측하지 않고 Spark UI의 **Environment** 탭 또는 `spark.sparkContext.getConf()` 결과로 실제 값을 남긴다.

### 4.3 Baseline 실행 순서

1. 입력 데이터의 기간, 크기, 파일 수와 파티션 구조를 기록한다.
2. Spark 및 클러스터 설정을 저장한다.
3. 각 Job을 독립적으로 실행한다.
4. Spark UI의 Job, Stage, SQL 정보를 캡처하거나 event log를 보관한다.
5. 출력 row count, 파일 수, 전체 크기와 파일 크기 분포를 기록한다.
6. 동일 조건으로 반복 실행하고 중앙값을 계산한다.

## 5. Spark UI 관찰 지표

### 5.1 Jobs 탭

- 전체 Job 및 stage 수
- Job별 duration
- 실패하거나 재시도한 stage
- 실행을 지연시키는 가장 오래 걸린 stage

### 5.2 Stages 탭

- task 수와 task duration 분포
- Input Size / Records
- Output Size / Records
- Shuffle Read / Records
- Shuffle Write / Records
- Spill (Memory), Spill (Disk)
- Peak Execution Memory
- GC Time
- scheduler delay와 task deserialization/serialization time
- locality level과 실패/재시도 task

평균값만 보지 않고 max, median, 상위 task를 함께 본다. 일부 task만 매우 오래 걸리거나 입력량이 크면 skew 또는 파티션 불균형을 의심한다.

### 5.3 SQL 탭

- 물리 실행 계획의 `FileScan`, `Filter`, `Exchange`, `Sort`, `Aggregate`, `Join`
- `PartitionFilters`, `PushedFilters`, `ReadSchema`
- join 방식: BroadcastHashJoin, SortMergeJoin 등
- AQE 적용 후 계획과 partition coalescing 여부
- 동일 source의 반복 scan 여부

### 5.4 Executors 탭

- executor별 active/failed/completed task 수
- executor별 input, shuffle read/write 편차
- storage memory 사용량
- GC time과 task time의 비율
- executor lost, OOM, disk spill 여부

### 5.5 Storage와 Environment 탭

- persist된 DataFrame의 storage level, 크기, partition 수
- memory와 disk에 저장된 비율
- 실험에 실제 적용된 Spark 설정

### 5.6 핵심 비교 지표

| 영역 | 지표 | 해석 |
|---|---|---|
| 실행 | 전체 및 주요 stage duration | 최종 성능과 병목 stage 확인 |
| 읽기 | input bytes, files read, records | pruning 효과 확인 |
| 셔플 | shuffle read/write bytes | 집계와 join의 네트워크·디스크 비용 확인 |
| 메모리 | memory/disk spill, peak memory | partition 크기와 메모리 압박 확인 |
| JVM | GC time / executor run time | 객체 및 메모리 압박 확인 |
| 균형 | task duration/input size의 max와 median | skew와 partition 불균형 확인 |
| 쓰기 | output files, 평균·최소·최대 파일 크기 | small file 및 후속 scan 효율 확인 |
| 비용 | 클러스터 실행 시간과 추정 비용 | 운영 환경의 경제성 확인 |

## 6. 튜닝 실험

각 실험은 `Baseline → 변경 → 반복 측정 → 정합성 확인 → 결과 판정` 순서로 진행한다.

### EXP-01. Column Pruning

**가설**: ETL에 필요한 컬럼만 `select`하면 Parquet에서 읽는 데이터와 메모리 사용량이 감소한다.

**방법**

1. 전체 컬럼을 읽는 Baseline을 실행한다.
2. 각 레이어에 필요한 컬럼 목록을 명시하여 실행한다.
3. Python UDF 또는 불필요한 중간 컬럼이 pruning을 방해하는지 실행 계획에서 확인한다.

**관찰 항목**

- SQL 계획의 `ReadSchema`
- scan input bytes와 records
- 주요 stage duration과 peak memory

**판정**: 정합성을 유지하면서 scan bytes 또는 duration이 유의미하게 감소하면 적용한다. row 기반 포맷이나 실제로 모든 컬럼을 사용하는 경우 효과가 작을 수 있다.

### EXP-02. Partition Pruning

**가설**: `year`, `month`, 필요 시 `day` 파티션 컬럼으로 먼저 필터링하면 대상 디렉터리만 읽는다.

**방법**

1. 전체 경로를 읽은 뒤 timestamp 함수로 기간을 필터링하는 경우를 측정한다.
2. 파티션 경로 또는 파티션 컬럼 조건으로 동일 기간을 읽는다.
3. 필터 컬럼의 cast나 함수 적용이 pruning을 막는지 확인한다.

**관찰 항목**

- `PartitionFilters`와 선택된 파일 수
- input bytes, scan duration
- S3의 경우 GET/list 요청과 읽은 데이터 양

**판정**: 읽는 파일과 bytes가 목표 기간에 맞게 감소해야 한다. 파티션 컬럼 타입과 필터 상수 타입도 일치시킨다.

### EXP-03. `spark.sql.shuffle.partitions`

**가설**: 데이터 크기와 cluster parallelism에 맞는 shuffle partition 수는 너무 큰 task와 지나치게 작은 task를 모두 줄인다.

**방법**

1. Baseline 값으로 Mart 집계를 실행한다.
2. 예: `50`, `100`, `200`, `400`처럼 데이터 규모에 맞는 후보를 비교한다.
3. AQE 실험과 분리하기 위해 먼저 AQE 조건을 고정한다.

**관찰 항목**

- shuffle stage의 task 수
- task별 shuffle read 크기와 duration 분포
- spill, GC time, scheduler overhead
- 전체 duration

**판정**: spill 없이 core를 충분히 활용하고 task 크기 편차가 허용 범위인 최소 실행 시간의 값을 선택한다. 특정 숫자를 모든 데이터 규모에 공통 적용하지 않는다.

### EXP-04. `repartition`과 `coalesce`

**가설**: 명시적 partition 조절은 후속 연산의 병렬성과 출력 파일 수를 개선할 수 있지만, 불필요한 `repartition`은 추가 셔플을 만든다.

**방법**

- 집계 또는 join 전 key 기준 `repartition` 유무 비교
- 출력 직전 `repartition(n)`과 `coalesce(n)` 비교
- partition을 늘리는 경우와 줄이는 경우를 구분
- `repartition(1)`은 소규모 검증 외에는 사용하지 않음

**관찰 항목**

- 실행 계획의 추가 `Exchange`
- shuffle read/write와 stage 수
- task 균형, 전체 duration
- 출력 파일 수와 파일 크기 분포

**판정**: 후속 연산 또는 파일 품질 개선 효과가 추가 셔플 비용보다 클 때만 적용한다. 큰 폭으로 partition을 줄일 때 `coalesce`가 일부 task에 데이터를 몰아주는지도 확인한다.

### EXP-05. Broadcast Join

**가설**: 작은 `dim_vendor` 또는 `dim_taxi_zone`을 broadcast하면 Sort-Merge Join의 양쪽 셔플을 피할 수 있다.

**방법**

1. hint 없이 실행하여 Spark가 선택한 join을 기록한다.
2. dimension의 실제 row 수와 직렬화 크기를 측정한다.
3. 자동 broadcast threshold 조정 또는 `broadcast()` hint를 각각 시험한다.

**관찰 항목**

- 물리 계획의 join 방식
- shuffle read/write와 join stage duration
- executor memory, GC, broadcast time
- timeout 및 OOM 여부

**판정**: dimension이 모든 executor에 안전하게 복제될 정도로 작고, memory 문제 없이 셔플과 실행 시간이 감소하면 적용한다. 큰 테이블에 hint를 강제하지 않는다.

### EXP-06. Persist / Cache

**가설**: 동일한 Fact DataFrame에서 여러 Mart를 연속 생성할 때 persist하면 반복 scan과 계산을 줄일 수 있다.

**방법**

1. Mart별로 Fact를 다시 읽는 경우를 측정한다.
2. `cache()` 또는 후보 storage level로 Fact를 persist한 뒤 materialize한다.
3. 첫 Mart, 후속 Mart, 전체 pipeline duration을 각각 측정한다.
4. 마지막 consumer가 끝나면 `unpersist()`한다.

**관찰 항목**

- SQL 계획의 반복 `FileScan`
- Storage 탭의 cached size와 partition 수
- cache materialization 시간
- memory/disk 저장 비율, eviction, spill과 GC
- 전체 Mart 묶음의 duration

**판정**: cache 생성 비용을 포함한 전체 실행 시간이 줄고 다른 연산의 메모리를 압박하지 않을 때만 적용한다. 한 번만 사용하는 DataFrame은 기본적으로 persist하지 않는다.

### EXP-07. Adaptive Query Execution(AQE)

**가설**: AQE는 런타임 통계를 사용하여 작은 shuffle partition을 합치고 join 전략과 skew partition 처리를 조정할 수 있다.

**방법**

1. 같은 코드와 shuffle partition 수로 AQE off/on을 비교한다.
2. partition coalescing, dynamic join conversion, skew join 기능을 단계적으로 확인한다.
3. Spark 버전과 관련 AQE 설정값을 함께 기록한다.

**관찰 항목**

- initial plan과 final adaptive plan
- `AQEShuffleRead`, coalesced partition 수
- join 전략 변경 여부
- skew partition split 여부
- task 수, duration, spill

**판정**: 정합성을 유지하고 전반적인 실행 시간 또는 안정성을 개선하면 적용한다. AQE가 수동 설계와 모든 skew 문제를 자동으로 해결한다고 가정하지 않는다.

### EXP-08. Data Skew 분석과 완화

**가설**: 특정 pickup zone, 시간, vendor 또는 null/unknown key에 레코드가 집중되면 일부 task가 stage 전체를 지연시킨다.

**분석 방법**

1. 집계 및 join key별 row count와 비율을 계산한다.
2. 파티션별 row count를 조사한다.
3. Spark UI에서 task input/shuffle read와 duration의 max, median, p95를 비교한다.
4. null 또는 sentinel key가 한 partition에 집중되는지 확인한다.

**완화 후보**

- AQE skew join 활성화 및 threshold 검증
- skew key와 일반 key를 분리하여 처리 후 union
- 매우 큰 key에 제한적인 salting 적용 후 재집계
- 더 고른 복합 key 또는 단계적 집계 사용
- 잘못된 null/unknown 레코드의 별도 처리

**관찰 항목**

- 가장 큰 key의 비율
- task input과 duration의 max/median 비율
- straggler 수와 stage duration
- 추가 셔플 및 코드 복잡도

**판정**: long tail이 줄고 추가 처리 비용보다 효과가 클 때 적용한다. salting은 정합성 및 코드 복잡도 비용이 크므로 skew가 확인된 경우에만 사용한다.

### EXP-09. Day-level Partial Aggregation

**가설**: 월 단위 대용량 원본을 바로 집계하는 대신 일 단위로 부분 집계한 뒤 월 단위로 재집계하면 shuffle 입력과 재처리 범위를 줄일 수 있다.

**처리 구조**

```text
Fact trips
  → day + hour + zone 기준 부분 집계
  → month + hour + zone 기준 최종 집계
```

**방법**

1. Fact에서 월 Mart를 직접 생성하는 Baseline을 측정한다.
2. 일 단위 partial aggregate를 저장하고 월 단위로 merge한다.
3. 합계형 지표는 합계를 다시 합산한다.
4. 평균은 평균들의 평균을 사용하지 않고 `sum`과 `count`를 저장해 가중 평균을 계산한다.
5. distinct, percentile처럼 단순 결합할 수 없는 지표는 별도 전략을 정의한다.

**관찰 항목**

- 각 단계 input/output row와 shuffle bytes
- 전체 최초 생성 시간
- 특정 일자 재처리 및 월 Mart 재생성 시간
- partial aggregate의 파일 수와 저장 비용
- 최종 결과 정합성

**판정**: 최초 전체 실행과 증분 재처리의 이점을 함께 평가한다. 작은 데이터에서는 중간 write/read 비용 때문에 직접 집계가 더 빠를 수 있다.

### EXP-10. S3 / Parquet Write 최적화

**가설**: 적절한 파티션 키, 파일 크기와 compression은 S3 요청 수, write 시간, 후속 scan 비용을 줄인다.

**방법**

- 출력 파티션 후보 비교: `year/month`, 필요 시 `day`
- cardinality가 높은 컬럼을 디렉터리 파티션으로 사용하지 않음
- 출력 직전 partition 수 조절 유무 비교
- Parquet compression codec과 row group 관련 설정 기록
- 동적 파티션 overwrite와 전체 overwrite의 범위 확인
- EC2 클러스터에서 사용 중인 S3 connector/committer와 speculative execution의 상호작용 확인

**관찰 항목**

- write stage duration, shuffle write, commit 시간
- 출력 파일 수와 총 크기
- 파일 크기의 min/median/max와 지나치게 작은 파일 비율
- 후속 query의 files read, input bytes와 scan duration
- S3 요청 수와 실패/재시도

**판정**: 목표 파일 크기는 데이터와 후속 workload를 기준으로 실험해 정한다. write만 빠르고 후속 read가 느려지는 구성은 채택하지 않는다. S3의 rename과 다수 small file 비용을 EC2 클러스터에서 실제로 측정하여 판단한다.

## 7. 실험 우선순위와 의존 관계

권장 순서는 다음과 같다.

```text
Baseline
  → Column/Partition Pruning
  → Shuffle Partitions
  → Repartition/Coalesce
  → Broadcast Join
  → Persist/Cache
  → AQE
  → Skew 분석 및 완화
  → Day-level Partial Aggregation
  → S3/Parquet Write 최적화
  → 채택 설정 통합 검증
```

먼저 읽는 양을 줄이고, 이후 셔플과 join을 최적화한다. persist, AQE, skew 완화는 실행 구조와 데이터 분포를 확인한 뒤 적용한다. 모든 S3 읽기·쓰기 결과는 동일한 EC2 클러스터 조건에서 비교한다.

## 8. 실험 결과 기록 템플릿

### 8.1 개별 실험 기록

```markdown
## EXP-XX: 실험명

### 기본 정보

- 실행 ID / Spark Application ID:
- 실행 일시:
- 코드 commit:
- 실행 환경: AWS EC2 수동 Spark 클러스터
- Spark 버전:
- 데이터 범위:
- 입력 크기 / 파일 수 / row 수:
- cluster 구성:
- executor 수 / cores / memory:

### 가설

-

### 변경 사항

- Baseline 설정:
- 변경 설정 또는 코드:
- 고정한 조건:

### 결과

| 지표 | Baseline | 변경 후 | 증감률 | 비고 |
|---|---:|---:|---:|---|
| 전체 duration | | | | |
| 주요 stage duration | | | | |
| input size | | | | |
| shuffle read | | | | |
| shuffle write | | | | |
| memory spill | | | | |
| disk spill | | | | |
| GC time | | | | |
| task duration median / max | | | | |
| output files | | | | |
| 평균 파일 크기 | | | | |
| 추정 비용 | | | | |

### 정합성 검증

- row count:
- 주요 집계 값:
- schema:
- 파티션별 건수:
- 검증 결과: PASS / FAIL

### Spark UI 관찰

- 병목 stage:
- physical plan 변화:
- task 분포와 skew:
- executor / memory 특이 사항:
- event log 또는 캡처 위치:

### 결론

- 결과: 채택 / 보류 / 기각
- 판단 근거:
- 적용 범위:
- 부작용 또는 주의점:
- 다음 실험:
```

증감률은 방향 혼동을 막기 위해 다음과 같이 계산한다.

```text
개선율(%) = (Baseline - 변경 후) / Baseline × 100
```

duration, shuffle, spill, 비용처럼 작을수록 좋은 지표에 사용한다. throughput처럼 클수록 좋은 지표는 별도 공식을 명시한다.

### 8.2 실험 요약표

| ID | 실험 | 환경 | 데이터 | 핵심 변경 | 시간 개선율 | Shuffle 개선율 | 비용 영향 | 정합성 | 결정 |
|---|---|---|---|---|---:|---:|---|---|---|
| EXP-01 | Column pruning | | | | | | | | |
| EXP-02 | Partition pruning | | | | | | | | |
| EXP-03 | Shuffle partitions | | | | | | | | |
| EXP-04 | Repartition/coalesce | | | | | | | | |
| EXP-05 | Broadcast join | | | | | | | | |
| EXP-06 | Persist/cache | | | | | | | | |
| EXP-07 | AQE | | | | | | | | |
| EXP-08 | Skew 완화 | | | | | | | | |
| EXP-09 | 일 단위 부분 집계 | | | | | | | | |
| EXP-10 | S3/Parquet write | | | | | | | | |

## 9. Phase별 적용 계획

### Phase 1. 기본 프로젝트 구성과 ETL 구현

**목표**: 정합성이 보장되는 단순하고 측정 가능한 파이프라인을 만든다.

- Raw, Cleaned, Fact, Mart schema와 grain 정의
- `year/month` 기준 데이터 레이어와 파티션 설계
- Job별 입력/출력 row count 및 품질 검증 구현
- 필요한 컬럼을 명시하고 filter를 가능한 이른 시점에 적용
- 튜닝 설정을 코드에 하드코딩하지 않고 실행 인자로 주입할 구조 마련
- application name, Job 설명, 로그, event log 경로 표준화

이 단계에서는 과도한 cache, salting, 수동 repartition을 적용하지 않는다. 정확한 Baseline을 만들 수 있는 구조가 완료 조건이다.

### Phase 2. EC2 수동 Spark 클러스터 구성과 UI 학습

**목표**: EC2에 수동 Spark 클러스터를 구성하고, 1개월 S3 데이터로 Job, Stage, Task, DAG와 주요 지표를 연결해 이해한다.

- EC2 master/worker 구성과 노드 간 통신 확인
- S3 입력 및 출력 경로 접근 확인
- Spark Application UI와 History Server 구성
- Baseline S 측정
- Column pruning과 partition pruning 확인
- shuffle partition 후보 비교
- `repartition`과 `coalesce`가 만드는 DAG 변화 확인
- 작은 dimension의 broadcast join 비교
- 동일 Fact에서 복수 Mart를 생성하여 persist/cache 비교
- AQE off/on 비교와 initial/final plan 확인

클러스터 구성, Spark 설정, EC2 instance type, EBS 사양과 S3 경로를 Baseline의 일부로 기록하여 이후 실험에서 동일하게 유지한다.

### Phase 3. EC2 수동 클러스터 튜닝과 S3 최적화

**목표**: 분산 환경의 네트워크, executor 불균형, S3 I/O 병목을 관찰한다.

- Baseline M 측정
- executor/core 수에 따른 shuffle partition 재산정
- executor별 task와 shuffle 편차 분석
- zone, hour, vendor, null key의 skew 분석
- AQE skew 처리와 제한적인 수동 완화 비교
- broadcast join의 executor memory 영향 확인
- S3/Parquet 파일 수, 파일 크기, 파티션 구조 실험
- 장애, executor loss, task retry와 재실행 가능성 확인

각 실험에 EC2 및 스토리지 실행 비용을 함께 기록한다.

### Phase 4. EC2 다년치 데이터와 통합 최적화

**목표**: EC2 수동 클러스터에서 다년치 데이터를 처리하며 성능, 안정성, 비용을 함께 최적화한다.

- Baseline L 측정 및 소규모·중간 규모 결과와 비교
- Spark 버전의 기본 AQE 및 join 설정 재확인
- cluster 크기 및 instance family별 비용 대비 처리량 비교
- 다년치 데이터의 partition pruning과 skew 재검증
- day-level partial aggregation 및 증분 재처리 실험
- S3 connector/committer, Parquet compression, 출력 파일 최적화
- event log와 Spark History Server를 통한 실행 이력 보관
- 채택한 개별 튜닝을 조합한 통합 회귀 실험
- 처리 시간 SLA, 실패율, 데이터 품질, 월별 예상 비용 문서화

## 10. 완료 기준

튜닝 계획은 다음 조건을 만족할 때 1차 완료로 본다.

- 모든 대상 Job의 Baseline이 같은 양식으로 기록되어 있다.
- 각 병목이 Spark UI 또는 실행 계획의 지표로 설명되어 있다.
- 채택한 변경은 3회 이상 반복 측정과 정합성 검증을 통과했다.
- 실행 시간, shuffle, spill, 파일 수와 비용의 전후 비교가 남아 있다.
- EC2의 데이터 규모 및 클러스터 구성별 결과 차이와 적용 범위가 기록되어 있다.
- 최종 설정의 근거, 적용 조건, 롤백 방법이 문서화되어 있다.

최종 목표는 가장 많은 설정을 적용하는 것이 아니라, 데이터 규모와 실행 환경에 맞는 최소한의 변경으로 재현 가능한 개선을 만드는 것이다.
