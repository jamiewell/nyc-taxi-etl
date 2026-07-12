# Spark UI 성능 분석 가이드

## 1. Jobs 탭 - 전체 파이프라인 개요

### 확인 항목
- **Total Uptime**: 전체 실행 시간 (현재: 9s)
- **Completed Jobs**: 성공한 job 수 (현재: 9개)
- **Failed Jobs**: 실패한 job 확인
- **Event Timeline**: Executor 추가/제거 시점

### 분석 포인트
```
✅ 정상: 모든 job이 "Succeeded" 상태
❌ 문제: "Failed" job이 있으면 에러 로그 확인
⚠️  주의: Job 간 대기 시간이 길면 병렬화 검토
```

### 현재 상태 분석
- 9개 job이 모두 성공 ✅
- Event Timeline에서 Executor 0, 1이 추가됨 (2개 executor 사용)
- parquet → count → showString 패턴 반복 (Read → Process → Output)

---

## 2. Stages 탭 - 단계별 성능 분석

### 핵심 지표

| 지표 | 의미 | 목표 |
|------|------|------|
| **Duration** | Stage 실행 시간 | 균등 분산 |
| **Input** | 읽은 데이터 크기 | - |
| **Output** | 쓴 데이터 크기 | - |
| **Shuffle Read** | Shuffle로 읽은 데이터 | 최소화 |
| **Shuffle Write** | Shuffle로 쓴 데이터 | 최소화 |

### 현재 상태 분석

**Completed Stages:**
```
Stage 11: parquet read (52.3 MiB input → 56.1 MiB output) - 4s ⚠️
  → 가장 오래 걸림, 데이터 읽기 단계

Stage 10: showString (Shuffle Write 2.8 KiB) - 76ms ✅
Stage 8:  showString (27.1 MiB input) - 0.4s ✅
Stage 7:  showString (9.1 MiB input) - 0.1s ✅

Stage 6:  count (Shuffle Read 118 B) - 71ms ✅
Stage 4:  count (14.0 MiB input, Shuffle Write 118 B) - 0.2s ✅
Stage 3:  count (Shuffle Read 116 B) - 69ms ✅
Stage 1:  count (21.9 KiB input, Shuffle Write 116 B) - 0.6s ✅

Stage 0:  parquet read - 0.3s ✅
```

**Skipped Stages: 3개**
```
Stage 9, 5, 2: Spark가 중복 계산 감지하여 스킵 (최적화됨) ✅
```

### 문제 발견 방법

#### A. Shuffle이 많은 Stage 찾기
```
Shuffle Read/Write > 100 MB → 데이터 이동 비용 큼
→ 해결: repartition 수 조정, broadcast join 고려
```

#### B. 오래 걸리는 Stage 찾기
```
Duration > 전체 평균 2배 → 병목 구간
→ 해결: 파티션 수 증가, 데이터 스큐 확인
```

#### C. Task 불균형 확인
```
Stage 클릭 → Task Metrics 확인
- Max Duration ÷ Median Duration > 3배 → 데이터 스큐
→ 해결: salting, 키 분산
```

---

## 3. SQL/DataFrame 탭 - 쿼리 실행 계획

### 확인 항목
1. **Physical Plan**: 실제 실행된 쿼리 계획
2. **Whole Stage Codegen**: 최적화 여부
3. **Exchange (Shuffle)**: Shuffle 발생 지점
4. **Scan**: 데이터 읽기 방식 (파티션 pruning 확인)

### 분석 예시
```sql
-- 좋은 예: Partition pruning 적용
Scan parquet [year=2026, month=1]  ✅
→ 필요한 파티션만 읽음

-- 나쁜 예: Full scan
Scan parquet [year, month]  ❌
→ 모든 파티션 읽음 (필터 조건이 partition key에 없음)
```

### Shuffle 최소화 팁
```python
# Before: 큰 Shuffle
df1.join(df2, "key")  # Exchange 발생

# After: Broadcast join
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "key")  # Shuffle 없음 (df2가 작을 때)
```

---

## 4. Executors 탭 - 리소스 사용량

### 확인 항목
- **Active Tasks**: 현재 실행 중인 task 수
- **Memory Used / Total**: 메모리 사용률
- **Disk Used**: Spill 발생 여부
- **GC Time**: Garbage Collection 시간

### 경고 신호
```
⚠️  Memory Used > 90% → OOM 위험, executor 메모리 증가 필요
⚠️  Disk Used > 0 → Spill 발생, 메모리 부족
⚠️  GC Time > 10% of Total Time → 메모리 압박
⚠️  Failed Tasks > 0 → OOM, Network timeout 등 확인
```

### 해결 방법
```bash
# Executor 메모리 증가
spark-submit --executor-memory 4g  # 기본 1g → 4g

# Executor 수 증가 (병렬성)
spark-submit --num-executors 4

# Executor 당 코어 조정
spark-submit --executor-cores 2
```

---

## 5. Storage 탭 - 캐싱 효과 확인

### 확인 항목
- **Cached Partitions**: 캐시된 파티션 수
- **Fraction Cached**: 캐시 비율
- **Size in Memory**: 메모리에 저장된 크기
- **Size on Disk**: 디스크에 저장된 크기

### 캐싱 전략
```python
# 반복 사용되는 DataFrame은 캐싱
df_fact = spark.read.parquet("fact_table").cache()

# 사용 후 캐시 해제
df_fact.unpersist()
```

---

## 6. Environment 탭 - Spark 설정 확인

### 주요 설정 확인
```
spark.executor.memory = 1g  → Executor 메모리
spark.executor.cores = 1    → Executor 코어
spark.sql.shuffle.partitions = 200  → Shuffle 파티션 수
spark.sql.adaptive.enabled = true   → AQE 활성화
```

### 최적화 설정 예시
```bash
spark-submit \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.autoBroadcastJoinThreshold=10485760 \
  --conf spark.sql.shuffle.partitions=100 \
  jobs/main.py
```

---

## 7. 성능 분석 체크리스트

### 단계별 분석

#### ✅ Step 1: Jobs 탭
- [ ] 모든 job이 성공했는가?
- [ ] Total Uptime이 예상 범위인가?
- [ ] Failed job이 있는가? → 로그 확인

#### ✅ Step 2: Stages 탭
- [ ] 오래 걸리는 Stage는? (Duration 확인)
- [ ] Shuffle이 많은 Stage는? (Shuffle Read/Write 확인)
- [ ] Task 불균형은? (개별 Stage 클릭하여 Task Metrics 확인)
- [ ] Skipped Stage는 왜 스킵됐나? (중복 계산 최적화 확인)

#### ✅ Step 3: SQL/DataFrame 탭
- [ ] Physical Plan에서 Exchange(Shuffle) 위치 확인
- [ ] Partition pruning이 적용됐는가?
- [ ] Broadcast join을 사용할 수 있는가?

#### ✅ Step 4: Executors 탭
- [ ] Memory 사용률이 90% 이하인가?
- [ ] Disk spill이 발생했는가?
- [ ] GC Time이 10% 이하인가?
- [ ] Failed tasks가 있는가?

#### ✅ Step 5: Storage 탭
- [ ] 반복 사용되는 DataFrame이 캐시됐는가?
- [ ] 캐시 비율이 100%인가? (일부만 캐시되면 메모리 부족)

---

## 8. 현재 Job 성능 개선 포인트

### 발견된 이슈

**1. Stage 11 (parquet read) - 4s**
```
전체 실행 시간의 44% (4s / 9s) 차지
→ 원인: 데이터 읽기가 느림
→ 해결:
  - 파티셔닝 확인 (year/month로 파티셔닝됐는가?)
  - 컬럼 pruning (필요한 컬럼만 select)
  - Predicate pushdown (where 조건을 읽기 단계에 적용)
```

**2. Shuffle Write가 작음 (KB 단위)**
```
✅ 좋은 신호: 데이터 이동이 적음
→ 현재는 최적화됨
```

**3. Skipped Stages 3개**
```
✅ Spark가 중복 계산을 자동으로 스킵
→ 추가 최적화 불필요
```

### 권장 개선 작업

#### A. 파티션 수 최적화
```python
# 현재 기본값: 200 파티션
# 데이터 크기가 작으면 파티션 수 감소
spark.conf.set("spark.sql.shuffle.partitions", "50")
```

#### B. 컬럼 pruning
```python
# Before: 모든 컬럼 읽기
df = spark.read.parquet("input.parquet")

# After: 필요한 컬럼만 읽기
df = spark.read.parquet("input.parquet").select("col1", "col2", "col3")
```

#### C. Predicate pushdown
```python
# Before: 읽은 후 필터링
df = spark.read.parquet("input.parquet")
df_filtered = df.filter(col("year") == 2026)

# After: 읽기 단계에서 필터링
df_filtered = spark.read.parquet("input.parquet") \
    .filter(col("year") == 2026)  # Parquet footer에서 필터링
```

---

## 9. 성능 측정 지표

### 처리량 (Throughput)
```
처리량 = 처리한 데이터 크기 / 실행 시간
현재: 52.3 MiB / 9s = 5.8 MiB/s

목표: > 50 MiB/s (클러스터 규모에 따라 다름)
```

### 병렬도 (Parallelism)
```
병렬도 = 동시 실행 가능한 Task 수
현재: Executor 2개 × 1 core = 2 tasks

권장: Executor 수 × Cores = 데이터 파티션 수 / 2
```

### 리소스 효율
```
리소스 효율 = 실제 작업 시간 / (Executor 수 × Uptime)
목표: > 70%

낮으면 → Executor가 idle 상태 → 파티션 수 증가 필요
```

---

## 10. 추가 분석 도구

### A. Spark History Server 로그
```bash
# 컨테이너 내부에서
docker exec nyc-taxi-spark ls /tmp/spark-events/
```

### B. Spark Event Log 분석
```bash
# Dr. Elephant (LinkedIn)
# Sparklens (Qubole)
# 등의 도구로 심화 분석 가능
```

### C. 프로파일링
```python
# PySpark 프로파일링
spark.sparkContext.setLogLevel("INFO")
```

---

## 참고: 성능 튜닝 우선순위

1. **데이터 스큐 해결** (가장 효과 큼)
2. **Shuffle 최소화** (Broadcast join, partition key 최적화)
3. **메모리 튜닝** (Executor 메모리, 캐싱)
4. **파티션 수 조정** (너무 많으면 오버헤드, 너무 적으면 병렬성 저하)
5. **압축 포맷** (Parquet + snappy 권장)
6. **코드 최적화** (UDF 제거, Spark SQL 활용)

---

## 요약

**현재 상태**: ✅ 정상 작동 중
- 9개 job 모두 성공
- Shuffle 최소화됨
- Skipped stage 3개 (최적화됨)

**개선 포인트**:
- Stage 11 (parquet read) 최적화 → 컬럼 pruning, predicate pushdown
- Executor 수 증가 고려 (2 → 4)
- 파티션 수 조정 (200 → 50 or 100)

**다음 단계**:
1. SQL/DataFrame 탭에서 Physical Plan 확인
2. 개별 Stage 클릭하여 Task Metrics 분석
3. 설정 변경 후 재실행하여 비교
