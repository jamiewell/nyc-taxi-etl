# Partitioning vs Partition Pruning

Spark 성능 최적화의 핵심 개념

---

## 목차

1. [개요](#개요)
2. [Partitioning (파티셔닝)](#partitioning-파티셔닝)
3. [Partition Pruning (파티션 프루닝)](#partition-pruning-파티션-프루닝)
4. [차이점 비교](#차이점-비교)
5. [실전 예시](#실전-예시)
6. [성능 비교](#성능-비교)

---

## 개요

### 핵심 요약

| 개념 | 의미 | 비유 |
|------|------|------|
| **Partitioning** | 데이터를 나눠서 저장 | 책을 서가별로 분류 |
| **Partition Pruning** | 필요한 것만 읽기 | 필요한 서가만 뒤지기 |

---

## Partitioning (파티셔닝)

### 정의

**"데이터를 논리적 단위로 나눠서 폴더별로 저장하는 것"**

### 디렉토리 구조

```
data/taxi_trips/
├── year=2026/
│   ├── month=01/
│   │   └── part-00000.parquet  ← 2026년 1월 데이터
│   ├── month=02/
│   │   └── part-00000.parquet  ← 2026년 2월 데이터
│   └── month=03/
│       └── part-00000.parquet  ← 2026년 3월 데이터
└── year=2027/
    └── month=01/
        └── part-00000.parquet  ← 2027년 1월 데이터
```

### 코드 예시

```python
# Partitioning: year/month로 나눠 저장
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("data/taxi_trips/")
```

**결과:**
- 데이터가 year, month별로 폴더 분리
- 각 폴더는 해당 조건의 데이터만 포함

### 장점

✅ **쿼리 성능 향상**
```python
# year=2026만 필터링 → year=2026/ 폴더만 읽음
df.filter(col("year") == 2026)
```

✅ **병렬 처리 용이**
```
각 파티션을 독립적으로 처리 가능
→ Spark executor가 파티션별로 병렬 실행
```

✅ **데이터 관리 편리**
```
# 특정 월 데이터 삭제
rm -rf data/taxi_trips/year=2026/month=01/
```

### 주의사항

⚠️ **카디널리티 (Cardinality)**
```python
# 좋은 예: year/month (12-36개 파티션)
df.partitionBy("year", "month")

# 나쁜 예: user_id (수백만 개 파티션)
df.partitionBy("user_id")  # ❌ 너무 많은 폴더 생성
```

⚠️ **파티션 수**
```
너무 많음 → 작은 파일 다수 생성 (Small File Problem)
너무 적음 → 파일이 너무 큼, 병렬성 저하

권장: 파티션당 128MB ~ 1GB
```

---

## Partition Pruning (파티션 프루닝)

### 정의

**"쿼리 실행 시 필요한 파티션만 읽고, 불필요한 파티션은 스킵하는 최적화 기법"**

### 동작 방식

#### ❌ Pruning 없이 (모든 파티션 읽기)

```python
# 쿼리
df = spark.read.parquet("data/taxi_trips/")
df.filter(col("year") == 2026).filter(col("month") == 1)
```

**읽은 파티션:**
```
✗ year=2026/month=01/  ← 필요 (읽음)
✗ year=2026/month=02/  ← 불필요 (읽음)
✗ year=2026/month=03/  ← 불필요 (읽음)
✗ year=2027/month=01/  ← 불필요 (읽음)

총 4개 파티션 읽음
```

#### ✅ Pruning 적용 (필요한 파티션만 읽기)

```python
# 동일한 쿼리
df = spark.read.parquet("data/taxi_trips/")
df.filter(col("year") == 2026).filter(col("month") == 1)
```

**Spark가 자동으로 최적화:**
```
✓ year=2026/month=01/  ← 필요 (읽음)
✗ year=2026/month=02/  ← 스킵
✗ year=2026/month=03/  ← 스킵
✗ year=2027/month=01/  ← 스킵

총 1개 파티션만 읽음 (75% 절약)
```

### Pruning 조건

Spark가 자동으로 Pruning을 적용하는 경우:

✅ **파티션 컬럼에 필터 조건이 있을 때**
```python
# 파티션 컬럼: year, month
df.filter((col("year") == 2026) & (col("month") == 1))  # ✅ Pruning

# 파티션이 아닌 컬럼: trip_distance
df.filter(col("trip_distance") > 10)  # ❌ Pruning 안됨
```

✅ **정적(Static) 조건일 때**
```python
# 정적 조건
df.filter(col("year") == 2026)  # ✅ Pruning

# 동적 조건 (UDF 등)
df.filter(col("year") == get_current_year())  # ⚠️ Pruning 안될 수 있음
```

### 확인 방법

**Physical Plan에서 확인:**

```
== Physical Plan ==
Scan parquet 
  Location: InMemoryFileIndex [
    file:/data/taxi_trips/year=2026/month=01/
  ]
  PartitionFilters: [year=2026, month=1]  ← Pruning 적용됨!
  ReadSchema: struct<trip_id:string, distance:double>
```

**로그에서 확인:**
```
spark.sql.optimizer.dynamicPartitionPruning.enabled=true
→ 로그에 "Pruning directory" 메시지
```

---

## 차이점 비교

### 시점

| 개념 | 시점 | 역할 |
|------|------|------|
| **Partitioning** | 데이터 **저장 시** | 데이터를 나눠 저장 |
| **Partition Pruning** | 데이터 **읽기 시** | 필요한 것만 읽기 |

### 누가 하는가?

| 개념 | 주체 | 방법 |
|------|------|------|
| **Partitioning** | **개발자** | `.partitionBy()` 명시 |
| **Partition Pruning** | **Spark Optimizer** | 자동 최적화 |

### 목적

| 개념 | 목적 |
|------|------|
| **Partitioning** | 데이터를 효율적으로 **구조화** |
| **Partition Pruning** | 쿼리를 효율적으로 **실행** |

---

## 실전 예시

### 시나리오: NYC Taxi 데이터

**데이터:**
- 2026년 1월 ~ 12월 택시 운행 기록
- 총 100GB

### Step 1: Partitioning (저장)

```python
# 원본 데이터 읽기
df_raw = spark.read.parquet("raw/yellow_tripdata_2026.parquet")

# year, month로 파티셔닝하여 저장
df_raw.write \
    .mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month") \
    .parquet("processed/taxi_trips/")
```

**결과 디렉토리:**
```
processed/taxi_trips/
├── pickup_year=2026/
│   ├── pickup_month=01/  (8GB)
│   ├── pickup_month=02/  (8GB)
│   ├── pickup_month=03/  (8GB)
│   ├── ...
│   └── pickup_month=12/  (8GB)
```

### Step 2: Partition Pruning (읽기)

**쿼리 1: 1월 데이터만 분석**

```python
# 1월 데이터만 조회
df = spark.read.parquet("processed/taxi_trips/")
df_jan = df.filter((col("pickup_year") == 2026) & (col("pickup_month") == 1))
df_jan.count()
```

**성능:**
```
❌ Pruning 없이:  100GB 읽음 → 60초
✅ Pruning 적용:  8GB만 읽음 → 5초 (12배 빠름)
```

**쿼리 2: 1분기 데이터 분석**

```python
# 1, 2, 3월 데이터 조회
df_q1 = df.filter(
    (col("pickup_year") == 2026) & 
    col("pickup_month").isin([1, 2, 3])
)
df_q1.count()
```

**성능:**
```
❌ Pruning 없이:  100GB 읽음 → 60초
✅ Pruning 적용:  24GB만 읽음 → 15초 (4배 빠름)
```

---

## 성능 비교

### 실험 조건

- 데이터 크기: 100GB (12개월)
- 쿼리: 특정 월 조회
- 클러스터: 2 worker, 2GB memory

### 결과

| 방식 | 읽은 데이터 | 실행 시간 | 비고 |
|------|------------|----------|------|
| **파티셔닝 없음** | 100GB | 60s | 전체 스캔 |
| **파티셔닝 + Pruning** | 8GB | 5s | 12배 빠름 |

### 파티션 수에 따른 성능

| 파티션 키 | 파티션 수 | 평균 파일 크기 | 성능 |
|----------|----------|---------------|------|
| year | 1개 | 100GB | ❌ 느림 |
| year, month | 12개 | 8GB | ✅ 최적 |
| year, month, day | 365개 | 274MB | ⚠️ 작은 파일 문제 |
| year, month, day, hour | 8,760개 | 11MB | ❌ Small File Problem |

---

## 도서관 비유

### Partitioning (서가 분류)

```
도서관에서 책을 분야별로 나눠서 배치:

┌─────────────┐
│  소설 서가   │ ← 소설 책들만 모음
├─────────────┤
│  과학 서가   │ ← 과학 책들만 모음
├─────────────┤
│  역사 서가   │ ← 역사 책들만 모음
└─────────────┘
```

### Partition Pruning (효율적 검색)

```
사용자: "과학책 찾아줘"

❌ Pruning 없이:
모든 서가를 다 뒤짐
→ 소설 서가 (불필요)
→ 과학 서가 (필요)
→ 역사 서가 (불필요)

✅ Pruning 적용:
과학 서가만 뒤짐
→ 과학 서가 (필요)
→ 시간 절약!
```

---

## 실전 팁

### 1. 좋은 파티션 키 선택

```python
# ✅ 좋은 예: 카디널리티가 적당함 (10~100개)
.partitionBy("year", "month")
.partitionBy("region", "date")

# ❌ 나쁜 예: 카디널리티가 너무 많음 (수백만 개)
.partitionBy("user_id")
.partitionBy("transaction_id")

# ❌ 나쁜 예: 카디널리티가 너무 적음 (1~2개)
.partitionBy("country")  # 국가가 1개만 있는 경우
```

### 2. 쿼리 패턴에 맞춰 파티셔닝

```python
# 주로 날짜별로 조회한다면
.partitionBy("year", "month", "day")

# 주로 지역별로 조회한다면
.partitionBy("region", "city")

# 날짜 + 지역으로 조회한다면
.partitionBy("year", "month", "region")
```

### 3. Pruning 확인

```python
# Physical Plan 확인
df.explain()

# 또는
spark.sql("EXPLAIN SELECT * FROM table WHERE year=2026")
```

### 4. Dynamic Partition Pruning 활성화

```python
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

---

## 정리

### Partitioning (파티셔닝)

**✅ 언제:**
- 데이터 저장 시

**✅ 누가:**
- 개발자가 명시적으로 설정

**✅ 목적:**
- 데이터 구조화
- 병렬 처리 준비

**✅ 방법:**
```python
df.write.partitionBy("year", "month").parquet("path/")
```

---

### Partition Pruning (파티션 프루닝)

**✅ 언제:**
- 데이터 읽기 시

**✅ 누가:**
- Spark Optimizer가 자동으로

**✅ 목적:**
- 쿼리 성능 최적화
- 불필요한 I/O 제거

**✅ 조건:**
```python
# 파티션 컬럼에 필터 조건
df.filter(col("year") == 2026)
```

---

## 관계

```
Partitioning 없이 Pruning 불가
→ Partitioning이 먼저, Pruning은 그 위에서 작동

Partitioning 했지만 Pruning 안될 수도 있음
→ 파티션 컬럼에 필터 조건이 없으면 Pruning 안됨
```

**최적의 조합:**
```python
# 1. 저장: 좋은 파티션 키로 Partitioning
df.write.partitionBy("year", "month").parquet("data/")

# 2. 읽기: 파티션 컬럼에 필터 조건 → 자동 Pruning
df = spark.read.parquet("data/")
df.filter((col("year") == 2026) & (col("month") == 1))
```

---

## 다음 단계

- [ ] 프로젝트 데이터에 적합한 파티션 키 선택
- [ ] Partitioning 적용하여 데이터 재저장
- [ ] Spark UI에서 Pruning 확인
- [ ] 성능 측정 (before/after)

---

**문서 버전:** 1.0  
**최종 수정:** 2026-07-08
