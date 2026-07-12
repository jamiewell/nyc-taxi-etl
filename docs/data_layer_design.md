# Data Layer Design

이 문서는 NYC Taxi 배치 ETL 프로젝트의 데이터 레이어 설계를 정의한다.  
목표는 원천 NYC Taxi 데이터를 단순히 처리하는 것이 아니라, 데이터 엔지니어링 관점에서 Raw → Cleaned → Fact → Dimension → Mart 구조로 단계적으로 적재하고, Spark 성능 튜닝이 가능한 데이터 처리 흐름을 만드는 것이다.

---

## 1. 설계 목표

본 프로젝트의 데이터 레이어 설계 목표는 다음과 같다.

1. NYC Taxi 원천 데이터를 재처리 가능한 형태로 보존한다.
2. 원천 데이터의 품질 문제를 정제하고 표준 컬럼을 생성한다.
3. 택시 운행 1건 단위의 Fact 테이블을 구성한다.
4. 지역, 벤더, 날짜, 시간 등 분석 기준이 되는 Dimension 테이블을 구성한다.
5. 분석 요구사항에 맞는 Mart 테이블을 생성한다.
6. 월별 신규 데이터가 들어오는 시나리오에서 증분 적재가 가능하도록 설계한다.
7. Spark groupBy, join, shuffle, partition overwrite가 자연스럽게 발생하는 구조를 만들어 성능 튜닝 실험이 가능하게 한다.

---

## 2. 전체 데이터 흐름

```text
Raw Layer
  ↓
Cleaned Layer
  ↓
Warehouse Layer
  ├── Fact Tables
  └── Dimension Tables
  ↓
Mart Layer
```

상세 흐름은 다음과 같다.

```text
NYC Taxi 원천 parquet/csv
  ↓
raw_yellow_taxi_trip
  ↓
cleaned_yellow_taxi_trip
  ↓
fact_taxi_trip
  ↓
목적별 mart tables
```

Dimension 테이블은 Fact/Mart 생성 과정에서 join된다.

```text
dim_taxi_zone
      ↓
fact_taxi_trip → mart_month_hour_zone_trip_metrics

dim_vendor
      ↓
fact_taxi_trip → mart_month_hour_vendor_trip_metrics
```

---

## 3. 데이터 레이어 정의

## 3.1 Raw Layer

### 목적

Raw Layer는 외부에서 수집한 NYC Taxi 원천 데이터를 가능한 원본 형태로 보존하는 영역이다.

이 레이어에서는 비즈니스 정제나 컬럼 변환을 수행하지 않는다. 재처리와 감사 추적을 위해 원본 파일을 유지한다.

### 입력 데이터

- NYC TLC Yellow Taxi Trip Data
- 파일 형식: parquet 또는 csv
- 기본 프로젝트 샘플: `yellow_tripdata_2026-01.parquet`

### 저장 경로 예시

로컬 개발 환경:

```text
data/input/yellow/year=2026/month=01/
```

AWS S3 확장 시:

```text
s3://<bucket>/nyc-taxi/raw/yellow/year=2026/month=01/
```

### 테이블명

```text
raw_yellow_taxi_trip
```

### 파티션 기준

```text
year
month
```

### 주요 원천 컬럼

```text
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
passenger_count
trip_distance
RatecodeID
store_and_fwd_flag
PULocationID
DOLocationID
payment_type
fare_amount
extra
mta_tax
tip_amount
tolls_amount
improvement_surcharge
total_amount
congestion_surcharge
airport_fee
cbd_congestion_fee
```

### 설계 원칙

- 원천 파일은 수정하지 않는다.
- 파일명과 수집 월 정보를 유지한다.
- Raw Layer는 재처리의 기준 데이터로 사용한다.
- Raw Layer에서 복잡한 정제 로직을 수행하지 않는다.

---

## 3.2 Cleaned Layer

### 목적

Cleaned Layer는 Raw 데이터를 분석 가능한 표준 형태로 정제한 영역이다.

이 단계에서는 데이터 품질 필터링, 컬럼명 표준화, 시간 파생 컬럼 생성, 기본 이상치 제거를 수행한다.

### 입력

```text
raw_yellow_taxi_trip
```

### 출력

```text
cleaned_yellow_taxi_trip
```

### 저장 경로 예시

로컬 개발 환경:

```text
data/output/cleaned/yellow_trip/pickup_year=2026/pickup_month=1/
```

AWS S3 확장 시:

```text
s3://<bucket>/nyc-taxi/cleaned/yellow_trip/year=2026/month=01/
```

### 파티션 기준

현재 로컬 코드 기준:

```text
pickup_year
pickup_month
```

AWS 확장 시 권장 기준:

```text
year
month
```

### 주요 변환 작업

1. 컬럼명 표준화
2. 시간 컬럼 타입 변환
3. 시간 파생 컬럼 생성
4. 데이터 품질 필터 적용
5. 분석에 필요한 컬럼만 선택

### 시간 파생 컬럼

```text
pickup_year
pickup_month
pickup_day
pickup_hour
pickup_dayofweek
```

추가 권장 컬럼:

```text
pickup_date
year_month
trip_duration_min
```

### 데이터 품질 필터

기본 필터:

```text
passenger_count > 0
trip_distance > 0
fare_amount > 0
total_amount > 0
```

추가 권장 필터:

```text
tpep_pickup_datetime is not null
tpep_dropoff_datetime is not null
tpep_dropoff_datetime > tpep_pickup_datetime
PULocationID is not null
DOLocationID is not null
trip_duration_min > 0
trip_duration_min <= 1440
```

### Cleaned Layer의 역할

- Fact/Mart 생성의 공통 입력 데이터
- 원천 데이터 품질 문제 제거
- 반복 사용 가능한 표준 데이터셋 제공
- Spark 성능 실험에서 column pruning, predicate pushdown, partition pruning 확인 대상

---

## 3.3 Warehouse Layer - Fact Table

### 목적

Warehouse Layer의 Fact Table은 분석의 중심이 되는 상세 이벤트 데이터를 저장한다.

NYC Taxi 프로젝트에서 핵심 Fact는 택시 운행 1건 단위의 `fact_taxi_trip`이다.

### 테이블명

```text
fact_taxi_trip
```

### Grain

```text
1 row = 택시 운행 1건
```

### 입력

```text
cleaned_yellow_taxi_trip
```

### 저장 경로 예시

로컬 개발 환경:

```text
data/output/warehouse/fact_taxi_trip/year=2026/month=01/
```

AWS S3 확장 시:

```text
s3://<bucket>/nyc-taxi/warehouse/fact_taxi_trip/year=2026/month=01/
```

### 파티션 기준

```text
year
month
```

### 주요 컬럼

```text
trip_id
vendor_id
pickup_datetime
dropoff_datetime
pickup_date
year_month
pickup_year
pickup_month
pickup_day
pickup_hour
pickup_dayofweek
pickup_location_id
dropoff_location_id
payment_type
ratecode_id
passenger_count
trip_distance
trip_duration_min
fare_amount
tip_amount
tolls_amount
total_amount
airport_fee
cbd_congestion_fee
```

### 파생 지표 후보

```text
tip_rate = tip_amount / total_amount
fare_per_mile = fare_amount / trip_distance
fare_per_minute = fare_amount / trip_duration_min
```

### Fact Table 설계 원칙

- 한 row는 반드시 하나의 trip을 의미한다.
- 집계된 값은 Fact Table에 저장하지 않는다.
- Mart 생성에 필요한 최소한의 분석 컬럼을 포함한다.
- Dimension join에 필요한 key를 유지한다.
- 월별 재처리가 가능하도록 year/month partition을 유지한다.

---

## 3.4 Warehouse Layer - Dimension Tables

Dimension Table은 Fact Table을 사람이 이해할 수 있게 설명하는 기준 정보 테이블이다.

## 3.4.1 dim_vendor

### 목적

VendorID를 벤더명으로 해석하기 위한 차원 테이블이다.

### Grain

```text
1 row = 1 vendor
```

### 컬럼

```text
vendor_id
vendor_name
```

### 예시

```text
1 = Creative Mobile Technologies
2 = VeriFone
```

### 저장 경로

```text
data/output/dim/vendor/
```

AWS S3 확장 시:

```text
s3://<bucket>/nyc-taxi/dim/vendor/
```

---

## 3.4.2 dim_taxi_zone

### 목적

PULocationID, DOLocationID를 Borough, Zone, Service Zone으로 해석하기 위한 차원 테이블이다.

### Grain

```text
1 row = 1 taxi zone location
```

### 컬럼

```text
location_id
borough
zone
service_zone
```

### 저장 경로

```text
data/output/dim/taxi_zone/
```

AWS S3 확장 시:

```text
s3://<bucket>/nyc-taxi/dim/taxi_zone/
```

---

## 3.4.3 dim_date

### 목적

날짜 기준 분석을 쉽게 하기 위한 달력 차원 테이블이다.

### Grain

```text
1 row = 1 calendar date
```

### 컬럼

```text
date_id
date
year
month
day
day_of_week
day_name
week_of_year
quarter
is_weekend
```

### 저장 경로

```text
data/output/dim/date/
```

---

## 3.4.4 dim_time

### 목적

시간대 분석을 쉽게 하기 위한 시간 차원 테이블이다.

### Grain

```text
1 row = 1 hour
```

### 컬럼

```text
hour
hour_label
time_bucket
```

### 예시

```text
07 = morning_commute
12 = lunch
18 = evening_commute
23 = night
```

### 저장 경로

```text
data/output/dim/time/
```

---

## 3.5 Mart Layer

### 목적

Mart Layer는 BI/Athena/Spark SQL 조회 성능을 위해 사전 집계된 분석 목적별 테이블을 저장하는 영역이다.

Fact Table은 trip 1건 단위이기 때문에 분석 시 매번 대량의 원천 데이터를 groupBy해야 한다. Mart Table은 자주 사용하는 분석 질문에 맞춰 미리 집계된 결과를 제공한다.

---

## 3.5.1 mart_month_hour_zone_trip_metrics

### 목적

월별, 시간별, 지역별 주행거리·주행시간·요금 추세 분석

### 대응 요구사항

- 월별, 시간별, 지역별 주행거리와 요금 추세
- 월별, 시간별, 지역별 주행시간과 요금 비교

### Grain

```text
1 row = year_month + pickup_hour + pickup_location_id
```

### 입력

```text
fact_taxi_trip
dim_taxi_zone
```

### 주요 컬럼

```text
year
month
year_month
pickup_hour
pickup_location_id
borough
zone
service_zone
trip_count
total_passenger_count
total_distance
avg_distance
total_duration_min
avg_duration_min
p50_duration_min
p90_duration_min
total_fare_amount
total_tip_amount
total_tolls_amount
total_amount
avg_fare_amount
avg_tip_amount
avg_total_amount
fare_per_minute
fare_per_mile
created_at
updated_at
```

### 저장 경로

```text
data/output/mart/month_hour_zone_trip_metrics/year=2026/month=01/
```

AWS S3 확장 시:

```text
s3://<bucket>/nyc-taxi/mart/mart_month_hour_zone_trip_metrics/year=2026/month=01/
```

### 성능 의도

- `year_month + pickup_hour + pickup_location_id` 기준 대량 groupBy 발생
- `dim_taxi_zone` join 발생
- 지역별 데이터 skew 확인 가능
- `percentile_approx` 적용 시 aggregation 비용 증가
- shuffle partition 튜닝 대상

---

## 3.5.2 mart_month_hour_vendor_trip_metrics

### 목적

월별, 시간별, 벤더별 운행시간과 요금 추이 분석

### 대응 요구사항

- 월별 시간별 택시 벤더별 요금 추이
- 월별, 시간별 벤더별 주행시간과 요금 비교

### Grain

```text
1 row = year_month + pickup_hour + vendor_id
```

### 입력

```text
fact_taxi_trip
dim_vendor
```

### 주요 컬럼

```text
year
month
year_month
pickup_hour
vendor_id
vendor_name
trip_count
total_passenger_count
total_distance
avg_distance
total_duration_min
avg_duration_min
p50_duration_min
p90_duration_min
total_fare_amount
total_tip_amount
total_amount
avg_fare_amount
avg_total_amount
fare_per_minute
fare_per_mile
created_at
updated_at
```

### 저장 경로

```text
data/output/mart/month_hour_vendor_trip_metrics/year=2026/month=01/
```

### 성능 의도

- `year_month + pickup_hour + vendor_id` 기준 groupBy 발생
- `dim_vendor`는 작은 dimension이므로 broadcast join 대상
- Fact 재사용 시 persist/cache 효과 확인 가능

---

## 3.5.3 mart_month_vendor_cumulative_metrics

### 목적

월별 벤더별 탑승자, 주행거리, 요금 누적량 분석

### 대응 요구사항

- 월별 누적 탑승자와 벤더별 총 주행 요금
- 월별 벤더별 요금 누적량

### Grain

```text
1 row = year_month + vendor_id
```

### 입력

```text
fact_taxi_trip
dim_vendor
```

### 주요 컬럼

```text
year
month
year_month
vendor_id
vendor_name
monthly_trip_count
monthly_passenger_count
monthly_distance
monthly_fare_amount
monthly_tip_amount
monthly_total_amount
cumulative_trip_count
cumulative_passenger_count
cumulative_distance
cumulative_fare_amount
cumulative_tip_amount
cumulative_total_amount
created_at
updated_at
```

### 저장 경로

```text
data/output/mart/month_vendor_cumulative_metrics/year=2026/month=01/
```

### 성능 의도

- 월별 집계 후 window function 또는 이전 누적 Mart와 join
- 누적 집계 처리 방식 비교 가능
- 전체 재계산 방식과 증분 누적 방식 비교 가능

### 누적 처리 정책

기본 정책:

```text
신규 월 적재 시 이전 월 cumulative 값 + 신규 월 monthly 값으로 이번 월 cumulative 계산
```

재처리 정책:

```text
특정 월을 재처리하면 해당 월부터 최신 월까지 cumulative 값을 재계산한다.
```

---

## 3.5.4 mart_month_zone_cumulative_metrics

### 목적

월별 지역별 주행거리, 탑승자, 요금 누적량 분석

### 대응 요구사항

- 월별 지역별 요금 누적량

### Grain

```text
1 row = year_month + pickup_location_id
```

### 입력

```text
fact_taxi_trip
dim_taxi_zone
```

### 주요 컬럼

```text
year
month
year_month
pickup_location_id
borough
zone
service_zone
monthly_trip_count
monthly_passenger_count
monthly_distance
monthly_fare_amount
monthly_tip_amount
monthly_total_amount
cumulative_trip_count
cumulative_passenger_count
cumulative_distance
cumulative_fare_amount
cumulative_tip_amount
cumulative_total_amount
created_at
updated_at
```

### 저장 경로

```text
data/output/mart/month_zone_cumulative_metrics/year=2026/month=01/
```

### 성능 의도

- 지역 기준 groupBy 발생
- `dim_taxi_zone` join 발생
- 특정 location_id 데이터 skew 확인 가능
- 이전 누적 Mart와 신규 월 집계 join 가능

---

## 4. Partial Mart Layer Optional

월별 전체 데이터를 한 번에 집계할 경우 지역·시간 기준 groupBy에서 큰 shuffle이 발생할 수 있다. 이를 개선하기 위해 무거운 지역·시간 마트는 일별 partial aggregation을 적용할 수 있다.

### 적용 대상

```text
mart_month_hour_zone_trip_metrics
```

### 처리 흐름

```text
fact_taxi_trip/year=2026/month=01
  ↓
partial_mart/day_hour_zone_trip_metrics/year=2026/month=01/day=01
partial_mart/day_hour_zone_trip_metrics/year=2026/month=01/day=02
...
  ↓
mart/month_hour_zone_trip_metrics/year=2026/month=01
```

### 장점

- 월 전체 groupBy 부담 감소
- 특정 일자 실패 시 해당 일자만 재처리 가능
- Step Functions Map 또는 병렬 실행 실험 가능
- partial aggregation 성능 비교 가능

### 단점

- 중간 산출물 관리 필요
- partial 결과를 다시 final mart로 집계해야 함
- small file 문제가 발생할 수 있음

### 저장 경로 예시

```text
data/output/partial_mart/day_hour_zone_trip_metrics/year=2026/month=01/day=01/
```

AWS S3 확장 시:

```text
s3://<bucket>/nyc-taxi/partial_mart/day_hour_zone_trip_metrics/year=2026/month=01/day=01/
```

---

## 5. 파티션 설계

### 기본 원칙

- 저장 파티션은 과도하게 세분화하지 않는다.
- 기본 파티션은 `year/month`를 사용한다.
- `hour`, `vendor_id`, `location_id`는 기본적으로 파티션 컬럼이 아니라 일반 컬럼으로 둔다.
- Spark 내부 처리 병렬성과 S3 저장 파티션은 별개로 관리한다.

### 레이어별 파티션

| Layer | Table | Partition |
|---|---|---|
| Raw | raw_yellow_taxi_trip | year, month |
| Cleaned | cleaned_yellow_taxi_trip | year, month |
| Fact | fact_taxi_trip | year, month |
| Mart | mart_month_hour_zone_trip_metrics | year, month |
| Mart | mart_month_hour_vendor_trip_metrics | year, month |
| Mart | mart_month_vendor_cumulative_metrics | year, month |
| Mart | mart_month_zone_cumulative_metrics | year, month |
| Partial Mart | partial_day_hour_zone_trip_metrics | year, month, day |

### 비권장 파티션

```text
year/month/day/hour/location_id
```

이와 같은 구조는 파티션 수가 급증하여 Glue Catalog, Athena, Spark metadata 처리 비용을 증가시킬 수 있다.

---

## 6. 적재 전략

## 6.1 초기 Backfill

초기 구축 시 과거 여러 월의 데이터를 한 번에 처리한다.

예시:

```text
2019-01 ~ 2026-01 전체 처리
```

처리 흐름:

```text
Raw 전체 적재
  ↓
Cleaned 전체 생성
  ↓
Fact 전체 생성
  ↓
Mart 전체 생성
  ↓
Cumulative Mart 전체 생성
```

### Backfill 특징

- 대량 데이터 처리로 Spark 성능 튜닝 대상
- 전체 기간 기준 cumulative 계산 가능
- 처리 시간이 길기 때문에 실험 결과 기록 필요

---

## 6.2 월별 Incremental Load

매월 신규 데이터가 들어오면 해당 월만 처리한다.

처리 흐름:

```text
신규 raw/year=YYYY/month=MM 수집
  ↓
cleaned/year=YYYY/month=MM overwrite
  ↓
fact/year=YYYY/month=MM overwrite
  ↓
일반 mart/year=YYYY/month=MM overwrite
  ↓
cumulative mart는 이전 누적값 + 신규 월 집계로 계산
  ↓
validation 수행
  ↓
batch_run_log 기록
```

### 일반 Mart 적재 정책

```text
해당 year/month partition만 overwrite
```

### Cumulative Mart 적재 정책

신규 월 적재:

```text
이전 월 cumulative + 신규 월 monthly aggregate
```

과거 월 재처리:

```text
재처리 대상 월부터 최신 월까지 cumulative 재계산
```

---

## 7. Glue Catalog / Metastore 설계

### 목적

Glue Catalog 또는 Hive Metastore는 S3/Parquet 데이터의 테이블명, 스키마, 파티션 정보를 관리하기 위한 메타데이터 저장소다.

### 중요한 원칙

Glue Catalog 등록은 S3 write 성능을 높이는 목적이 아니다.  
목적은 Spark SQL, Athena, Glue, EMR 등이 테이블명 기반으로 데이터를 조회하고 partition pruning을 수행할 수 있게 하는 것이다.

### 권장 방식

Raw Layer:

```text
Glue Crawler 사용 가능
```

Cleaned/Fact/Mart Layer:

```text
DDL로 직접 테이블 생성 권장
```

신규 파티션 등록:

```text
MSCK REPAIR TABLE보다 ALTER TABLE ADD PARTITION 또는 Glue API 권장
```

### 이유

- Raw 데이터는 스키마 탐색 목적이 크므로 Crawler가 유용하다.
- Fact/Mart는 grain과 컬럼 타입을 명확히 설계한 테이블이므로 자동 추론보다 DDL 관리가 적합하다.
- 월별 신규 파티션은 이미 알고 있으므로 전체 S3 경로를 스캔하는 MSCK REPAIR TABLE보다 명시적 파티션 등록이 효율적이다.

---

## 8. 데이터 품질 검증 기준

각 레이어 적재 후 다음 검증을 수행한다.

### Raw → Cleaned

```text
raw_count >= cleaned_count
cleaned_count > 0
passenger_count <= 0 count = 0
trip_distance <= 0 count = 0
fare_amount <= 0 count = 0
total_amount <= 0 count = 0
pickup_datetime null count = 0
dropoff_datetime null count = 0
```

### Cleaned → Fact

```text
fact_count = cleaned_count 또는 사전에 정의한 추가 필터 반영 후 count
trip_duration_min > 0
tip_rate >= 0
fare_per_mile > 0
```

### Fact → Mart

```text
SUM(mart_month_hour_zone_trip_metrics.trip_count) = COUNT(fact_taxi_trip)
SUM(mart_month_hour_vendor_trip_metrics.trip_count) = COUNT(fact_taxi_trip)
SUM(mart_month_vendor_cumulative_metrics.monthly_trip_count) = COUNT(fact_taxi_trip) by month
SUM(mart_month_zone_cumulative_metrics.monthly_trip_count) = COUNT(fact_taxi_trip) by month
```

---

## 9. Spark 성능 튜닝 연결 지점

이 데이터 레이어 구조는 Spark 성능 실험을 위해 다음 병목을 의도적으로 포함한다.

| 처리 구간 | 발생 연산 | 튜닝 포인트 |
|---|---|---|
| Raw → Cleaned | filter, select, write | column pruning, predicate pushdown |
| Cleaned → Fact | derived column, write | repartition, output file size |
| Fact → Zone Mart | groupBy, zone join | shuffle partition, broadcast join, skew |
| Fact → Vendor Mart | groupBy, vendor join | broadcast join, persist |
| Fact → Cumulative Mart | groupBy, window | cumulative strategy, partition pruning |
| Mart Write | parquet write | coalesce/repartition, small file control |

### 실험 후보

```text
spark.sql.shuffle.partitions = 50 / 100 / 200 / 400
repartition by year_month, pickup_hour, pickup_location_id
AQE on/off
broadcast join on/off
persist fact_taxi_trip on/off
day partial aggregation on/off
vendor split processing vs month processing
```

---

## 10. 현재 구현과 향후 확장

### 현재 구현 상태

현재 `jobs/nyc_taxi_etl.py`는 Raw 데이터를 읽어 기본 정제 후 parquet로 저장하는 단일 ETL job이다.

현재 변환:

```text
tpep_pickup_datetime 기준 year/month/day/hour/dayofweek 생성
passenger_count > 0
trip_distance > 0
fare_amount > 0
total_amount > 0
pickup_year, pickup_month 기준 parquet 저장
```

### 향후 분리 계획

```text
jobs/raw_to_cleaned.py
jobs/cleaned_to_fact.py
jobs/build_dim_vendor.py
jobs/build_dim_taxi_zone.py
jobs/build_mart_month_hour_zone_trip_metrics.py
jobs/build_mart_month_hour_vendor_trip_metrics.py
jobs/build_mart_month_vendor_cumulative_metrics.py
jobs/build_mart_month_zone_cumulative_metrics.py
```

---

## 11. 최종 목표 구조

```text
data/
  input/
    yellow/year=2026/month=01/

  output/
    cleaned/
      yellow_trip/year=2026/month=01/

    warehouse/
      fact_taxi_trip/year=2026/month=01/

    dim/
      vendor/
      taxi_zone/
      date/
      time/

    partial_mart/
      day_hour_zone_trip_metrics/year=2026/month=01/day=01/

    mart/
      month_hour_zone_trip_metrics/year=2026/month=01/
      month_hour_vendor_trip_metrics/year=2026/month=01/
      month_vendor_cumulative_metrics/year=2026/month=01/
      month_zone_cumulative_metrics/year=2026/month=01/
```

AWS S3 확장 시:

```text
s3://<bucket>/nyc-taxi/
  raw/
  cleaned/
  warehouse/
  dim/
  partial_mart/
  mart/
```

---

## 12. 요약

본 데이터 레이어 설계는 다음 목적을 가진다.

1. Raw 원천 데이터 보존
2. Cleaned 표준 데이터 생성
3. Trip 1건 단위 Fact Table 구성
4. 분석 기준 Dimension Table 구성
5. 목적별 Mart Table 생성
6. 월별 증분 적재와 재처리 가능 구조 확보
7. Spark groupBy, join, shuffle, cumulative aggregation 튜닝 실험 가능 구조 설계

이 구조를 통해 프로젝트는 단순 Spark ETL이 아니라 데이터 모델링, 마트 설계, 월별 적재 전략, Spark 성능 개선을 함께 다루는 데이터 엔지니어링 프로젝트가 된다.
