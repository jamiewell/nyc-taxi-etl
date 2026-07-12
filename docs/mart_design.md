# Mart Design

이 문서는 NYC Taxi 배치 ETL 프로젝트의 Mart Layer 설계 기준을 정의한다.  
Mart Layer는 `fact_taxi_trip`을 기반으로 BI/Athena/Spark SQL에서 빠르게 조회할 수 있도록 목적별로 사전 집계한 분석 테이블이다.

---

## 1. Mart Layer의 목적

NYC Taxi 원천 데이터는 택시 운행 1건 단위의 상세 데이터다.  
따라서 시간대, 지역, 벤더별 수요와 요금을 분석할 때 매번 전체 fact 데이터를 스캔하면 비용과 시간이 증가한다.

Mart Layer의 목적은 다음과 같다.

1. 자주 조회하는 분석 질문에 맞는 grain으로 데이터를 사전 집계한다.
2. 대량 fact 테이블 반복 스캔을 줄인다.
3. 월별 신규 데이터가 들어왔을 때 해당 월 mart partition만 갱신한다.
4. Spark groupBy, join, window aggregation, partition overwrite 성능 실험 대상을 만든다.
5. 최종적으로 Athena/BI 또는 Spark SQL에서 빠르게 조회 가능한 분석 테이블을 제공한다.

---

## 2. Mart 설계 원칙

### 2.1 Grain을 먼저 정의한다

모든 Mart Table은 한 row가 무엇을 의미하는지 명확해야 한다.

예:

```text
mart_month_hour_zone_trip_metrics
1 row = year_month + pickup_hour + pickup_location_id
```

Grain이 불명확하면 `trip_count`, `total_amount`, `avg_duration_min` 같은 지표가 중복 또는 왜곡될 수 있다.

---

### 2.2 Mart는 Fact를 기준으로 생성한다

모든 Mart는 기본적으로 `fact_taxi_trip`에서 생성한다.

```text
fact_taxi_trip
  ↓ groupBy / join / window
mart tables
```

Raw 또는 Cleaned 데이터를 직접 Mart로 만들 수도 있지만, 프로젝트에서는 데이터 모델링 흐름을 명확히 하기 위해 Fact Layer를 기준으로 한다.

---

### 2.3 Dimension Join은 Mart 생성 시점에 수행한다

조회 편의성을 위해 Mart에는 일부 dimension 설명 컬럼을 포함한다.

예:

```text
pickup_location_id
borough
zone
service_zone
```

또는:

```text
vendor_id
vendor_name
```

이렇게 하면 BI/Athena 조회 시 매번 dimension join을 하지 않아도 된다.

---

### 2.4 Partition은 year/month 기준을 기본으로 한다

Mart Table의 S3 저장 파티션은 기본적으로 `year`, `month`를 사용한다.

권장:

```text
s3://bucket/nyc-taxi/mart/mart_month_hour_zone_trip_metrics/year=2026/month=01/
```

비권장:

```text
s3://bucket/nyc-taxi/mart/mart_month_hour_zone_trip_metrics/year=2026/month=01/hour=08/location_id=132/
```

`hour`, `location_id`, `vendor_id`는 파티션 컬럼이 아니라 일반 컬럼으로 둔다.  
파티션을 너무 세분화하면 small file과 Glue Catalog partition 관리 부담이 커진다.

---

### 2.5 월별 증분 적재를 기본으로 한다

최초 구축 시에는 과거 데이터를 backfill하고, 이후에는 월별 신규 데이터만 처리한다.

```text
Backfill:
2019-01 ~ 2026-01 전체 처리

Incremental:
신규 target_year, target_month만 처리
```

일반 Mart는 해당 월 partition만 overwrite한다.

누적 Mart는 재처리 월 이후의 cumulative 값이 달라질 수 있으므로 별도 재계산 정책이 필요하다.

---

## 3. 최종 Mart 목록

프로젝트에서 우선 구현할 Mart는 다음 4개다.

| Mart Table | 목적 | Grain | 주요 성능 포인트 |
|---|---|---|---|
| `mart_month_hour_zone_trip_metrics` | 월별·시간별·지역별 운행거리, 운행시간, 요금 분석 | year_month + pickup_hour + pickup_location_id | 대량 groupBy, zone join, shuffle, skew |
| `mart_month_hour_vendor_trip_metrics` | 월별·시간별·벤더별 운행시간, 요금 분석 | year_month + pickup_hour + vendor_id | groupBy, vendor broadcast join |
| `mart_month_vendor_cumulative_metrics` | 월별 벤더별 탑승자, 주행거리, 요금 누적 분석 | year_month + vendor_id | window function, cumulative aggregation |
| `mart_month_zone_cumulative_metrics` | 월별 지역별 탑승자, 주행거리, 요금 누적 분석 | year_month + pickup_location_id | cumulative aggregation, zone join, 재처리 정책 |

---

## 4. Mart A: mart_month_hour_zone_trip_metrics

### 4.1 목적

월별·시간별·지역별로 택시 운행거리, 운행시간, 요금 추세를 분석하기 위한 핵심 Mart다.

대응 요구사항:

1. 월별, 시간별, 지역별 주행거리와 요금 추세
2. 월별, 시간별, 지역별 주행시간과 요금 비교

---

### 4.2 Grain

```text
1 row = year_month + pickup_hour + pickup_location_id
```

예:

```text
2026-01 + 08시 + JFK Airport
```

---

### 4.3 Input Table

```text
fact_taxi_trip
dim_taxi_zone
```

---

### 4.4 주요 Group By Key

```text
year
month
year_month
pickup_hour
pickup_location_id
```

---

### 4.5 Output Columns

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

---

### 4.6 지표 정의

| 지표 | 정의 |
|---|---|
| `trip_count` | 정제 조건을 통과한 trip 수 |
| `total_passenger_count` | `SUM(passenger_count)` |
| `total_distance` | `SUM(trip_distance)` |
| `avg_distance` | `AVG(trip_distance)` |
| `total_duration_min` | `SUM(trip_duration_min)` |
| `avg_duration_min` | `AVG(trip_duration_min)` |
| `p50_duration_min` | `percentile_approx(trip_duration_min, 0.5)` |
| `p90_duration_min` | `percentile_approx(trip_duration_min, 0.9)` |
| `total_fare_amount` | `SUM(fare_amount)` |
| `total_tip_amount` | `SUM(tip_amount)` |
| `total_tolls_amount` | `SUM(tolls_amount)` |
| `total_amount` | `SUM(total_amount)` |
| `avg_fare_amount` | `AVG(fare_amount)` |
| `avg_tip_amount` | `AVG(tip_amount)` |
| `avg_total_amount` | `AVG(total_amount)` |
| `fare_per_minute` | `total_amount / total_duration_min` |
| `fare_per_mile` | `total_amount / total_distance` |

---

### 4.7 Spark 처리 특징

```text
fact_taxi_trip
  → groupBy(year_month, pickup_hour, pickup_location_id)
  → aggregate metrics
  → join dim_taxi_zone
  → write partitioned parquet by year/month
```

이 Mart는 프로젝트의 메인 성능 튜닝 대상이다.

성능 실험 포인트:

```text
- groupBy shuffle 발생
- pickup_location_id 기준 데이터 skew 확인
- dim_taxi_zone broadcast join 적용
- percentile_approx 집계 비용 확인
- spark.sql.shuffle.partitions 튜닝
- AQE on/off 비교
- output file 수 확인
```

---

### 4.8 S3 저장 경로

```text
s3://bucket/nyc-taxi/mart/mart_month_hour_zone_trip_metrics/year=2026/month=01/
```

로컬 개발 환경:

```text
data/output/mart/mart_month_hour_zone_trip_metrics/year=2026/month=01/
```

---

## 5. Mart B: mart_month_hour_vendor_trip_metrics

### 5.1 목적

월별·시간별·택시 벤더별 운행시간과 요금 추이를 분석한다.

대응 요구사항:

1. 월별 시간별 택시 벤더별 요금 추이
2. 월별 시간별 벤더별 주행시간과 요금 비교

---

### 5.2 Grain

```text
1 row = year_month + pickup_hour + vendor_id
```

---

### 5.3 Input Table

```text
fact_taxi_trip
dim_vendor
```

---

### 5.4 Output Columns

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

---

### 5.5 Spark 처리 특징

```text
fact_taxi_trip
  → groupBy(year_month, pickup_hour, vendor_id)
  → aggregate metrics
  → join dim_vendor
  → write partitioned parquet by year/month
```

성능 실험 포인트:

```text
- dim_vendor는 작은 dimension이므로 broadcast join 적용
- vendor_id cardinality가 낮아 groupBy 결과가 작음
- fact 재사용 시 persist/cache 효과 확인 가능
```

---

### 5.6 S3 저장 경로

```text
s3://bucket/nyc-taxi/mart/mart_month_hour_vendor_trip_metrics/year=2026/month=01/
```

---

## 6. Mart C: mart_month_vendor_cumulative_metrics

### 6.1 목적

월별 벤더별 누적 탑승자 수, 주행거리, 요금 누적량을 분석한다.

대응 요구사항:

1. 월별 누적 탑승자와 벤더별 총 주행 요금
2. 월별 벤더별 요금 누적량

---

### 6.2 Grain

```text
1 row = year_month + vendor_id
```

---

### 6.3 Input Table

```text
fact_taxi_trip
dim_vendor
```

또는 월별 집계 중간 결과:

```text
monthly_vendor_metrics
```

---

### 6.4 Output Columns

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

---

### 6.5 누적 지표 계산 방식

#### 방식 A: 전체 기간 재계산

```text
전체 fact 또는 monthly aggregate를 읽음
→ vendor_id, year_month 기준 정렬
→ window function으로 cumulative 계산
```

장점:

```text
구현이 단순하고 결과 정합성이 높다.
```

단점:

```text
매월 전체 기간을 다시 읽어야 하므로 비효율적이다.
```

---

#### 방식 B: 이전 누적값 + 신규 월 집계

```text
이전 월 cumulative 값 읽기
+ 신규 월 monthly 집계
→ 이번 월 cumulative 계산
```

장점:

```text
월별 증분 처리에 적합하다.
```

단점:

```text
이전 누적값 관리와 재처리 정책이 필요하다.
```

---

### 6.6 재처리 정책

누적 Mart는 특정 과거 월을 재처리하면 이후 누적값이 달라진다.

예:

```text
2024-03 재처리
→ 2024-03, 2024-04, ..., 최신 월까지 cumulative 재계산 필요
```

따라서 누적 Mart 재처리 정책은 다음 중 하나를 선택한다.

```text
1. 재처리 월부터 최신 월까지 cumulative 재계산
2. 전체 누적 Mart 재계산
3. 개인 프로젝트에서는 우선 전체 재계산 방식으로 구현 후 증분 방식으로 개선
```

---

### 6.7 Spark 처리 특징

성능 실험 포인트:

```text
- window function 사용
- monthly aggregate 재사용
- 전체 재계산 방식과 증분 방식 비교
- dim_vendor broadcast join
```

---

### 6.8 S3 저장 경로

```text
s3://bucket/nyc-taxi/mart/mart_month_vendor_cumulative_metrics/year=2026/month=01/
```

---

## 7. Mart D: mart_month_zone_cumulative_metrics

### 7.1 목적

월별 지역별 누적 탑승자 수, 주행거리, 요금 누적량을 분석한다.

대응 요구사항:

1. 월별 지역별 요금 누적량

---

### 7.2 Grain

```text
1 row = year_month + pickup_location_id
```

---

### 7.3 Input Table

```text
fact_taxi_trip
dim_taxi_zone
```

또는 월별 집계 중간 결과:

```text
monthly_zone_metrics
```

---

### 7.4 Output Columns

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

---

### 7.5 Spark 처리 특징

```text
fact_taxi_trip
  → groupBy(year_month, pickup_location_id)
  → monthly zone aggregate
  → window cumulative aggregation
  → join dim_taxi_zone
  → write partitioned parquet by year/month
```

성능 실험 포인트:

```text
- pickup_location_id 기준 groupBy shuffle
- 특정 지역 데이터 skew 확인
- dim_taxi_zone broadcast join
- window function 사용
- 누적 Mart 재처리 정책 필요
```

---

### 7.6 S3 저장 경로

```text
s3://bucket/nyc-taxi/mart/mart_month_zone_cumulative_metrics/year=2026/month=01/
```

---

## 8. Partial Mart 설계

월별 전체 데이터를 한 번에 groupBy하면 지역·시간 기준 Mart에서 큰 shuffle이 발생할 수 있다.  
이를 실험하기 위해 무거운 Mart에 한해 Partial Mart Layer를 추가할 수 있다.

---

### 8.1 적용 대상

Partial Mart 적용 추천:

```text
mart_month_hour_zone_trip_metrics
```

Partial Mart 적용 비추천 또는 후순위:

```text
mart_month_hour_vendor_trip_metrics
mart_month_vendor_cumulative_metrics
mart_month_zone_cumulative_metrics
```

---

### 8.2 Partial Mart 흐름

```text
fact_taxi_trip / target month
  ↓
day 단위 partial aggregation
  ↓
partial_mart_day_hour_zone_trip_metrics
  ↓
monthly final aggregation
  ↓
mart_month_hour_zone_trip_metrics
```

---

### 8.3 Partial Mart Table

테이블명:

```text
partial_mart_day_hour_zone_trip_metrics
```

Grain:

```text
1 row = pickup_date + pickup_hour + pickup_location_id
```

저장 경로:

```text
s3://bucket/nyc-taxi/partial_mart/day_hour_zone_trip_metrics/year=2026/month=01/day=01/
```

장점:

```text
- 특정 일자 실패 시 day 단위 재처리 가능
- Step Functions Map 또는 병렬 job 실험 가능
- 월별 final aggregation의 input size 감소
- partial aggregation 설계 경험 확보
```

단점:

```text
- 중간 산출물 저장 공간 증가
- final aggregation 단계 추가
- small file 관리 필요
- 데이터 정합성 검증 단계 증가
```

---

## 9. 적재 전략

### 9.1 최초 Backfill

```text
1. Raw 전체 월 수집
2. Cleaned 전체 월 생성
3. Fact 전체 월 생성
4. Mart 전체 월 생성
5. Cumulative Mart 전체 재계산
```

예:

```text
2019-01 ~ 2026-01
```

---

### 9.2 월별 Incremental Load

```text
1. 신규 raw 파일 수집
2. cleaned target month overwrite
3. fact target month overwrite
4. 일반 Mart target month overwrite
5. 누적 Mart는 target month 이후 재계산 또는 전체 재계산
6. validation 수행
7. batch_run_log 기록
```

---

### 9.3 overwrite 정책

Spark 설정:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

일반 Mart:

```text
해당 year/month partition만 overwrite
```

누적 Mart:

```text
target month 이후 partition 재계산 필요
```

---

## 10. Glue Catalog 등록 전략

Mart 데이터는 S3에 Parquet로 저장하고 Glue Data Catalog에 External Table로 등록한다.

단, `MSCK REPAIR TABLE`은 전체 S3 prefix를 탐색할 수 있어 파티션이 많아질수록 느려질 수 있다.  
월별 신규 파티션을 알고 있는 배치 구조에서는 아래 방식을 우선 사용한다.

```sql
ALTER TABLE mart_month_hour_zone_trip_metrics
ADD IF NOT EXISTS
PARTITION (year='2026', month='01')
LOCATION 's3://bucket/nyc-taxi/mart/mart_month_hour_zone_trip_metrics/year=2026/month=01/';
```

권장:

```text
- 초기 테이블은 DDL로 생성
- 신규 월 partition은 ALTER TABLE ADD PARTITION 또는 Glue API로 등록
- Raw Layer 탐색에는 Glue Crawler 사용 가능
- Fact/Mart Layer는 Crawler보다 DDL 기반 관리 권장
```

---

## 11. 데이터 품질 검증

Mart 생성 후 다음 검증을 수행한다.

### 11.1 Row Count 검증

```text
SUM(mart_month_hour_zone_trip_metrics.trip_count)
= COUNT(fact_taxi_trip for target month)
```

단, Mart grain과 기준이 동일한 경우에만 적용한다.

---

### 11.2 금액 검증

```text
SUM(mart.total_amount)
= SUM(fact.total_amount)
```

---

### 11.3 Null 검증

```text
pickup_location_id is not null
vendor_id is not null
year_month is not null
```

---

### 11.4 Dimension Join 검증

```text
zone is not null 비율 확인
vendor_name is not null 비율 확인
```

---

## 12. Spark 성능 실험 연결

Mart 설계는 Spark 성능 실험과 직접 연결된다.

| Mart | 주요 연산 | 실험 포인트 |
|---|---|---|
| `mart_month_hour_zone_trip_metrics` | groupBy + zone join + percentile | shuffle partition, skew, broadcast join, AQE |
| `mart_month_hour_vendor_trip_metrics` | groupBy + vendor join | broadcast join, column pruning |
| `mart_month_vendor_cumulative_metrics` | monthly aggregation + window | window function, cumulative 방식 비교 |
| `mart_month_zone_cumulative_metrics` | zone aggregation + window + join | skew, window, 재처리 정책 |
| `partial_mart_day_hour_zone_trip_metrics` | day partial aggregation | partial aggregation vs monthly full aggregation 비교 |

---

## 13. 구현 우선순위

### Phase 1

```text
1. mart_month_hour_zone_trip_metrics 구현
2. Spark UI로 baseline 측정
3. shuffle partition 튜닝
4. dim_taxi_zone broadcast join 적용
```

### Phase 2

```text
1. mart_month_hour_vendor_trip_metrics 구현
2. dim_vendor broadcast join 적용
3. fact persist/cache 실험
```

### Phase 3

```text
1. mart_month_vendor_cumulative_metrics 구현
2. mart_month_zone_cumulative_metrics 구현
3. window cumulative 방식 구현
```

### Phase 4

```text
1. partial_mart_day_hour_zone_trip_metrics 구현
2. 월 전체 직접 집계 방식과 partial aggregation 방식 비교
3. 성능 실험 결과 문서화
```

---

## 14. 포트폴리오 설명 문장

본 프로젝트는 NYC Taxi Trip 데이터를 단순히 Spark로 빠르게 처리하는 것이 아니라, BI 분석 요구사항을 가정하여 목적별 Mart Table을 설계하는 것을 목표로 한다.  
원천 데이터는 택시 운행 1건 단위이므로 `fact_taxi_trip`을 기준으로 지역, 시간, 벤더 차원과 결합해 분석 가능한 구조로 모델링했다.

Mart Layer는 월별·시간별·지역별 운행거리와 요금 추세, 벤더별 요금 추이, 월별 누적 탑승자 및 요금 누적량 분석을 지원하도록 설계했다.  
특히 Spark 성능 학습 목적을 반영하여 대량 groupBy, dimension join, window cumulative aggregation, partition overwrite, partial aggregation이 발생하는 구조로 설계했다.

이를 통해 Spark UI 기반으로 shuffle read/write, skew, broadcast join, AQE, persist/cache, small file 문제를 실험하고 개선한다.
