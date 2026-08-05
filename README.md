# NYC Taxi Batch ETL with PySpark

NYC Taxi 데이터를 처리하는 PySpark ETL 프로젝트입니다.

## 구조

```
nyc-taxi-batch/
├── docker/
│   ├── Dockerfile              # Spark 3.5.3 이미지 (현재 미사용, bitnami/spark 사용 중)
│   ├── docker-compose.yml      # Spark standalone cluster 설정
│   └── start-spark.sh          # Spark 시작 스크립트
├── jobs/
│   ├── main.py                  # 파이프라인 오케스트레이터 (Raw→Cleaned→Fact/Dim→Mart)
│   ├── save_raw_layer.py        # Raw Layer: 원본 데이터 보존
│   ├── raw_to_cleaned.py        # Cleaned Layer: 품질 필터 + 표준 컬럼
│   ├── cleaned_to_fact.py       # Fact/Dimension Layer: fact_taxi_trip, dim_vendor, dim_taxi_zone
│   ├── fact_to_mart_zone.py             # Mart: mart_month_hour_zone_trip_metrics
│   ├── fact_to_mart_vendor.py           # Mart: mart_month_hour_vendor_trip_metrics
│   ├── fact_to_mart_vendor_cumulative.py # Mart: mart_month_vendor_cumulative_metrics
│   ├── fact_to_mart_zone_cumulative.py   # Mart: mart_month_zone_cumulative_metrics
│   └── nyc_taxi_etl.py          # (구) 단일 파일 ETL, main.py 파이프라인으로 대체됨
├── data/
│   ├── input/                  # 입력 데이터
│   ├── sample/                 # 샘플 원천 parquet
│   ├── reference/               # taxi_zone_lookup.csv (dim_taxi_zone 소스)
│   └── output/                  # 레이어별 출력 (raw/cleaned/warehouse/dim/mart)
├── docs/                        # 레이어/모델링/성능 설계 문서
└── scripts/
    └── submit.sh               # Job submit 스크립트
```

## 환경 구성

### 로컬 환경
- Java 17
- PySpark 3.5.3
- Docker & Docker Compose

### Docker Spark Cluster
- Spark Master + Worker (단일 컨테이너)
- Spark 3.5.3
- Python 3.9

## 사용 방법

### 1. Spark 클러스터 시작

```bash
cd docker
docker-compose up -d
```

### 2. 클러스터 상태 확인

```bash
docker-compose logs -f
```

### 3. Spark Job 제출

```bash
# 기본 실행 (jobs/main.py, 전체 레이어 파이프라인)
./scripts/submit.sh

# 커스텀 경로 지정
./scripts/submit.sh jobs/main.py data/sample/yellow_tripdata_2026-01.parquet data/output
./scripts/submit.sh jobs/main.py data/sample/yellow_tripdata_2026-01.parquet data/output data/reference/taxi_zone_lookup.csv

# (구) 단일 파일 ETL, main.py 도입 이전 버전
./scripts/submit.sh jobs/nyc_taxi_etl.py data/input/my_data.parquet data/output/result
```

`dim_taxi_zone`은 `data/reference/taxi_zone_lookup.csv` (공식 NYC TLC taxi zone lookup, 265개 zone)를 조회해서 생성됩니다. 4번째 인자를 생략하면 이 경로가 기본값으로 사용됩니다.

`main.py`는 다음 7단계를 순서대로 실행하는 파이프라인 오케스트레이터입니다:

1. **Raw Layer** (`save_raw_layer.py`) — 원천 데이터를 변형 없이 `year`/`month` 파티션으로 보존 (`data/output/raw/yellow_trip`)
2. **Cleaned Layer** (`raw_to_cleaned.py`) — 품질 필터링, trip_duration 계산, 컬럼 표준화 (`data/output/cleaned/yellow_trip`)
3. **Fact/Dimension Layer** (`cleaned_to_fact.py`) — `fact_taxi_trip`(`data/output/warehouse/fact_taxi_trip`), `dim_vendor`, `dim_taxi_zone` 생성 (`data/output/dim/`)
4. **Mart** (`fact_to_mart_zone.py`) — `mart_month_hour_zone_trip_metrics`
5. **Mart** (`fact_to_mart_vendor.py`) — `mart_month_hour_vendor_trip_metrics`
6. **Mart** (`fact_to_mart_vendor_cumulative.py`) — `mart_month_vendor_cumulative_metrics`
7. **Mart** (`fact_to_mart_zone_cumulative.py`) — `mart_month_zone_cumulative_metrics`

각 Mart는 `data/output/mart/<mart_name>/year=.../month=.../`에 저장됩니다.

> `dim_date`, `dim_time` 차원은 아직 구현되지 않았습니다 (`dim_vendor`, `dim_taxi_zone`만 존재).

### 4. Web UI 모니터링

- **Master UI**: http://localhost:8080 - 클러스터 상태 확인
- **Worker UI**: http://localhost:8081 - Worker 상태 확인
- **Application UI**: http://localhost:4040 - 실행 중인 Job 모니터링 (Job 실행 시에만 접근 가능)
- **History Server**: http://localhost:18080 - 완료된 Job 이력 확인

### 5. 클러스터 중지

```bash
cd docker
docker-compose down
```

## 데이터 파이프라인 (레이어 아키텍처)

```
Source (parquet/csv)
   └─▶ Raw Layer        (원본 보존, year/month 파티션)
        └─▶ Cleaned Layer   (품질 필터, 표준 컬럼, pickup_year/pickup_month 파티션)
             └─▶ Fact/Dimension Layer
                    ├─ fact_taxi_trip (trip 1건 단위)
                    ├─ dim_vendor
                    └─ dim_taxi_zone
                  └─▶ Mart Layer (BI 조회용 사전 집계)
                        ├─ mart_month_hour_zone_trip_metrics
                        ├─ mart_month_hour_vendor_trip_metrics
                        ├─ mart_month_vendor_cumulative_metrics
                        └─ mart_month_zone_cumulative_metrics
```

각 레이어는 독립된 job 파일이 담당하며, 하나의 job은 하나의 레이어 변환만 수행합니다. 자세한 설계는 `docs/data_layer_design.md`, `docs/data_modeling.md`, `docs/mart_design.md` 참고.

## 개발 워크플로우

1. 로컬에서 레이어별 job 코드 작성/수정 (`jobs/save_raw_layer.py`, `raw_to_cleaned.py`, `cleaned_to_fact.py`, `fact_to_mart_*.py`)
2. Docker Spark 클러스터로 `jobs/main.py` submit
3. Web UI에서 실행 상태 및 데이터 lineage 확인
4. 로그 확인: `docker-compose logs -f`
5. 결과 검증: `data/output/{raw,cleaned,warehouse,dim,mart}/` 디렉토리 확인

## 다음 단계

- [ ] `dim_date`, `dim_time` 차원 테이블 구현
- [ ] AWS EC2에 동일 환경 구성
- [ ] AWS EMR Serverless로 마이그레이션
