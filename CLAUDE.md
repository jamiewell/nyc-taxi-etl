# CLAUDE.md

이 파일은 Claude Code (claude.ai/code)가 이 레포지토리에서 작업할 때 참고하는 가이드입니다.

<!-- ## 프로젝트 개요 -->

<!-- PySpark 3.5.3을 사용한 NYC Taxi 배치 ETL 파이프라인. Docker에서 실행되는 Spark standalone 클러스터로 NYC 택시 운행 데이터를 처리하며, 로컬 개발과 Docker 클러스터 제출 두 가지 실행 모드를 지원합니다. -->

## 프로젝트 목표
이 프로젝트는 단순 ETL이 아니라 NYC Taxi 데이터를 기반으로 Raw → Cleaned → Fact → Mart Layer를 구성하는 데이터 엔지니어링 프로젝트이다.

최종 목표:
1. 월별 신규 taxi trip 데이터를 수집한다. ✓
2. raw 데이터를 cleaned layer로 정제한다. ✓
3. fact_taxi_trip 테이블을 생성한다. ✓
4. dim_vendor, dim_taxi_zone, dim_date, dim_time 차원을 구성한다. (dim_vendor, dim_taxi_zone 완료 / dim_date, dim_time 미구현)
5. 분석 목적별 mart table을 생성한다. ✓ (4개 mart 모두 구현 완료, 아래 참고)
6. Spark UI 기반으로 groupBy, join, shuffle 성능을 측정하고 개선한다. (진행 중, `docs/spark_performance_analysis_guide.md` 참고)

## 아키텍처

**데이터 흐름**: Source parquet/CSV → **Raw Layer** → **Cleaned Layer** → **Fact/Dimension Layer** → **Mart Layer** (레이어별 parquet, year/month 파티셔닝)

`jobs/main.py`가 전체 파이프라인을 순서대로 오케스트레이션한다. 각 job 파일은 레이어 하나만 담당한다 (개발 원칙 참고).

**실행 환경**:
- **로컬 모드**: 호스트 머신에서 직접 PySpark 실행 (Java 17 + PySpark 3.5.3)
- **클러스터 모드**: Docker Spark standalone 클러스터 (Bitnami Spark 이미지)

**주요 구성요소**:
- `jobs/main.py`: 파이프라인 오케스트레이터. `spark-submit main.py <input_path> <base_output_path> [zone_lookup_path]`로 실행
- `jobs/save_raw_layer.py`: Source → Raw Layer (`read_source_data`, `save_to_raw_layer`). 변형 없이 `year`/`month` 파티션으로 원본 보존
- `jobs/raw_to_cleaned.py`: Raw → Cleaned Layer (`read_raw_data`, `transform_to_cleaned`, `write_cleaned_data`). 시간 피처 추출, trip_duration 계산, 품질 필터, 컬럼 표준화. `pickup_year`/`pickup_month` 파티션
- `jobs/cleaned_to_fact.py`: Cleaned → Fact/Dimension Layer (`build_and_write_dim_vendor`, `build_and_write_dim_taxi_zone`, `transform_to_fact`, `write_fact_table`). `dim_taxi_zone`은 `data/reference/taxi_zone_lookup.csv`(NYC TLC 공식, 265개 zone)에서 생성
- `jobs/fact_to_mart_zone.py`, `fact_to_mart_vendor.py`, `fact_to_mart_vendor_cumulative.py`, `fact_to_mart_zone_cumulative.py`: Fact/Dim → Mart Layer, mart 4종 각각 담당
- `jobs/nyc_taxi_etl.py`: (구) 단일 파일 ETL. `main.py` 파이프라인 도입 이전 버전으로, 현재는 사용되지 않지만 삭제되지 않고 남아 있음
- `scripts/submit.sh`: `docker exec`로 Docker Spark 클러스터에 job 제출. 기본 job은 `jobs/main.py`이며 4번째 인자로 zone_lookup_path 전달 가능
- `docker/docker-compose.yml`: Spark master + worker 서비스 정의
- `docker/Dockerfile`: 커스텀 Spark 3.5.3 이미지 (현재 미사용; 프로젝트는 bitnami/spark 사용)

**데이터 품질 필터** (`jobs/raw_to_cleaned.py`):
- null 체크: pickup/dropoff datetime, PULocationID, DOLocationID
- 시간 유효성: dropoff > pickup, trip_duration 0~1440분(24시간) 이내
- 비즈니스 로직: passenger_count 1~8, trip_distance 0~200마일, fare_amount > 0, total_amount > 0

## 자주 사용하는 명령어

### Docker Spark 클러스터

클러스터 시작:
```bash
cd docker && docker-compose up -d
```

클러스터 상태 확인:
```bash
docker-compose ps
docker-compose logs --tail 30  # 또는 -f로 실시간 로그 확인
```

클러스터 중지:
```bash
cd docker && docker-compose down
```

### ETL Job 실행

Docker 클러스터에 제출 (프로젝트 루트에서, 기본 job은 `jobs/main.py`):
```bash
./scripts/submit.sh
# 또는 커스텀 경로 지정 (input_path, output_base_path, [zone_lookup_path]):
./scripts/submit.sh jobs/main.py data/sample/yellow_tripdata_2026-01.parquet data/output
./scripts/submit.sh jobs/main.py data/sample/yellow_tripdata_2026-01.parquet data/output data/reference/taxi_zone_lookup.csv
```

로컬에서 실행 (호스트에 Java 17 + PySpark 3.5.3 필요):
```bash
python3 jobs/main.py data/sample/yellow_tripdata_2026-01.parquet data/output
```

### 모니터링

Job 실행 중 접속 가능한 UI:
- **Master UI**: http://localhost:8080 - 클러스터 토폴로지, 제출된 앱
- **Worker UI**: http://localhost:8081 - Worker 리소스
- **Application UI**: http://localhost:4040 - 실행 중인 job DAG, stages, SQL 쿼리
- **History Server**: http://localhost:18080 - 완료된 job 이력

### 데이터 관리

입력 데이터 위치: `data/input/` 또는 `data/sample/`, dim_taxi_zone 참조 데이터: `data/reference/taxi_zone_lookup.csv`

출력 데이터 위치 (레이어별, `data/output/` 하위):
- `raw/yellow_trip/year=.../month=.../` — Raw Layer
- `cleaned/yellow_trip/pickup_year=.../pickup_month=.../` — Cleaned Layer
- `warehouse/fact_taxi_trip/year=.../month=.../` — Fact Layer
- `dim/vendor/`, `dim/taxi_zone/` — Dimension Layer
- `mart/mart_month_hour_zone_trip_metrics/`, `mart/mart_month_hour_vendor_trip_metrics/`, `mart/mart_month_vendor_cumulative_metrics/`, `mart/mart_month_zone_cumulative_metrics/` — Mart Layer (각각 `year=.../month=.../` 파티션)

## 개발 노트

**볼륨 마운트**: Docker 컨테이너는 로컬 디렉토리를 마운트 (`docker/docker-compose.yml`):
- `jobs/` → `/opt/spark/jobs/`
- `data/` → `/opt/spark/data/`
- `scripts/` → `/opt/spark/scripts/`

`jobs/` 아래 변경 사항은 재빌드 없이 컨테이너에서 즉시 사용 가능합니다.

**클러스터 설정** (`docker/docker-compose.yml`):
- Master: 1개 인스턴스, 포트 7077
- Worker: 1개 인스턴스, 2G 메모리, 2 코어
- 보안 비활성화 (개발 모드): RPC 인증/암호화 없음

**Job 제출 기본값** (`scripts/submit.sh:21-26`):
- Deploy mode: client
- Driver memory: 1g
- Executor memory: 1g
- Executor cores: 1

**스키마 전제조건** (`jobs/raw_to_cleaned.py`):
파이프라인은 다음 컬럼을 포함한 입력 데이터를 기대합니다: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `VendorID`, `PULocationID`, `DOLocationID`, `RatecodeID`, `payment_type`, `store_and_fwd_flag`, `passenger_count`, `trip_distance`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `Airport_fee`, `cbd_congestion_fee`. 컬럼 누락 시 job이 실패합니다.

## 마이그레이션 계획

README.md에 명시된 AWS 마이그레이션 계획:
1. AWS EC2 배포 (동일한 Docker 설정)
2. AWS EMR Serverless (향후)


## 개발 원칙

- 모든 ETL job은 input_path와 output_path를 CLI argument로 받는다.
- 하나의 job은 하나의 데이터 레이어 변환만 담당한다.
- Raw 데이터는 수정하지 않는다.
- Cleaned/Fact/Mart 출력은 parquet로 저장한다.
- 출력 partition은 기본적으로 year/month를 사용한다.
- Spark 성능 튜닝은 baseline 측정 후 변경한다.

## 데이터 레이어 규칙

- Raw Layer: 원천 파일 보존
- Cleaned Layer: 품질 필터와 표준 컬럼 생성
- Fact Layer: trip 1건 단위의 fact_taxi_trip 생성
- Dimension Layer: vendor, zone, date, time 등 기준 정보 생성
- Mart Layer: BI 조회 목적의 사전 집계 테이블 생성

## Mart 설계 기준

최종 Mart는 다음 4개를 우선 구현한다.

1. mart_month_hour_zone_trip_metrics
2. mart_month_hour_vendor_trip_metrics
3. mart_month_vendor_cumulative_metrics
4. mart_month_zone_cumulative_metrics

Mart 생성 시 groupBy, dimension join, cumulative aggregation이 명확히 드러나야 한다.

## Claude Code 작업 규칙

- 큰 변경 전에는 먼저 변경 계획을 요약한다.
- 기존 실행 명령어와 Docker 경로를 깨지 않는다.
- 새 job을 만들 경우 scripts/submit.sh 또는 별도 실행 스크립트도 함께 갱신한다.
- 코드 변경 후 실행 방법을 README 또는 docs에 반영한다.
- 임의로 라이브러리를 추가하지 말고 필요한 경우 먼저 이유를 설명한다.
- Spark job은 로컬 모드와 Docker 클러스터 모드 양쪽에서 실행 가능해야 한다.

## 참고 설계 문서

- `docs/data_layer_design.md`: Raw/Cleaned/Fact/Mart 레이어 설계
- `docs/data_modeling.md`: Fact/Dimension 테이블 설계
- `docs/mart_design.md`: 목적별 Mart 테이블 설계
- `docs/spark_tuning_plan.md`: Spark 성능 실험 계획
<!-- - `docs/pipeline_roadmap.md`: 단계별 구현 로드맵 -->