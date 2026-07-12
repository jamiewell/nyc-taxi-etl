# NYC Taxi Batch ETL with PySpark

NYC Taxi 데이터를 처리하는 PySpark ETL 프로젝트입니다.

## 구조

```
nyc-taxi-batch/
├── docker/
│   ├── Dockerfile              # Spark 3.5.3 이미지
│   ├── docker-compose.yml      # Spark standalone cluster 설정
│   └── start-spark.sh          # Spark 시작 스크립트
├── jobs/
│   └── nyc_taxi_etl.py        # PySpark ETL 작업
├── data/
│   ├── input/                  # 입력 데이터
│   └── output/                 # 출력 데이터
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
# 기본 실행
./scripts/submit.sh

# 커스텀 경로 지정
./scripts/submit.sh jobs/nyc_taxi_etl.py data/input/my_data.parquet data/output/result

# main.py 파이프라인 (raw→cleaned→fact/dim→mart), zone lookup 경로 지정 가능
./scripts/submit.sh jobs/main.py data/sample/yellow_tripdata_2026-01.parquet data/output
./scripts/submit.sh jobs/main.py data/sample/yellow_tripdata_2026-01.parquet data/output data/reference/taxi_zone_lookup.csv
```

`dim_taxi_zone`은 `data/reference/taxi_zone_lookup.csv` (공식 NYC TLC taxi zone lookup, 265개 zone)를 조회해서 생성됩니다. 4번째 인자를 생략하면 이 경로가 기본값으로 사용됩니다.

`main.py`는 Raw → Cleaned → Fact/Dimension → Mart(`mart_month_hour_zone_trip_metrics`, `mart_month_hour_vendor_trip_metrics`) 전 레이어를 한 번에 실행하며, 각 Mart 단계는 `data/output/mart/<mart_name>/year=.../month=.../`에 저장되고 fact 대비 row count/금액 검증까지 자동 수행합니다.

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

## 데이터 파이프라인

1. **Read**: Parquet/CSV 형식의 NYC Taxi 데이터 읽기
2. **Transform**:
   - 날짜/시간 파싱 (year, month, day, hour, dayofweek)
   - 데이터 정제 (invalid records 제거)
   - 집계 및 통계 계산
3. **Write**: Parquet 형식으로 year/month 기준 파티셔닝하여 저장

## 개발 워크플로우

1. 로컬에서 코드 작성/수정 (`jobs/nyc_taxi_etl.py`)
2. Docker Spark 클러스터로 job submit
3. Web UI에서 실행 상태 및 데이터 lineage 확인
4. 로그 확인: `docker-compose logs -f`
5. 결과 검증: `data/output/` 디렉토리 확인

## 다음 단계

- [ ] 샘플 데이터 다운로드
- [ ] AWS EC2에 동일 환경 구성
- [ ] AWS EMR Serverless로 마이그레이션
