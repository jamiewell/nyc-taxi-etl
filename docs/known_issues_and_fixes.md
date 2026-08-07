# 개발 중 확인된 이슈와 수정 사항

개발 과정에서 발견된 버그, 설정 오류, 설계상 위험을 정리한 기록. 발생 배경과 조치를 함께 남겨 재발 방지 및 후속 작업 시 참고용으로 사용한다.

## 1. PySpark 3.5.3은 Java 21을 지원하지 않음

**증상**: AWS EC2 클러스터를 Java 21 + PySpark 3.5.3으로 구성하려 함.

**원인**: Apache Spark 3.5.x 공식 지원 Java 버전은 8/11/17뿐이며, Java 21 지원은 Spark 4.x부터 추가됨. Java 21로 진행 시 클러스터 기동 또는 실행 중 런타임 오류 위험.

**조치**: 설치 전 공식 문서로 검증 후 **Java 17**로 결정. `CLAUDE.md`에도 Java 17 + PySpark 3.5.3 조합이 명시되어 있어 그대로 일치시킴.

**관련 문서**: `docs/aws_infra_setup.md`

## 2. pip로 설치한 PySpark는 standalone 클러스터 기동 스크립트가 없음

**증상**: EC2 노드에 `pip install pyspark==3.5.3` 후 `$SPARK_HOME/sbin/start-master.sh` 실행 시 `No such file or directory`.

**원인**: pip 배포판의 `sbin/`에는 `start-history-server.sh`류만 포함되어 있고, `start-master.sh`/`start-worker.sh`/`start-all.sh`는 빠져 있음 (공식 바이너리 tarball 배포판에만 존재). pip 배포판은 client/local 모드 위주로 패키징되어 있음.

**조치**: `sbin/spark-daemon.sh`를 직접 호출해 Master/Worker 데몬을 기동하는 방식으로 우회.
```bash
spark-daemon.sh start org.apache.spark.deploy.master.Master 1 --host <ip> --port 7077 --webui-port 8080
spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 spark://<master-ip>:7077 --webui-port 8081
```
`spark-daemon.sh`는 내부적으로 `start-master.sh`가 호출하는 것과 동일한 로직이라 로그/PID 관리는 동일하게 동작함.

**남은 제약**: 이 방식은 systemd 서비스가 아니라 단순 프로세스라 EC2 재부팅 시 자동 재기동되지 않음 (아직 미해결, `docs/aws_infra_setup.md`의 "남은 작업" 참고).

## 3. CLAUDE.md/README가 실제 코드 구조와 불일치

**증상**: 문서에는 `jobs/nyc_taxi_etl.py` 단일 파일 ETL만 설명되어 있었지만, 실제 코드베이스는 이미 `jobs/main.py`가 오케스트레이션하는 Raw → Cleaned → Fact/Dim → Mart 레이어드 파이프라인(`save_raw_layer.py`, `raw_to_cleaned.py`, `cleaned_to_fact.py`, `fact_to_mart_*.py`)으로 전환되어 있었음.

**원인**: 파이프라인 구조를 리팩터링하면서 문서 갱신이 누락됨.

**조치**: `CLAUDE.md`, `README.md`를 실제 구조에 맞게 재작성 (레이어별 job 파일 설명, 데이터 흐름 다이어그램, dim_date/dim_time 미구현 명시 등).

**부수 발견**: `CLAUDE.md`에 Docker 볼륨 마운트 경로가 `/opt/bitnami/spark/...`로 잘못 적혀 있었음. `docker/docker-compose.yml` 확인 결과 실제 마운트 경로는 `/opt/spark/...`. 문서 갱신 시 함께 수정.

## 4. 택시 타입(yellow/green/fhvhv/fhv)별 스키마가 근본적으로 다름

**증상**: `raw_to_cleaned.py`가 yellow 스키마(`VendorID`, `fare_amount`, `passenger_count`, `trip_distance` 등)를 고정 전제로 품질 필터와 컬럼 셀렉트를 하드코딩하고 있었음. green/fhvhv/fhv를 같은 로직에 통과시키면 실패.

**원인 (실제 S3 데이터로 스키마 직접 확인)**:
- yellow/green: fare/승객수 컬럼 존재, pickup/dropoff 컬럼명만 다름 (`tpep_*` vs `lpep_*`)
- fhvhv: `VendorID` 없음, 요금이 `base_passenger_fare`/`tolls`/`bcf`/`sales_tax`/`tips`/`driver_pay`로 분해되어 있음
- fhv: `passenger_count`/`trip_distance`/`fare_amount` 자체가 아예 없음 (`dispatching_base_num`, pickup/dropoff, location ID, `SR_Flag`뿐). 위치 컬럼명도 다른 타입과 다름 (`PUlocationID`/`DOlocationID`, 소문자 l). dropoff 컬럼명도 `dropOff_datetime`(대문자 O)으로 표기가 다름.

**조치**: `transform_to_cleaned(df, taxi_type)`가 타입별 private 함수(`_transform_yellow/green/fhvhv/fhv`)로 디스패치하도록 재작성. 각 타입은 자신에게 의미 있는 품질 필터만 적용(예: fhv는 시간/위치 검증만, 승객수/거리/요금 필터 없음)하고 별도 cleaned 경로(`cleaned/<taxi_type>_trip`)에 저장. Fact/Mart는 당장 yellow 스키마만 지원하도록 범위를 한정하고, 그 외 타입 연결은 후속 작업으로 명시.

**검증**: 4개 타입 실제 S3 데이터(fhvhv 535MB/2200만 건 포함)로 raw→cleaned 개별 실행 및 멀티 타입 조합(`--taxi-types yellow,green`) 후 Fact/Mart validation까지 통과 확인.

## 5. 파티션 정적 overwrite로 인한 데이터 유실 위험 (연월 범위 처리 도입 시 발견)

**증상**: `--start-year-month`/`--end-year-month`로 특정 월만 좁혀서 재실행하면, 이전에 이미 처리해둔 다른 달의 raw/cleaned 파티션이 통째로 사라지는 것을 재현 테스트 중 발견.

**원인**: `save_raw_layer.py`/`raw_to_cleaned.py`의 write가 `.mode("overwrite").partitionBy(...)`였는데, Spark의 기본 overwrite 동작(static overwrite)은 새로 쓰는 파티션뿐 아니라 **출력 경로 전체**를 지우고 다시 씀. 예전에는 매 실행마다 항상 전체 데이터를 다시 읽고 썼기 때문에 문제가 드러나지 않았지만, 특정 월만 선택적으로 처리하는 기능을 추가하면서 실제로 위험한 동작이 됨.

**조치**: `SparkSession` 생성 시 `spark.sql.sources.partitionOverwriteMode`를 기본값 `dynamic`으로 설정. 이후 `.mode("overwrite")`는 실제로 쓰여진 `year=`/`month=` 파티션만 교체하고 나머지는 보존.

**검증**: 로컬에서 2025-11만 처리 → 2026-01만 처리 순서로 두 번 실행 후, 두 파티션이 raw/cleaned 레이어 모두에 그대로 남아있는 것을 직접 확인.

**후속 변경 (CLI에서 선택 가능하도록 확장)**: 의도적으로 출력 경로 전체를 재생성해야 하는 경우(예: 스키마 변경 후 풀 리빌드)를 위해 `--overwrite-mode static|dynamic` 플래그를 `main.py`에 추가. 기본값은 안전한 `dynamic`이며, `static`을 선택하고 동시에 `--start-year-month`/`--end-year-month`로 범위를 좁힌 경우 실행 시점에 경고 메시지를 출력한다. `--overwrite-mode static`으로 2025-11 처리 후 2026-01만 재처리하면 실제로 2025-11 파티션이 삭제되는 것을 재현 테스트로 재확인함 (`scripts/submit.sh`도 8번째 인자/`OVERWRITE_MODE` 환경변수로 동일하게 전달 가능).

## 6. 로컬 테스트 데이터의 파티션 명명 규칙 불일치 (테스트 아티팩트, 실제 버그 아님)

**증상**: 연월 범위 기능을 로컬에서 검증할 때, Spark `partitionBy`로 직접 만든 테스트 데이터는 `month=1`처럼 zero-padding 없이 써지는데, `collection_range.py`가 조회 후 재구성하는 실제 읽기 경로는 `month=01`(zero-padded)이라 파일을 못 찾는 오류가 발생.

**원인 확인**: 실제 `nyc-taxi-collector`가 Lambda에서 S3에 쓰는 키 규칙(`s3_key = f"{BASE_PREFIX}/{taxi_type}/year={year}/month={month:02d}/..."`)은 zero-padded이고, `aws s3 ls`로 실제 버킷을 확인해도 `month=01` ~ `month=12` 형태임을 재확인. 즉 코드 자체는 실제 운영 데이터 규칙과 정확히 일치했고, 로컬 재현 테스트에서 Spark의 `partitionBy` 자동 파티셔닝(zero-padding 없음)을 그대로 써서 생긴 테스트 픽스처 불일치였음.

**조치**: 테스트 데이터 생성 스크립트를 `partitionBy` 대신 zero-padded 경로(`year={y}/month={m:02d}`)에 명시적으로 쓰도록 수정해 재현 테스트 통과 확인. 프로덕션 코드는 변경 없음.
