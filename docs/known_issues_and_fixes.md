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

## 7. GitHub Actions ↔ EC2 배포 연동 시 확인된 이슈

`main` 브랜치를 EC2 Spark 마스터 노드에 배포하는 GitHub Actions 워크플로우(`.github/workflows/deploy.yml`, 상세는 `docs/deployment.md`)를 구성하며 확인된 사항들.

**이슈 1: root 계정 자격증명을 GitHub Secrets에 넣는 것은 위험**

`aws sts get-caller-identity`로 확인해보니 로컬에서 쓰던 `cotedazure12` 프로파일이 IAM 사용자가 아니라 **root 계정** 자격증명이었음. GitHub Secrets에 root 키를 등록하면 유출 시 계정 전체가 뚫림.

**조치**: 배포 전용 IAM 사용자 `github-actions-nyc-taxi-deploy`를 새로 생성하고, 필요한 작업(마스터 인스턴스 조회/기동, 배포용 보안그룹 규칙 추가/삭제)만 허용하는 최소 권한 정책을 부여. `ec2:StartInstances`도 마스터 인스턴스 ARN으로 한정해, 다른 인스턴스는 이 자격증명으로 건드릴 수 없게 함.

**이슈 2: IP 제한 보안그룹이 GitHub-hosted runner와 근본적으로 충돌**

기존 보안그룹은 SSH(22)를 관리자 개인 IP `/32`에만 허용하도록 되어 있었음(`docs/aws_infra_setup.md`). GitHub Actions의 hosted runner는 매 실행마다 IP가 바뀌기 때문에, 이 구조로는 애초에 워크플로우가 마스터에 SSH 접속을 못 함.

**검토한 대안과 선택 이유**:
- 0.0.0.0/0 상시 개방 → 기각 (공격 표면 확대)
- GitHub 공개 IP 대역(`api.github.com/meta`) 상시 허용 → 기각 (범위가 넓고 유지보수 필요)
- **채택**: 워크플로우 실행 시점에 그 runner의 현재 IP만 SG에 임시로 authorize하고, 배포가 끝나면(성공/실패 무관하게 `if: always()`) 즉시 revoke. 평소 SG는 기존과 동일하게 좁은 상태를 유지.

**이슈 3: 마스터 노드 Public IP가 stop/start마다 바뀜 (기존에 알려진 제약, 배포 워크플로우에도 동일 적용)**

Elastic IP를 쓰지 않기로 한 기존 결정(`docs/aws_infra_setup.md`)에 따라, 배포 워크플로우도 매 실행마다 `describe-instances`로 현재 Public IP를 동적 조회해서 SSH 대상으로 사용하도록 구성. 하드코딩된 IP를 GitHub Secrets 등에 저장하지 않음.

**작업 순서상 특이사항**: 워크플로우 코드를 작성하기 전에, 마스터 인스턴스를 실제로 잠깐 기동해서 `rsync` 배포 + `python3 jobs/main.py --help` 검증까지 수동으로 먼저 실행해 각 단계(SSH 접속, rsync, venv 활성화, 배포된 코드 import)가 실제로 동작하는지 확인한 뒤, 그 명령어들을 그대로 워크플로우 YAML에 옮겼음. 이 과정에서 로컬 관리자 IP가 이전 등록값과 달라져 있어 보안그룹 규칙을 먼저 갱신해야 했음 (동적 IP 환경에서 인프라 작업 시 흔히 겪는 문제).

**이슈 4: IAM 최소 권한 정책에서 `DescribeInstanceStatus` 누락 (실제 워크플로우 첫 실행에서 발견)**

수동 검증(root 계정 자격증명 사용) 때는 문제없었지만, 실제로 GitHub Actions에서 배포 전용 IAM 사용자로 첫 실행했을 때 `Start master instance if stopped` 단계가 22초 만에 실패. `ec2:StartInstances`는 정상적으로 실행되어 인스턴스가 `stopped`→`pending`으로 전환됐지만, 바로 다음의 `aws ec2 wait instance-status-ok`가 다음 에러로 실패:

```
aws: [ERROR]: Waiter InstanceStatusOk failed: An error occurred (UnauthorizedOperation):
You are not authorized to perform this operation. User: .../github-actions-nyc-taxi-deploy
is not authorized to perform: ec2:DescribeInstanceStatus because no identity-based policy
allows the ec2:DescribeInstanceStatus action
```

**원인**: `aws ec2 wait instance-status-ok`(및 `describe-instance-status`)는 `ec2:DescribeInstances`가 아니라 별개의 IAM 액션인 `ec2:DescribeInstanceStatus`를 필요로 함. 최소 권한 정책을 작성할 때 이걸 빠뜨림 — 두 API가 이름이 비슷해서 하나만 넣으면 될 거라고 착각하기 쉬운 부분.

**조치**: IAM 인라인 정책의 `DescribeInstances` statement에 `ec2:DescribeInstanceStatus`를 추가. `docs/deployment.md`의 정책 스니펫도 함께 갱신.

**교훈**: root 계정으로 사전에 수동 검증했더라도, root는 모든 권한을 갖고 있어 "최소 권한 IAM 정책이 실제로 충분한지"는 검증되지 않는다. 스코프를 좁힌 자격증명은 반드시 그 자격증명 자체로 최소 한 번 실제 실행까지 확인해야 한다.

## 8. 첫 실전 데이터(2009-01/2011-01) 처리 중 확인된 이슈들

`nyc-taxi-collector-raw` S3 실데이터로 EC2 클러스터에서 처음 `spark-submit`을 돌리며 확인된 것들. 인프라는 다 준비됐어도 실제 데이터로 한 번은 꼭 돌려봐야 드러나는 종류의 문제들이었다.

**이슈 1: S3 접근용 hadoop-aws 커넥터 부재**

pip로 설치한 PySpark에는 S3A 커넥터(`hadoop-aws`, `aws-java-sdk-bundle`)가 기본 포함되어 있지 않음. `spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4`로 런타임에 받아와야 하며, 버전은 번들 Hadoop client(`hadoop-client-*-3.3.4.jar`)와 맞춰야 한다.

**이슈 2: S3 인증 - 정적 키 대신 EC2 Instance Profile**

Spark executor가 워커 노드에서 직접 S3를 읽고 쓰므로(driver뿐 아니라 실행 노드 전체에 인증이 필요), 정적 access key를 노드에 두지 않기 위해 IAM Role(`nyc-taxi-spark-ec2-s3`)을 인스턴스 프로필로 4대 전부에 연결. `nyc-taxi-collector-raw`는 읽기 전용, `nyc-taxi-batch-output`은 읽기/쓰기로 권한을 한정. 프로필이 붙어있으면 Hadoop의 기본 Credential Provider Chain이 EC2 메타데이터에서 자동으로 인증정보를 가져오므로 별도 설정 불필요.

**이슈 3: 택시 데이터 스키마가 연도별로 최소 3세대 존재**

2009-01로 첫 테스트했다가 실패. 원인은 코드가 아니라 실제 데이터: NYC TLC yellow 택시 데이터는 연도에 따라 완전히 다른 스키마를 쓴다.

| 시기 | VendorID | 위치 표현 |
|---|---|---|
| 2009 | `vendor_name` (문자열) | 없음, GPS 좌표만 |
| 2010 초반 | `vendor_id` (소문자) | GPS 좌표 (`pickup_longitude` 등) |
| 2011~ | `VendorID` | `PULocationID`/`DOLocationID` (현재 코드가 기대하는 형태) |

**조치**: 지금 코드는 2011년 이후 데이터만 지원. 2009~2010초 지원은 별도 작업(좌표→zone 역매핑 등)으로 범위 밖에 둠. 2011-01로 전환해 테스트 계속 진행.

**이슈 4: zone lookup CSV를 로컬 경로로 넘기면 워커에서 못 읽음**

`spark.read.csv(zone_lookup_path)`는 분산 읽기라 executor(워커 노드)가 그 경로를 직접 열려고 시도한다. 코드는 마스터에만 배포되어 있고(`docs/deployment.md`) 워커엔 없으므로, `data/reference/taxi_zone_lookup.csv`처럼 로컬 경로를 그대로 넘기면 실패한다.

**조치**: `aws s3 cp data/reference/taxi_zone_lookup.csv s3://nyc-taxi-batch-output/reference/`로 S3에 올리고 `s3a://` 경로로 넘기도록 변경. "마스터에만 배포하면 된다"는 원칙(`docs/deployment.md`)은 애플리케이션 코드에만 해당하고, 드라이버가 executor에게 읽으라고 지시하는 데이터 파일에는 해당하지 않는다는 걸 실제로 겪고 나서 알게 됨.

**이슈 5: History Server가 `/tmp/spark-events` 없이 기동하면 조용히 죽음**

`start-history-server.sh`를 실행하면 포트 바인딩 로그(`Bound HistoryServer to 0.0.0.0...`)까지는 정상 출력되지만, 그 직후 `spark.history.fs.logDirectory`(기본값 `/tmp/spark-events`) 디렉터리가 없으면 `FileNotFoundException`으로 프로세스가 죽는다. `jps`에 `HistoryServer`가 안 보이는데 로그엔 "시작됨"이라고 나와 있어서 헷갈리기 쉬움.

**조치**: `mkdir -p /tmp/spark-events`를 먼저 실행한 뒤 History Server를 기동. 또한 `spark-submit`에 `--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events`가 없으면 History Server를 제대로 띄워도 이벤트 자체가 기록되지 않는다 (과거 실행 이력은 소급 복원 안 됨, 이후 실행부터 표준 명령에 포함하도록 `docs/aws_infra_setup.md`에 반영).

## 9. 연월을 나눠서 누적 처리할 때만 드러나는 구조적 버그 2건 (2011-11/2011-12 처리 중 발견)

2011-01 하나만 처리했을 땐 안 보이다가, 두 번째/세 번째 달을 누적으로 추가하자마자 바로 재현된 문제들. "한 달씩 나눠서 여러 번 돌린다"는 이 프로젝트의 실제 사용 패턴에서만 드러나는 종류라 별도로 기록한다.

**버그 1: Cleaned Layer가 이번 실행과 무관한 과거 raw 데이터까지 매번 통째로 다시 읽음**

`raw_to_cleaned.read_raw_data(spark, raw_layer_path)`가 `--start-year-month`/`--end-year-month`로 범위를 좁혀도 raw layer 디렉터리 전체(`s3a://.../raw/yellow_trip`)를 읽었다. 2011-11/2011-12를 raw로 쓴 뒤 Cleaned 단계가 이 전체 경로를 읽으면서, 이미 있던 2011-01 raw 파티션과 스키마를 병합하려다 `congestion_surcharge` 컬럼의 물리 타입 불일치(double vs INT32)로 크래시.

**조치**: `collection_range.py`에 `resolve_year_months()`(연월 매칭 로직만 분리)와 `raw_layer_month_paths()`(Spark `partitionBy`가 쓰는 zero-padding 없는 파티션 경로 생성)를 추가. `main.py`가 Cleaned Layer 읽기를 이번 실행 대상 월의 raw 파티션 경로 목록으로만 한정하도록 수정 (`spark.read.parquet(*paths)`). 부수 효과로 매번 전체 재처리하던 것도 없어져 성능도 개선됨.

**버그 2: 요금 컬럼이 소스 원본의 물리 타입(int32/double)을 그대로 물려받음**

버그 1을 고치고 다시 돌리니, 이번엔 한 단계 아래(`cleaned_to_fact.read_cleaned_data`가 Cleaned Layer 전체를 읽는 지점)에서 똑같은 `congestion_surcharge` 타입 충돌이 재발. Fact 테이블은 설계상 누적 이력 전체를 읽어야 하므로 버그 1과 같은 "읽기 범위 좁히기"로는 해결 불가 — 근본 원인을 고쳐야 했음.

원인: `raw_to_cleaned.py`가 `spark_round(col("congestion_surcharge"), 2)`처럼 컬럼을 그대로 반올림만 했는데, `round()`는 입력 타입을 유지할 뿐 강제로 바꾸지 않는다. TLC 원본 parquet이 그 달의 값이 전부 0 같은 정수였다는 이유로 `congestion_surcharge`를 INT32로 인코딩한 달이 있었고, 그 타입이 그대로 우리 Cleaned Layer 출력까지 전파됨.

**조치**: `_round_double(column_name)` 헬퍼를 추가해 `col(name).cast(DoubleType())`을 먼저 적용한 뒤 반올림하도록 변경. yellow/green/fhvhv의 모든 요금·거리 컬럼에 일괄 적용해, 소스 월별 인코딩과 무관하게 Cleaned Layer 출력 타입을 항상 고정.

**검증**: 로컬에서 한 달은 INT 타입, 다른 한 달은 DOUBLE 타입으로 `congestion_surcharge`를 강제로 다르게 인코딩한 synthetic 데이터를 만들어 각각 별도의 `spark-submit` 실행(실제 운영과 동일하게 서로 다른 실행에서 파티션이 쓰이는 상황 재현)으로 Cleaned Layer에 두 파티션을 쓴 뒤, Fact 단계가 둘을 합쳐 읽어도 실패하지 않는 것을 확인. 이후 실제 클러스터에서 2011-01(기존)+2011-11+2011-12(신규) 세 달을 합쳐 Fact/Mart까지 끝까지 성공하는 것으로 재확인.

## 10. History Server 이벤트 로그가 EC2 stop/start(재부팅)마다 사라짐

**증상**: 비용 절감을 위해 EC2를 stop했다가 다시 start한 뒤 History Server를 재기동했더니, 이전에 분명히 쌓여있던(`api/v1/applications`로 직접 확인했던) job 이력이 전부 사라지고 빈 목록으로 시작함.

**원인**: `spark.eventLog.dir`/`spark.history.fs.logDirectory`를 `/tmp/spark-events`로 설정해뒀는데, `/tmp`는 별도 tmpfs 마운트가 아니라 루트 EBS 볼륨(`/dev/xvda1`)의 일반 디렉터리임(`mount`로 확인). 다만 Ubuntu는 `systemd-tmpfiles-setup.service`가 **매 부팅마다** `/tmp` 내용을 청소하도록 기본 설정되어 있어서, EC2 stop→start(=재부팅)를 거치면 디스크 자체는 멀쩡해도 `/tmp` 안의 내용만 사라짐. `~/nyc-taxi-batch`처럼 홈 디렉터리에 있던 배포 코드나 `~/spark-submit-*.log` 같은 실행 로그는 이 청소 대상이 아니라서 그대로 남아있었던 것과 대조됨.

**조치**: `spark.eventLog.dir`와 `spark.history.fs.logDirectory`를 `/tmp/spark-events`에서 `/home/ubuntu/spark-events`(홈 디렉터리)로 변경. `docs/aws_infra_setup.md`의 표준 명령/History Server 기동 절차에 반영.

**참고**: 이 방식은 EC2 **stop/start**에는 살아남지만, 인스턴스를 **terminate하고 새로 만들면** 당연히 EBS 볼륨 자체가 사라지므로 함께 사라진다. 인스턴스 교체까지 견디는 이력이 필요하면 `spark.eventLog.dir`를 `s3a://nyc-taxi-batch-output/spark-events` 같은 S3 경로로 돌리는 게 더 근본적인 해결책이지만, History Server는 `spark-submit`과 달리 `--packages`로 기동하지 않아서 `hadoop-aws` jar를 별도로 클래스패스에 올려야 하는 추가 작업이 필요함 (아직 미적용, 필요해지면 진행).

## 11. 여러 달을 한 번에 합쳐 읽으면 물리 타입이 다른 소스 파일 자체가 못 읽힘 (2011-01~04 4개월 처리 중 발견)

**배경**: `#9`에서 우리가 만든 Cleaned Layer 출력의 `congestion_surcharge` 타입 불일치는 고쳤다. 이번엔 그와 별개로, **TLC가 애초에 배포한 원본 소스 parquet 파일 자체**가 달마다 물리 타입이 다르다는 게 드러났다 (`2011-02` 원본은 `congestion_surcharge`가 INT32, 다른 달은 double). `--start-year-month 2011-01 --end-year-month 2011-04`로 한 번에 4개월을 처리하려다 STEP 1(Raw Layer, `save_raw_layer.read_source_data`가 4개월치 소스 경로를 `spark.read.parquet(*paths)`로 한 번에 읽는 지점)에서 바로 실패했다.

```
org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException:
column: [congestion_surcharge], physicalType: INT32, logicalType: double
```

**시도 1 (실패): `spark.sql.parquet.enableVectorizedReader=false`**

Parquet 물리 타입 불일치는 보통 "벡터화 리더가 타입 승격(int→double)을 못 해서 나는 문제"로 알려져 있고, 비벡터화(레거시 row 기반) 리더로 전환하면 해결된다는 게 일반적인 통설이다. 실제로 로컬에서 재현 테스트를 해보니 **이 Spark 버전(3.5.3)에서는 비벡터화 리더도 실패**했다 — 에러 종류만 바뀐다:

```
java.lang.ClassCastException: class ...MutableDouble cannot be cast to class ...MutableInt
```

즉 벡터화 여부와 무관하게, **물리 타입이 다른 여러 parquet 파일을 하나의 read 호출로 병합하는 것 자체를 Spark가 지원하지 않는다.** 설정 하나로 우회할 수 있는 문제가 아니었다.

**시도 2 (성공): 파일을 합쳐서 읽지 않고, 달마다 개별로 읽어서 처리한 뒤 합치기**

근본적인 해결책은 "여러 달을 한 번의 read 호출에 몰아넣지 않는 것"이다. 각 달을 **개별로** 읽으면 애초에 스키마를 병합할 필요 자체가 없어진다 (병합 대상이 파일 1개뿐이므로).

`main.py`의 `run_raw_and_cleaned_layer`를 range 모드일 때 다음과 같이 재구성:
- **STEP 1 (Raw Layer)**: `year_months` 리스트를 순회하며 월별로 `read_source_data` → `save_to_raw_layer`를 개별 호출. Raw Layer는 원본을 그대로 보존해야 하므로(캐스팅 금지) 이 방식이 유일한 해법 — 어차피 한 파일씩만 다루니 원본 물리 타입도 그대로 보존됨.
- **STEP 2 (Cleaned Layer)**: 마찬가지로 월별 raw 파티션 경로를 순회하며 개별로 `read_raw_data` → `transform_to_cleaned` 호출 (이 단계에서 `#9`의 `_round_double()`이 각 월 DataFrame을 개별적으로 `DoubleType`으로 캐스팅), 그 결과 DataFrame들을 **캐스팅이 끝난 뒤에** `unionByName()`으로 합쳐서 씀. 캐스팅을 먼저 하고 합치는 순서가 핵심 — 합친 뒤에 캐스팅하려 하면 union 자체가 이미 스키마 병합을 요구해서 다시 같은 문제가 재발한다.

**검증**: 로컬에서 4개월치 synthetic 소스 데이터를 `double/INT32/double/INT32` 순서로 강제로 다르게 인코딩해서 `--start-year-month 2011-01 --end-year-month 2011-04` 한 번의 `spark-submit` 실행으로 raw→cleaned→fact→mart까지 전부 성공하는 것을 확인 후 클러스터에 배포.

**교훈**: "타입 불일치" 에러가 나면 반사적으로 벡터화 리더 설정부터 의심하게 되는데, 이번 케이스처럼 설정으로 우회가 안 되는 경우가 있다. 여러 파일을 한 번에 병합해서 읽어야 하는 구조 자체가 문제라면, 설정을 바꾸기보다 **애초에 병합이 필요 없도록 개별 처리 후 union**하는 쪽이 더 견고하다.
