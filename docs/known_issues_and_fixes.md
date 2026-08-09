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
