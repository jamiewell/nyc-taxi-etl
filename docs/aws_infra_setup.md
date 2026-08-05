# AWS 인프라 구성 (PySpark 클러스터)

NYC Taxi 배치 파이프라인을 AWS EC2로 마이그레이션하기 위해 구성한 4노드 PySpark 클러스터 인프라 기록.

## 계정 / 리전

| 항목 | 값 |
|---|---|
| AWS Profile | `cotedazure12` |
| Account ID | `962088872927` |
| Region | `ap-northeast-2` (서울) |
| Availability Zone | `ap-northeast-2a` (4대 전부 동일 AZ, AZ 간 전송 비용 방지) |

## 네트워크

기존에 존재하던 **default VPC**를 그대로 재사용했다 (신규 VPC 생성 없음).

| 리소스 | ID | 비고 |
|---|---|---|
| VPC | `vpc-0c103ab7d5dd3f610` | CIDR `172.31.0.0/16`, default VPC |
| Subnet | `subnet-035440b158ea73356` | `ap-northeast-2a`, public subnet (auto-assign public IP 활성) |
| Internet Gateway | default VPC에 기본 연결됨 | 별도 생성 안 함 |
| Route Table | default 라우팅 테이블 사용 | 별도 생성 안 함 |

## Security Group

- 이름: `nyc-taxi-spark-sg`
- ID: `sg-0cffb9429839840cd`
- VPC: `vpc-0c103ab7d5dd3f610`

| 방향 | 포트 | 소스 | 용도 |
|---|---|---|---|
| Inbound | 22 | 관리자 IP `/32` | SSH |
| Inbound | 8080 | 관리자 IP `/32` | Spark Master UI |
| Inbound | 8081 | 관리자 IP `/32` | Spark Worker UI |
| Inbound | 4040 | 관리자 IP `/32` | Spark Application UI |
| Inbound | 18080 | 관리자 IP `/32` | Spark History Server |
| Inbound | all | self (`sg-0cffb9429839840cd`) | 클러스터 노드 간 내부 통신 (Spark RPC 등) |

> 관리자 IP는 생성 시점 기준 `118.235.74.148/32`로 등록. IP가 바뀌면 SG 규칙 갱신 필요.

## SSH Key Pair

- Key pair 이름: `nyc-taxi-cluster`
- 생성 방식: 로컬에서 `ssh-keygen -t ed25519`로 키쌍 생성 후 **공개키만 AWS에 import** (`import-key-pair`)
- 개인키 위치: `~/.ssh/nyc-taxi-cluster` (로컬 전용, AWS에는 전송되지 않음)

## EC2 인스턴스

| 역할 | Instance ID | Type | Private IP | Public IP |
|---|---|---|---|---|
| Master | `i-0131fffaf58ee5509` | t2.small | 172.31.3.142 | 3.34.200.25 |
| Worker 1 | `i-079d25447827c38fa` | t2.small | 172.31.13.176 | 54.180.243.232 |
| Worker 2 | `i-0b5d4a1f0d77eb2c7` | t2.small | 172.31.11.1 | 3.36.109.56 |
| Worker 3 | `i-0e8f2fa3bc114aabc` | t2.small | 172.31.10.110 | 52.78.33.49 |

**OS**: Ubuntu 22.04 LTS (AMI `ami-012a353bb3afb92ee`)

> 원래 RHEL로 검토했으나 EC2에서 RHEL은 시간당 OS 라이선스 비용이 별도로 부과되어, t2.small처럼 저사양 인스턴스에서는 비용 부담이 커 Ubuntu로 결정.

## 접속 방법

```bash
ssh -i ~/.ssh/nyc-taxi-cluster ubuntu@3.34.200.25   # master
ssh -i ~/.ssh/nyc-taxi-cluster ubuntu@54.180.243.232 # worker 1
ssh -i ~/.ssh/nyc-taxi-cluster ubuntu@3.36.109.56    # worker 2
ssh -i ~/.ssh/nyc-taxi-cluster ubuntu@52.78.33.49    # worker 3
```

## 비용 참고

- t2.small 온디맨드 (서울 리전) 약 $0.026/시간 × 4대 ≈ **시간당 $0.10** (상시 가동 시 월 약 $75)
- EBS(gp3, 인스턴스당 기본 8GB) 및 데이터 전송 비용은 별도

## Spark Standalone 클러스터 구성

**Java/PySpark 버전**: PySpark 3.5.3은 Java 8/11/17만 공식 지원 (Java 21 미지원, Spark 4.x부터 지원). 따라서 **Java 17 (OpenJDK)** 로 설치.

**설치 방식**: 각 노드에 `python3 -m venv ~/pyspark-venv` 가상환경을 만들고 그 안에 `pip install pyspark==3.5.3`. 4대 전부 동일하게 설치.

> pip로 설치한 PySpark 배포판은 `sbin/`에 `start-master.sh` / `start-worker.sh` / `start-all.sh`가 포함되어 있지 않음 (history-server 스크립트만 존재). 그래서 표준 standalone 클러스터처럼 각 노드에서 `sbin/spark-daemon.sh`를 직접 호출해 Master/Worker 데몬을 기동함:
> - Master: `spark-daemon.sh start org.apache.spark.deploy.master.Master 1 --host <master-private-ip> --port 7077 --webui-port 8080`
> - Worker: `spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 spark://<master-private-ip>:7077 --webui-port 8081`

**클러스터 토폴로지**:

| 역할 | 접속 주소 | 상태 |
|---|---|---|
| Master | `spark://172.31.3.142:7077`, UI `http://3.34.200.25:8080` | ALIVE |
| Worker 1/2/3 | Master에 자동 등록 | 3대 모두 ALIVE, 각 1 core / 1024MB |

**검증**: `spark-submit --master spark://172.31.3.142:7077 examples/src/main/python/pi.py 10` 실행 → 클러스터 전체 분산 실행 확인 완료.

**알려진 제약**:
- `spark-daemon.sh`로 기동한 데몬은 systemd 서비스가 아니라 단순 프로세스라 **EC2 재부팅 시 자동 재기동되지 않음** — 상시 운영하려면 systemd unit 등록 필요.
- Worker 리소스가 t2.small 스펙(1 core/1024MB)만큼만 잡혀 있어, job 제출 시 `--executor-memory`를 낮게(예: 512m) 지정하지 않으면 리소스 부족 발생 가능.

## 남은 작업

- Spark 데몬 systemd 서비스화 (재부팅 시 자동 기동)
- `scripts/submit.sh`를 EC2 클러스터(`spark://172.31.3.142:7077`) 대상으로 확장
