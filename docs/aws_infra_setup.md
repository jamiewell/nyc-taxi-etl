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

## 남은 작업

- 각 노드에 Java 17 + PySpark 3.5.3 설치
- Master/Worker 간 Spark standalone 클러스터 구성 (네이티브 설치 또는 기존 `docker/docker-compose.yml` 방식 재사용 검토)
- `scripts/submit.sh`를 EC2 클러스터 대상으로 확장
