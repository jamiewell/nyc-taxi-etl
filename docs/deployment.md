# EC2 Spark 클러스터 배포 (GitHub Actions)

`main` 브랜치에 푸시된 소스코드를 EC2 Spark 마스터 노드에 배포하는 절차. AWS CodeDeploy/CodeCommit은 비용 문제로 사용하지 않고, GitHub Actions에서 직접 SSH/rsync로 배포한다.

## 왜 wheel/zip 패키징이 아니라 rsync인가

이 프로젝트의 job들(`jobs/main.py` 외 `save_raw_layer.py`, `raw_to_cleaned.py`, `cleaned_to_fact.py`, `fact_to_mart_*.py`, `collection_range.py`)은 서로를 평범한 Python import로 참조한다. `spark-submit`은 실행 대상 스크립트가 있는 디렉터리를 자동으로 `sys.path`에 추가하므로, 이 파일들이 **같은 디렉터리에 물리적으로 존재하기만 하면** 별도 패키징 없이 그대로 동작한다 (로컬/Docker 실행에서 이미 확인된 방식).

또한 이 프로젝트의 job들은 Python UDF나 RDD를 쓰지 않고 DataFrame API(`col()`, `groupBy()`, `join()`)만 사용한다. 즉 실제 데이터 처리는 JVM executor에서 일어나고, Python 코드는 드라이버(스크립트를 실행하는 노드)가 실행 계획을 세우는 데만 쓰인다. 그래서 **코드는 마스터 노드(드라이버 역할)에만 배포하면 되고, 워커 노드에는 필요 없다**.

이런 이유로 wheel 빌드나 `--py-files` zip 패키징 대신, 리포지토리를 그대로 `rsync`로 마스터 노드에 동기화하는 방식을 택했다.

## 배포 대상

| 항목 | 값 |
|---|---|
| 마스터 인스턴스 | `i-0131fffaf58ee5509` (`ap-northeast-2`) |
| 배포 경로 | `/home/ubuntu/nyc-taxi-batch` |
| 배포 방식 | `rsync -az --delete` (전체 리포 동기화, `data/output`/`data/spark-events`/`.git` 등 제외) |
| 실행 환경 | 마스터에 이미 설치된 `~/pyspark-venv` (Java 17 + PySpark 3.5.3, `docs/aws_infra_setup.md` 참고) |

## GitHub Actions 워크플로우 (`.github/workflows/deploy.yml`)

**트리거**: `workflow_dispatch` 수동 실행만. `main` push 시 자동 배포하지 않는다 — 마스터 노드는 비용 절감을 위해 평소 `stopped` 상태로 두기 때문에, push마다 자동으로 EC2를 깨우는 걸 피하기 위함.

**단계**:
1. 마스터 인스턴스가 `stopped`면 `ec2:StartInstances`로 기동, `running`/`status-ok`까지 대기
2. 마스터의 현재 Public IP를 `describe-instances`로 조회 (stop/start마다 IP가 바뀌므로 매번 동적 조회, Elastic IP 미사용)
3. 이 워크플로우를 실행 중인 GitHub-hosted runner의 현재 Public IP를 확인하고, 보안그룹(`sg-0cffb9429839840cd`)의 22번 포트에 **그 IP만 임시로 허용**
4. SSH 준비될 때까지 폴링 후 `rsync`로 리포지토리 동기화
5. 배포된 코드가 정상 로드되는지 `python3 jobs/main.py --help`로 가볍게 검증 (Spark 세션은 띄우지 않음)
6. **항상 실행되는 cleanup 단계**에서 3번에서 추가한 임시 SSH 허용 규칙을 삭제 (배포 성공/실패 여부와 무관하게 실행)

## 왜 보안그룹에 임시 규칙을 추가/삭제하는가

기존 보안그룹은 SSH(22)를 관리자의 특정 IP `/32`에만 허용하도록 되어 있었다 (`docs/aws_infra_setup.md`). GitHub-hosted runner는 고정 IP가 없어 이 상태로는 애초에 접속이 불가능하다.

대안으로 0.0.0.0/0 개방이나 GitHub의 공개 IP 대역(`api.github.com/meta`) 전체 허용도 있었지만, 둘 다 상시 노출 범위가 넓어진다. 대신 **매 실행마다 그 순간의 runner IP만 허용했다가 작업이 끝나면 바로 회수**하는 방식을 택해, 평소에는 기존과 동일하게 좁은 허용 범위를 유지한다.

## IAM 자격증명

배포 전용 IAM 사용자 `github-actions-nyc-taxi-deploy`를 만들어 사용한다 (root 계정 키를 GitHub Secrets에 넣지 않기 위함). 권한은 다음으로 최소화:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "DescribeInstances", "Effect": "Allow", "Action": "ec2:DescribeInstances", "Resource": "*" },
    { "Sid": "StartMasterOnly", "Effect": "Allow", "Action": "ec2:StartInstances",
      "Resource": "arn:aws:ec2:ap-northeast-2:962088872927:instance/i-0131fffaf58ee5509" },
    { "Sid": "ManageDeploySecurityGroupIngress", "Effect": "Allow",
      "Action": ["ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress"],
      "Resource": "arn:aws:ec2:ap-northeast-2:962088872927:security-group/sg-0cffb9429839840cd" }
  ]
}
```

인스턴스 정지(`StopInstances`)나 다른 리소스에 대한 권한은 없음 — 배포 워크플로우가 할 수 있는 일은 "마스터를 켜고, SSH로 코드를 올리는 것"뿐이다.

## 필요한 GitHub Secrets

리포지토리 Settings → Secrets and variables → Actions 에서 등록:

| Secret | 값 |
|---|---|
| `AWS_ACCESS_KEY_ID` | `github-actions-nyc-taxi-deploy` IAM 사용자의 Access Key ID |
| `AWS_SECRET_ACCESS_KEY` | 위 사용자의 Secret Access Key |
| `EC2_SSH_PRIVATE_KEY` | `nyc-taxi-cluster` key pair의 개인키 (`~/.ssh/nyc-taxi-cluster`) 전체 내용 |

## 실행 방법

1. GitHub 리포지토리 → Actions 탭 → "Deploy to EC2 Spark Master" 워크플로우 선택
2. "Run workflow" 클릭 (기본 `ref: main`)
3. 완료 후 로그 마지막에 찍히는 안내에 따라 **마스터 노드를 수동으로 stop** (워크플로우는 배포만 하고, 비용 절감을 위해 인스턴스를 자동으로 끄지 않음):
   ```bash
   aws ec2 stop-instances --instance-ids i-0131fffaf58ee5509 --region ap-northeast-2
   ```

## 배포 후 실제 job 실행

이 워크플로우는 코드 배포까지만 수행한다. 배포된 코드로 실제 ETL을 실행하려면 마스터 노드에 SSH 접속 후 직접 `spark-submit`을 실행해야 한다 (자동 실행은 의도적으로 포함하지 않음 — 배포와 실행을 분리):

```bash
ssh -i ~/.ssh/nyc-taxi-cluster ubuntu@<master-public-ip>
cd /home/ubuntu/nyc-taxi-batch
source ~/pyspark-venv/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
spark-submit --master spark://<master-private-ip>:7077 jobs/main.py <source_path> <base_output_path> --taxi-types yellow
```
