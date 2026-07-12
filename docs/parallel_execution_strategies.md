# 순수 Python 병렬 실행 전략

NYC Taxi ETL 프로젝트에서 Mart 레이어 병렬 처리를 위한 전략 가이드

---

## 목차

1. [개요](#개요)
2. [병렬 처리 방법](#병렬-처리-방법)
3. [데이터 크기별 전략](#데이터-크기별-전략)
4. [실제 구현 예시](#실제-구현-예시)
5. [성능 비교](#성능-비교)

---

## 개요

### 병렬 처리가 필요한 이유

**현재 상황:**
```
Raw → Cleaned → Fact/Dim (순차) ✅
                    ↓
          Mart 레이어 (순차) ⚠️
          ├─ mart_vendor
          ├─ mart_zone
          ├─ mart_cumulative_vendor
          └─ mart_cumulative_zone
```

**문제:**
- 4개 Mart가 순차 실행 → 시간 4배
- 각 Mart는 독립적 → 병렬 처리 가능!

**목표:**
- Mart 레이어를 병렬로 실행하여 시간 단축

---

## 병렬 처리 방법

### 1️⃣ ProcessPoolExecutor (멀티프로세스, 추천)

**특징:**
- 각 작업이 독립된 Python 프로세스에서 실행
- GIL(Global Interpreter Lock) 우회 → 진짜 병렬 처리
- 프로세스 간 격리 → 메모리 안전

**코드 예시:**

```python
# jobs/parallel_mart_executor.py
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
from pyspark.sql import SparkSession

def create_vendor_mart(vendor_id, vendor_name, fact_path, output_path):
    """각 vendor별로 독립적인 프로세스에서 Spark job 실행"""
    spark = SparkSession.builder \
        .appName(f"Mart-Vendor-{vendor_id}") \
        .getOrCreate()
    
    # Fact 읽고 해당 vendor만 필터링
    df_fact = spark.read.parquet(fact_path)
    df_vendor = df_fact.filter(f"vendor_id = {vendor_id}")
    
    # Mart 생성 및 저장
    df_mart = df_vendor.groupBy("month", "hour", "zone_id") \
        .agg({"trip_count": "sum", "total_amount": "sum"})
    
    df_mart.write.mode("overwrite") \
        .parquet(f"{output_path}/vendor={vendor_id}")
    
    spark.stop()
    return f"Vendor {vendor_id} ({vendor_name}) completed"


def main():
    # CSV에서 vendor 리스트 로딩
    vendors_df = pd.read_csv("data/reference/vendors.csv")
    
    fact_path = "data/output/warehouse/fact_taxi_trip"
    output_path = "data/output/mart/vendor_split"
    
    # 병렬 실행
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = []
        for _, row in vendors_df.iterrows():
            future = executor.submit(
                create_vendor_mart,
                row['vendor_id'],
                row['vendor_name'],
                fact_path,
                output_path
            )
            futures.append(future)
        
        # 결과 수집
        for future in futures:
            print(future.result())

if __name__ == "__main__":
    main()
```

**장점:**
- ✅ 완전히 독립된 프로세스 실행
- ✅ GIL 우회 → 진짜 병렬 처리
- ✅ 메모리 격리 → 안전

**단점:**
- ❌ 프로세스 생성 오버헤드
- ❌ 메모리 복제 발생

---

### 2️⃣ ThreadPoolExecutor (멀티스레드)

**특징:**
- 같은 프로세스 내에서 여러 스레드 실행
- SparkSession 공유 가능
- I/O 바운드 작업에 적합

**코드 예시:**

```python
# jobs/threaded_mart_executor.py
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

def create_zone_mart_thread(zone_id, zone_name, spark, fact_path, output_path):
    """각 zone별로 독립적인 스레드에서 처리"""
    df_fact = spark.read.parquet(fact_path)
    df_zone = df_fact.filter(f"pickup_zone_id = {zone_id}")
    
    df_mart = df_zone.groupBy("month", "hour") \
        .agg({"trip_count": "sum", "total_amount": "sum"})
    
    df_mart.write.mode("overwrite") \
        .parquet(f"{output_path}/zone={zone_id}")
    
    return f"Zone {zone_id} ({zone_name}) completed"


def main():
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Mart-Zone-Parallel") \
        .getOrCreate()
    
    zones_df = pd.read_csv("data/reference/taxi_zone_lookup.csv")
    fact_path = "data/output/warehouse/fact_taxi_trip"
    output_path = "data/output/mart/zone_split"
    
    # 병렬 실행 (10개 zone 동시)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                create_zone_mart_thread, 
                row['LocationID'], 
                row['Zone'],
                spark,
                fact_path, 
                output_path
            )
            for _, row in zones_df.iterrows()
        ]
        
        for future in futures:
            print(future.result())
    
    spark.stop()

if __name__ == "__main__":
    main()
```

**장점:**
- ✅ 가볍다 (프로세스보다)
- ✅ SparkSession 공유 가능
- ✅ I/O 바운드 작업에 적합

**단점:**
- ❌ GIL 때문에 CPU 바운드 작업은 느림
- ❌ Spark 자체가 멀티스레드라 충돌 가능성

---

### 3️⃣ Subprocess + spark-submit (완전 독립, 가장 안전)

**특징:**
- 각 작업이 별도의 spark-submit 프로세스로 실행
- Docker/클러스터 환경에서 안전
- Spark UI에서 각 job 개별 추적 가능

**코드 예시:**

```python
# scripts/parallel_submit.py
import subprocess
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

def submit_vendor_job(vendor_id, vendor_name):
    """각 vendor별로 독립적인 spark-submit 실행"""
    cmd = [
        "docker", "exec", "nyc-taxi-spark",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--conf", f"spark.app.name=Mart-Vendor-{vendor_id}",
        "/opt/spark/jobs/create_vendor_mart.py",
        str(vendor_id),
        f"/opt/spark/data/output/mart/vendor={vendor_id}"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    return f"Vendor {vendor_id} exit code: {result.returncode}"


def main():
    vendors_df = pd.read_csv("data/reference/vendors.csv")
    
    # 병렬로 spark-submit 실행
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(submit_vendor_job, row['vendor_id'], row['vendor_name'])
            for _, row in vendors_df.iterrows()
        ]
        
        for future in futures:
            print(future.result())

if __name__ == "__main__":
    main()
```

**장점:**
- ✅ 완전히 독립된 Spark job
- ✅ Docker/클러스터 환경에서 안전
- ✅ Spark UI에서 각 job 개별 추적
- ✅ 실패한 job만 재실행 가능

**단점:**
- ❌ spark-submit 오버헤드
- ❌ 복잡한 설정

---

### 4️⃣ Multiprocessing Pool (클래식)

**코드 예시:**

```python
# jobs/mp_mart_executor.py
from multiprocessing import Pool
import pandas as pd

def create_vendor_mart_mp(args):
    """Multiprocessing Pool용 함수"""
    vendor_id, vendor_name, fact_path, output_path = args
    
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName(f"Mart-Vendor-{vendor_id}") \
        .getOrCreate()
    
    df_fact = spark.read.parquet(fact_path)
    df_vendor = df_fact.filter(f"vendor_id = {vendor_id}")
    
    # Mart 생성
    df_mart = df_vendor.groupBy("month", "hour", "zone_id") \
        .agg({"trip_count": "sum", "total_amount": "sum"})
    
    df_mart.write.mode("overwrite") \
        .parquet(f"{output_path}/vendor={vendor_id}")
    
    spark.stop()
    return f"Vendor {vendor_id} done"


def main():
    vendors_df = pd.read_csv("data/reference/vendors.csv")
    fact_path = "data/output/warehouse/fact_taxi_trip"
    output_path = "data/output/mart/vendor_split"
    
    # 작업 리스트 생성
    tasks = [
        (row['vendor_id'], row['vendor_name'], fact_path, output_path)
        for _, row in vendors_df.iterrows()
    ]
    
    # 멀티프로세싱 Pool
    with Pool(processes=2) as pool:
        results = pool.map(create_vendor_mart_mp, tasks)
    
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
```

---

### 5️⃣ DB에서 메타데이터 로딩

**코드 예시:**

```python
# jobs/db_driven_mart.py
import psycopg2
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

def load_vendors_from_db():
    """PostgreSQL에서 vendor 리스트 로딩"""
    conn = psycopg2.connect(
        host="localhost",
        database="taxi_metadata",
        user="user",
        password="pass"
    )
    
    query = "SELECT vendor_id, vendor_name FROM dim_vendor"
    vendors_df = pd.read_sql(query, conn)
    conn.close()
    
    return vendors_df


def main():
    # DB에서 vendor 정보 로딩
    vendors_df = load_vendors_from_db()
    
    # 병렬 처리
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(create_vendor_mart, row['vendor_id'], ...)
            for _, row in vendors_df.iterrows()
        ]
        
        for future in futures:
            print(future.result())
```

---

## 데이터 크기별 전략

### 📊 작은 데이터 (< 1GB, vendor 2개, zone 265개)

**추천: ThreadPoolExecutor**

```python
# 간단, 빠름, 오버헤드 최소
with ThreadPoolExecutor(max_workers=10) as executor:
    # 10개 zone 동시 처리
```

**이유:**
- 오버헤드 최소
- SparkSession 공유
- I/O 바운드 작업에 적합

---

### 📊 중간 데이터 (1-10GB)

**추천: ProcessPoolExecutor**

```python
# 안전한 병렬성
with ProcessPoolExecutor(max_workers=2) as executor:
    # vendor별 독립 프로세스
```

**이유:**
- 메모리 격리
- GIL 우회
- 안정적인 병렬 처리

---

### 📊 큰 데이터 (> 10GB, production)

**추천: Subprocess + spark-submit**

```python
# 완전 독립 Spark job
subprocess.run([
    "spark-submit",
    "--master", "spark://master:7077",
    "--executor-memory", "4g",
    "create_mart.py", vendor_id
])
```

**이유:**
- 각 vendor/zone이 별도 Spark job
- 클러스터 리소스 독립 할당
- Spark UI 개별 모니터링
- 실패한 job만 재실행

---

## 실제 구현 예시

### Orchestrator 구조

```python
# jobs/mart_orchestrator.py
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

def main():
    """
    Mart Layer 병렬 실행 Orchestrator
    """
    print("=== Mart Layer Parallel Execution ===")
    
    # 1. Metadata 로딩
    vendors = pd.read_csv("data/reference/vendors.csv")
    zones = pd.read_csv("data/reference/taxi_zone_lookup.csv")
    
    print(f"Vendors: {len(vendors)}")
    print(f"Zones: {len(zones)}")
    
    # 2. Vendor별 병렬 처리
    print("\n[Phase 1] Creating Vendor Marts...")
    with ProcessPoolExecutor(max_workers=2) as executor:
        vendor_futures = [
            executor.submit(create_vendor_mart, v['vendor_id'], v['vendor_name'])
            for _, v in vendors.iterrows()
        ]
        
        for future in vendor_futures:
            print(f"  ✓ {future.result()}")
    
    # 3. Zone별 병렬 처리 (vendor 완료 후)
    print("\n[Phase 2] Creating Zone Marts...")
    with ProcessPoolExecutor(max_workers=10) as executor:
        zone_futures = [
            executor.submit(create_zone_mart, z['LocationID'], z['Zone'])
            for _, z in zones.head(10).iterrows()  # 테스트: 10개만
        ]
        
        for future in zone_futures:
            print(f"  ✓ {future.result()}")
    
    print("\n=== All Marts Completed! ===")

if __name__ == "__main__":
    main()
```

---

## 성능 비교

### 순차 처리 (현재)

```
Mart 1 (vendor): 10분
Mart 2 (zone):   10분
Mart 3 (cumul):  10분
Mart 4 (other):  10분
─────────────────────
Total:           40분
```

### Mart 테이블별 병렬

```
Mart 1,2,3,4: 동시 실행
─────────────────────
Total:        10분 (4배 빠름)
```

### Vendor/Zone별 병렬 (Phase 1 → Phase 2)

```
Phase 1: Vendor 2개 병렬      5분
Phase 2: Zone 10개 병렬       3분
─────────────────────────────
Total:                        8분 (5배 빠름)
```

### 완전 병렬 (Mart + Vendor/Zone)

```
4개 Mart × (2 vendor + 10 zone) 동시
───────────────────────────────────
Total:                        ~5분 (8배 빠름)
```

---

## 핵심 아이디어

### 1. Metadata 기반 병렬화

```python
# vendors.csv 또는 DB에서 병렬 단위 결정
vendors = load_metadata()  # 2개

for vendor in vendors:
    submit_job(vendor_id)  # 병렬 실행
```

### 2. ProcessPoolExecutor로 독립 실행

```python
# GIL 우회 → 진짜 병렬
with ProcessPoolExecutor(max_workers=2) as executor:
    executor.submit(create_mart, vendor_id)
```

### 3. Subprocess로 완전 격리

```python
# Production 안전
subprocess.run([
    "docker", "exec", "spark-container",
    "spark-submit", "mart.py", vendor_id
])
```

---

## 주의사항

### 1. 메모리 관리

```python
# 동시 실행 수 제한
max_workers = min(cpu_count(), available_memory // job_memory)
```

### 2. 실패 처리

```python
# 실패한 job만 재실행
failed_vendors = []
for future in futures:
    try:
        result = future.result()
    except Exception as e:
        failed_vendors.append(vendor_id)
        print(f"Failed: {vendor_id}, {e}")

# 재시도
for vendor in failed_vendors:
    retry_job(vendor)
```

### 3. 진행 상황 추적

```python
from tqdm import tqdm

with ProcessPoolExecutor(max_workers=2) as executor:
    futures = [executor.submit(create_mart, v) for v in vendors]
    
    for future in tqdm(futures, desc="Creating Marts"):
        future.result()
```

---

## 결론

### 추천 전략

**개발/테스트 환경:**
```
ProcessPoolExecutor (max_workers=2)
→ 간단, 안전, 병렬성 확보
```

**Production 환경:**
```
Subprocess + spark-submit
→ 완전 격리, 모니터링 우수, 재실행 용이
```

### 구현 순서

1. ✅ 기본 Mart 로직 완성 (순차)
2. ✅ ProcessPoolExecutor로 병렬화
3. ✅ 성능 측정 및 비교
4. ✅ Subprocess 방식으로 Production 전환

---

## 다음 단계

- [ ] vendors.csv, zones.csv 생성
- [ ] ProcessPoolExecutor 버전 구현
- [ ] 성능 측정 (순차 vs 병렬)
- [ ] Subprocess 버전 구현
- [ ] Production 배포

---

**문서 버전:** 1.0  
**최종 수정:** 2026-07-08
