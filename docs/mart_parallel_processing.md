# Mart Layer 병렬 처리 가이드

## 개요

NYC Taxi 프로젝트에서 Mart Layer는 Fact Layer에만 의존하고 각 Mart 테이블이 서로 독립적이므로 병렬 처리가 가능합니다.

```
Raw Layer (순차)
  ↓
Cleaned Layer (순차)
  ↓
Fact/Dimension Layer (순차)
  ↓
Mart Layer (병렬 가능!) ← 이 단계를 병렬 처리
  ├─ mart_month_hour_zone_trip_metrics
  ├─ mart_month_hour_vendor_trip_metrics
  ├─ mart_month_vendor_cumulative_metrics
  └─ mart_month_zone_cumulative_metrics
```

## 병렬 처리 전략

### 전략 1: Mart 테이블별 병렬 처리
4개 Mart 테이블을 동시에 생성 (추천)

### 전략 2: Vendor/Zone 단위 병렬 처리
각 Mart 내에서 Vendor(2개) 또는 Zone(265개)별로 분할 처리

---

## 방법 1: ProcessPoolExecutor (멀티프로세스) ⭐ 추천

### 개요
각 Mart를 독립된 Python 프로세스에서 실행

### 코드 예시

```python
# jobs/mart_parallel_executor.py
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
from pyspark.sql import SparkSession


def process_vendor_mart(vendor_id, vendor_name, fact_path, output_path):
    """
    각 vendor별로 독립적인 Python 프로세스에서 Spark job 실행
    """
    spark = SparkSession.builder \
        .appName(f"Mart-Vendor-{vendor_id}") \
        .getOrCreate()
    
    try:
        # Fact 읽고 해당 vendor만 필터링
        df_fact = spark.read.parquet(fact_path)
        df_vendor = df_fact.filter(f"vendor_id = {vendor_id}")
        
        # Mart 생성 (groupBy, aggregation 등)
        df_mart = df_vendor.groupBy("pickup_month", "pickup_hour", "vendor_id") \
            .agg({
                "trip_count": "sum",
                "total_amount": "sum",
                "trip_distance": "avg"
            })
        
        # 저장
        df_mart.write.mode("overwrite").parquet(f"{output_path}/vendor={vendor_id}")
        
        return f"✓ Vendor {vendor_id} ({vendor_name}) completed"
    
    except Exception as e:
        return f"✗ Vendor {vendor_id} failed: {str(e)}"
    
    finally:
        spark.stop()


def process_zone_mart(zone_id, zone_name, fact_path, output_path):
    """
    각 zone별로 독립적인 Python 프로세스에서 Spark job 실행
    """
    spark = SparkSession.builder \
        .appName(f"Mart-Zone-{zone_id}") \
        .getOrCreate()
    
    try:
        df_fact = spark.read.parquet(fact_path)
        df_zone = df_fact.filter(f"pickup_zone_id = {zone_id}")
        
        df_mart = df_zone.groupBy("pickup_month", "pickup_hour", "pickup_zone_id") \
            .agg({
                "trip_count": "sum",
                "total_amount": "sum",
                "passenger_count": "avg"
            })
        
        df_mart.write.mode("overwrite").parquet(f"{output_path}/zone={zone_id}")
        
        return f"✓ Zone {zone_id} ({zone_name}) completed"
    
    except Exception as e:
        return f"✗ Zone {zone_id} failed: {str(e)}"
    
    finally:
        spark.stop()


def main_parallel_processing():
    """
    메인 orchestrator: CSV/DB에서 메타데이터 로딩 후 병렬 실행
    """
    # CSV에서 vendor 리스트 로딩
    vendors_df = pd.read_csv("data/reference/vendors.csv")
    zones_df = pd.read_csv("data/reference/taxi_zone_lookup.csv")
    
    fact_path = "data/output/warehouse/fact_taxi_trip"
    output_base = "data/output/mart"
    
    print("="*70)
    print("Starting Parallel Mart Processing")
    print("="*70)
    
    # Vendor별 병렬 처리
    print("\n[Phase 1] Processing Vendor Marts...")
    with ProcessPoolExecutor(max_workers=2) as executor:
        vendor_futures = [
            executor.submit(
                process_vendor_mart,
                row['vendor_id'],
                row['vendor_name'],
                fact_path,
                f"{output_base}/vendor_split"
            )
            for _, row in vendors_df.iterrows()
        ]
        
        for future in vendor_futures:
            print(future.result())
    
    # Zone별 병렬 처리 (10개씩 동시)
    print("\n[Phase 2] Processing Zone Marts...")
    with ProcessPoolExecutor(max_workers=10) as executor:
        zone_futures = [
            executor.submit(
                process_zone_mart,
                row['LocationID'],
                row['Zone'],
                fact_path,
                f"{output_base}/zone_split"
            )
            for _, row in zones_df.iterrows()
        ]
        
        # 진행 상황 출력
        completed = 0
        for future in zone_futures:
            result = future.result()
            completed += 1
            if completed % 50 == 0:
                print(f"Progress: {completed}/{len(zone_futures)} zones completed")
    
    print("\n" + "="*70)
    print("✓ All Mart Processing Completed!")
    print("="*70)


if __name__ == "__main__":
    main_parallel_processing()
```

### 장점
- ✅ 각 프로세스가 **완전히 독립** → 메모리 격리
- ✅ GIL(Global Interpreter Lock) 우회 → **진짜 병렬 처리**
- ✅ 프로세스 간 격리로 **안전성** 보장
- ✅ 한 프로세스 실패해도 다른 프로세스 영향 없음

### 단점
- ❌ 프로세스 생성 **오버헤드** (ThreadPool보다 무거움)
- ❌ 메모리 복제 발생 (각 프로세스가 독립 메모리)

### 적합한 경우
- Vendor(2개) 또는 작은 단위 병렬
- 안정성이 중요한 Production 환경
- 메모리가 충분한 환경

---

## 방법 2: ThreadPoolExecutor (멀티스레드)

### 개요
동일한 SparkSession을 공유하며 스레드별로 처리

### 코드 예시

```python
# jobs/mart_thread_executor.py
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from pyspark.sql import SparkSession


# 전역 SparkSession 생성 (스레드 간 공유)
spark = SparkSession.builder \
    .appName("Mart-Thread-Pool") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()


def process_zone_mart_thread(zone_id, zone_name, fact_path, output_path):
    """
    각 zone별로 독립적인 스레드에서 처리
    SparkSession은 thread-safe하므로 공유 가능
    """
    try:
        df_fact = spark.read.parquet(fact_path)
        df_zone = df_fact.filter(f"pickup_zone_id = {zone_id}")
        
        df_mart = df_zone.groupBy("pickup_month", "pickup_hour", "pickup_zone_id") \
            .agg({
                "trip_count": "sum",
                "total_amount": "sum"
            })
        
        df_mart.write.mode("overwrite").parquet(f"{output_path}/zone={zone_id}")
        
        return f"✓ Zone {zone_id} completed"
    
    except Exception as e:
        return f"✗ Zone {zone_id} failed: {str(e)}"


def main_thread_processing():
    """
    스레드 기반 병렬 처리
    """
    zones_df = pd.read_csv("data/reference/taxi_zone_lookup.csv")
    
    fact_path = "data/output/warehouse/fact_taxi_trip"
    output_path = "data/output/mart/zone_thread"
    
    print("Starting Thread-based Mart Processing...")
    
    # 10개 zone 동시 처리
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                process_zone_mart_thread,
                row['LocationID'],
                row['Zone'],
                fact_path,
                output_path
            )
            for _, row in zones_df.iterrows()
        ]
        
        for future in futures:
            print(future.result())
    
    spark.stop()
    print("✓ Thread Processing Completed!")


if __name__ == "__main__":
    main_thread_processing()
```

### 장점
- ✅ **가볍다** (프로세스보다 훨씬 적은 오버헤드)
- ✅ SparkSession 공유 가능 (메모리 효율)
- ✅ I/O 바운드 작업(파일 읽기/쓰기)에 **매우 효과적**

### 단점
- ❌ GIL 때문에 **CPU 바운드 작업은 느림**
- ❌ Spark 자체가 멀티스레드라 충돌 가능성
- ❌ 디버깅 어려움 (스레드 간 상호작용)

### 적합한 경우
- Zone(265개) 같은 **많은 단위 병렬**
- I/O 중심 작업 (Spark read/write)
- 메모리가 제한적인 환경

---

## 방법 3: Subprocess + spark-submit (완전 독립) ⭐ Production 추천

### 개요
각 Mart를 독립적인 `spark-submit` 프로세스로 실행

### 코드 예시

```python
# jobs/mart_subprocess_executor.py
import subprocess
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


def submit_vendor_job(vendor_id, vendor_name, fact_path, output_path):
    """
    각 vendor별로 독립적인 spark-submit 프로세스 실행
    """
    cmd = [
        "docker", "exec", "nyc-taxi-spark",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--driver-memory", "1g",
        "--executor-memory", "1g",
        "--conf", f"spark.app.name=Mart-Vendor-{vendor_id}",
        "/opt/spark/jobs/create_vendor_mart.py",  # 별도 스크립트
        str(vendor_id),
        fact_path,
        output_path
    ]
    
    print(f"[Vendor {vendor_id}] Starting spark-submit...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        return f"✓ Vendor {vendor_id} ({vendor_name}) completed"
    else:
        return f"✗ Vendor {vendor_id} failed: {result.stderr}"


def submit_zone_job(zone_id, zone_name, fact_path, output_path):
    """
    각 zone별로 독립적인 spark-submit 프로세스 실행
    """
    cmd = [
        "docker", "exec", "nyc-taxi-spark",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--conf", f"spark.app.name=Mart-Zone-{zone_id}",
        "/opt/spark/jobs/create_zone_mart.py",
        str(zone_id),
        fact_path,
        output_path
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        return f"✓ Zone {zone_id} completed"
    else:
        return f"✗ Zone {zone_id} failed"


def main_subprocess_processing():
    """
    Subprocess 기반 완전 독립 실행
    """
    vendors_df = pd.read_csv("data/reference/vendors.csv")
    
    fact_path = "/opt/spark/data/output/warehouse/fact_taxi_trip"
    output_base = "/opt/spark/data/output/mart"
    
    print("Starting Subprocess-based Mart Processing...")
    
    # 병렬로 spark-submit 실행 (ThreadPool로 subprocess 관리)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                submit_vendor_job,
                row['vendor_id'],
                row['vendor_name'],
                fact_path,
                f"{output_base}/vendor_subprocess/vendor={row['vendor_id']}"
            )
            for _, row in vendors_df.iterrows()
        ]
        
        for future in futures:
            print(future.result())
    
    print("✓ Subprocess Processing Completed!")


if __name__ == "__main__":
    main_subprocess_processing()
```

### 개별 Mart Job 스크립트

```python
# jobs/create_vendor_mart.py
import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) < 4:
        print("Usage: create_vendor_mart.py <vendor_id> <fact_path> <output_path>")
        sys.exit(1)
    
    vendor_id = int(sys.argv[1])
    fact_path = sys.argv[2]
    output_path = sys.argv[3]
    
    spark = SparkSession.builder.getOrCreate()
    
    # Fact 로딩
    df_fact = spark.read.parquet(fact_path)
    df_vendor = df_fact.filter(f"vendor_id = {vendor_id}")
    
    # Mart 생성
    df_mart = df_vendor.groupBy("pickup_month", "pickup_hour", "vendor_id") \
        .agg({
            "trip_count": "sum",
            "total_amount": "sum"
        })
    
    # 저장
    df_mart.write.mode("overwrite").parquet(output_path)
    
    print(f"✓ Vendor {vendor_id} Mart completed")
    spark.stop()

if __name__ == "__main__":
    main()
```

### 장점
- ✅ **완전히 독립된 Spark job** → 최고 안전성
- ✅ Docker/클러스터 환경에서 가장 안정적
- ✅ **Spark UI에서 각 job 개별 추적** 가능
- ✅ Production 환경에 최적화

### 단점
- ❌ spark-submit **오버헤드** (프로세스 시작 시간)
- ❌ 복잡한 설정 (각 job별 스크립트 필요)
- ❌ 로그 관리 복잡

### 적합한 경우
- **Production 환경**
- 큰 데이터셋 (10GB+)
- 완전한 격리가 필요한 경우
- Spark UI 모니터링이 중요한 경우

---

## 방법 4: Multiprocessing Pool (클래식)

### 개요
Python 기본 multiprocessing 라이브러리 사용

### 코드 예시

```python
# jobs/mart_multiprocessing_executor.py
from multiprocessing import Pool
import pandas as pd
from pyspark.sql import SparkSession


def process_vendor_mart_mp(args):
    """
    멀티프로세싱 Pool용 함수
    """
    vendor_id, vendor_name, fact_path, output_path = args
    
    spark = SparkSession.builder \
        .appName(f"Mart-Vendor-{vendor_id}") \
        .getOrCreate()
    
    try:
        df_fact = spark.read.parquet(fact_path)
        df_vendor = df_fact.filter(f"vendor_id = {vendor_id}")
        
        df_mart = df_vendor.groupBy("pickup_month", "pickup_hour", "vendor_id") \
            .agg({"trip_count": "sum", "total_amount": "sum"})
        
        df_mart.write.mode("overwrite").parquet(f"{output_path}/vendor={vendor_id}")
        
        return f"✓ Vendor {vendor_id} done"
    
    finally:
        spark.stop()


def main_multiprocessing():
    """
    Multiprocessing Pool 실행
    """
    vendors_df = pd.read_csv("data/reference/vendors.csv")
    
    fact_path = "data/output/warehouse/fact_taxi_trip"
    output_path = "data/output/mart/vendor_mp"
    
    # 작업 리스트 생성
    tasks = [
        (row['vendor_id'], row['vendor_name'], fact_path, output_path)
        for _, row in vendors_df.iterrows()
    ]
    
    # 멀티프로세싱 Pool (2개 프로세스)
    with Pool(processes=2) as pool:
        results = pool.map(process_vendor_mart_mp, tasks)
    
    for result in results:
        print(result)
    
    print("✓ Multiprocessing Completed!")


if __name__ == "__main__":
    main_multiprocessing()
```

### 장점
- ✅ Python 기본 라이브러리 (별도 설치 불필요)
- ✅ 간단한 API (`map` 함수로 일괄 처리)

### 단점
- ❌ ProcessPoolExecutor보다 **유연성 낮음**
- ❌ 에러 처리가 어려움

### 적합한 경우
- 간단한 배치 작업
- Python 기본 라이브러리만 사용 가능한 환경

---

## 방법 5: DB에서 메타데이터 로딩

### 개요
Vendor/Zone 정보를 PostgreSQL/MySQL 같은 DB에서 로딩 후 병렬 처리

### 코드 예시

```python
# jobs/mart_db_loader.py
import psycopg2
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from pyspark.sql import SparkSession


def load_vendors_from_db():
    """
    PostgreSQL에서 vendor 리스트 로딩
    """
    conn = psycopg2.connect(
        host="localhost",
        database="taxi_metadata",
        user="admin",
        password="password"
    )
    
    query = "SELECT vendor_id, vendor_name FROM dim_vendor"
    vendors_df = pd.read_sql(query, conn)
    conn.close()
    
    return vendors_df


def load_zones_from_db():
    """
    PostgreSQL에서 zone 리스트 로딩
    """
    conn = psycopg2.connect(
        host="localhost",
        database="taxi_metadata",
        user="admin",
        password="password"
    )
    
    query = "SELECT location_id, zone, borough FROM dim_taxi_zone"
    zones_df = pd.read_sql(query, conn)
    conn.close()
    
    return zones_df


def process_vendor_from_db(vendor_id, vendor_name, fact_path, output_path):
    """
    Vendor별 Mart 생성
    """
    spark = SparkSession.builder \
        .appName(f"Mart-Vendor-{vendor_id}") \
        .getOrCreate()
    
    try:
        df_fact = spark.read.parquet(fact_path)
        df_vendor = df_fact.filter(f"vendor_id = {vendor_id}")
        
        df_mart = df_vendor.groupBy("pickup_month", "vendor_id") \
            .agg({"trip_count": "sum", "total_amount": "sum"})
        
        df_mart.write.mode("overwrite").parquet(f"{output_path}/vendor={vendor_id}")
        
        return f"✓ Vendor {vendor_id} ({vendor_name}) completed"
    
    finally:
        spark.stop()


def main_db_processing():
    """
    DB에서 메타데이터 로딩 후 병렬 처리
    """
    # DB에서 vendor 정보 로딩
    print("Loading vendors from database...")
    vendors_df = load_vendors_from_db()
    print(f"Loaded {len(vendors_df)} vendors")
    
    # DB에서 zone 정보 로딩
    print("Loading zones from database...")
    zones_df = load_zones_from_db()
    print(f"Loaded {len(zones_df)} zones")
    
    fact_path = "data/output/warehouse/fact_taxi_trip"
    output_base = "data/output/mart"
    
    # Vendor별 병렬 처리
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                process_vendor_from_db,
                row['vendor_id'],
                row['vendor_name'],
                fact_path,
                f"{output_base}/vendor_db"
            )
            for _, row in vendors_df.iterrows()
        ]
        
        for future in futures:
            print(future.result())
    
    print("✓ DB-based Processing Completed!")


if __name__ == "__main__":
    main_db_processing()
```

### 장점
- ✅ **중앙화된 메타데이터 관리**
- ✅ CSV 파일 관리 불필요
- ✅ 메타데이터 업데이트가 실시간 반영

### 단점
- ❌ DB 의존성 추가
- ❌ 네트워크 I/O 오버헤드

### 적합한 경우
- 메타데이터가 자주 변경되는 환경
- 여러 시스템에서 공유하는 메타데이터

---

## 성능 비교

### 데이터 크기별 추천

| 데이터 크기 | 병렬 단위 | 추천 방법 | 예상 성능 |
|------------|----------|---------|---------|
| **작은 데이터 (< 1GB)** | Mart 테이블별 (4개) | ThreadPoolExecutor | 순차 대비 **3~4배 빠름** |
| **중간 데이터 (1-10GB)** | Vendor별 (2개) | ProcessPoolExecutor | 순차 대비 **2배 빠름** |
| **큰 데이터 (> 10GB)** | Zone별 (265개) | Subprocess + spark-submit | 순차 대비 **5~10배 빠름** |

### 실행 시간 예상

**순차 처리 (현재):**
```
Mart 1: 10분
Mart 2: 10분
Mart 3: 10분
Mart 4: 10분
Total: 40분
```

**Mart별 병렬 (ThreadPool):**
```
Mart 1,2,3,4: 동시 10분
Total: 10분 (4배 빠름)
```

**Vendor/Zone별 병렬 (ProcessPool):**
```
Vendor 1, 2: 동시 5분
Zone 1~265: 동시 7분
Total: ~12분 (추가 30% 향상)
```

---

## 실전 적용 가이드

### Phase 1: Mart 테이블별 병렬 (간단)

```python
# main.py의 Mart 단계를 수정
from concurrent.futures import ThreadPoolExecutor

def create_marts_parallel(fact_df, output_base):
    """
    4개 Mart를 동시에 생성
    """
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(create_mart_vendor, fact_df, f"{output_base}/vendor"),
            executor.submit(create_mart_zone, fact_df, f"{output_base}/zone"),
            executor.submit(create_mart_cumul_vendor, fact_df, f"{output_base}/cumul_vendor"),
            executor.submit(create_mart_cumul_zone, fact_df, f"{output_base}/cumul_zone"),
        ]
        
        for future in futures:
            print(future.result())
```

### Phase 2: Vendor/Zone별 병렬 (고급)

```python
# 별도 orchestrator 스크립트 생성
python3 jobs/mart_parallel_executor.py
```

---

## 모니터링

### Spark UI 확인 포인트

1. **Application UI (http://localhost:4040)**
   - Jobs 탭: 병렬 실행되는 job 확인
   - Stages 탭: Shuffle 발생 여부
   - Executors 탭: 리소스 사용률

2. **Master UI (http://localhost:8080)**
   - Running Applications: 동시 실행 중인 job 수
   - Completed Applications: 완료된 job 이력

3. **History Server (http://localhost:18080)**
   - 완료된 job의 상세 분석
   - 병렬 처리 전후 비교

---

## 주의사항

### 1. 메모리 관리
```python
# 각 프로세스가 Spark job 실행 후 반드시 stop
spark.stop()
```

### 2. 동시 실행 개수 제한
```python
# Worker가 2개 core면 max_workers=2로 제한
with ProcessPoolExecutor(max_workers=2) as executor:
    # ...
```

### 3. 에러 처리
```python
# 각 job의 성공/실패 로깅
try:
    result = future.result()
    print(f"✓ {result}")
except Exception as e:
    print(f"✗ Failed: {str(e)}")
```

### 4. 경로 확인
```python
# Docker 컨테이너 내부 경로 사용
fact_path = "/opt/spark/data/output/warehouse/fact_taxi_trip"
# 로컬 경로 아님: data/output/...
```

---

## 결론

| 방법 | 난이도 | 성능 | 안정성 | 추천 용도 |
|-----|-------|------|--------|---------|
| **ProcessPoolExecutor** | ★★☆ | ★★★ | ★★★ | **Vendor별 병렬, 중간 데이터** |
| **ThreadPoolExecutor** | ★☆☆ | ★★☆ | ★★☆ | **Mart별 병렬, 작은 데이터** |
| **Subprocess + spark-submit** | ★★★ | ★★★ | ★★★★ | **Production, 큰 데이터** |
| **Multiprocessing Pool** | ★☆☆ | ★★☆ | ★★☆ | 간단한 배치 |
| **DB 메타데이터** | ★★☆ | ★★★ | ★★★ | 메타데이터 중앙 관리 |

### 최종 추천

1. **개발/테스트**: `ThreadPoolExecutor` (Mart 4개 동시)
2. **중간 규모**: `ProcessPoolExecutor` (Vendor 2개 동시)
3. **Production**: `Subprocess + spark-submit` (완전 격리)

---

## 참고 자료

- [Python concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html)
- [Spark Parallel Processing](https://spark.apache.org/docs/latest/rdd-programming-guide.html#parallelized-collections)
- [NYC Taxi 프로젝트 CLAUDE.md](../CLAUDE.md)
