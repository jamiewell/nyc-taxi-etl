"""
Sample parquet 파일을 로딩하고 스키마 확인하는 테스트 스크립트
"""
from pyspark.sql import SparkSession

def main():
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("Load Sample Parquet") \
        .master("local[*]") \
        .getOrCreate()

    # Sample parquet 파일 경로
    input_path = "data/sample/yellow_tripdata_2026-01.parquet"

    print(f"Loading parquet file: {input_path}")

    # Parquet 파일 로딩
    df = spark.read.parquet(input_path)

    # 기본 정보 출력
    print(f"\n총 레코드 수: {df.count():,}")

    print("\n스키마:")
    df.printSchema()

    print("\n샘플 데이터 (10건):")
    df.show(10, truncate=False)

    print("\n컬럼 리스트:")
    for col in df.columns:
        print(f"  - {col}")

    # 기본 통계
    print("\n기본 통계:")
    df.describe().show()

    spark.stop()

if __name__ == "__main__":
    main()
