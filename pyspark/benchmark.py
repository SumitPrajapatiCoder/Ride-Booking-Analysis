
import os
import sys
import time
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = BASE_DIR / "results" / "cleaned_rides.csv"
OUTPUT_FILE = BASE_DIR / "results" / "benchmark_results.csv"


def main():
    # Pandas benchmark
    start = time.perf_counter()
    pandas_df = pd.read_csv(INPUT_FILE)
    pandas_result = pandas_df.groupby(
        "Booking_Status", dropna=False
    ).size()
    pandas_seconds = time.perf_counter() - start

    # Spark benchmark
    spark = (
        SparkSession.builder
        .appName("PandasVsSparkBenchmark")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    start = time.perf_counter()
    spark_df = spark.read.option("header", True).csv(str(INPUT_FILE))
    spark_result = spark_df.groupBy("Booking_Status").count()
    spark_result.collect()  # Force execution before timing ends.
    spark_seconds = time.perf_counter() - start

    results = pd.DataFrame([
        {
            "Tool": "Pandas",
            "Rows": len(pandas_df),
            "Operation": "Booking status aggregation",
            "Runtime_Seconds": round(pandas_seconds, 4)
        },
        {
            "Tool": "PySpark",
            "Rows": spark_df.count(),
            "Operation": "Booking status aggregation",
            "Runtime_Seconds": round(spark_seconds, 4)
        }
    ])

    results.to_csv(OUTPUT_FILE, index=False)
    print(results.to_string(index=False))
    print("Saved:", OUTPUT_FILE)

    spark.stop()


if __name__ == "__main__":
    main()
