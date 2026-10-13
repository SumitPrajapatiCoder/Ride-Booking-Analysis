
import os
import sys
import csv
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_CSV = BASE_DIR / "data" / "mumbai_booking_data.csv"
RESULTS_DIR = BASE_DIR / "results"
OUTPUT_CSV = RESULTS_DIR / "cleaned_rides.csv"


def create_spark():
    return (
        SparkSession.builder
        .appName("RideBookingDataCleaning")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def clean_dataset():
    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            f"CSV not found: {INPUT_CSV}\n"
            "Put your original file in data/mumbai_booking_data.csv"
        )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    spark = create_spark()

    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("nullValue", "null")
            .option("encoding", "UTF-8")
            .csv(str(INPUT_CSV))
        )

        # Clean whitespace in column names.
        for column in df.columns:
            cleaned_name = column.strip()
            if column != cleaned_name:
                df = df.withColumnRenamed(column, cleaned_name)

        # Standardise empty values and common null markers.
        for column, dtype in df.dtypes:
            if dtype == "string":
                value = F.trim(F.col(column))
                df = df.withColumn(
                    column,
                    F.when(
                        F.lower(value).isin(
                            "", "null", "none", "nan", "n/a", "na"
                        ),
                        F.lit(None)
                    ).otherwise(value)
                )

        # Parse Date safely. Source examples use dd-MM-yyyy.
        df = df.withColumn(
            "Date",
            F.coalesce(
                F.to_date("Date", "dd-MM-yyyy"),
                F.to_date("Date", "yyyy-MM-dd"),
                F.to_date("Date", "MM/dd/yyyy")
            )
        )

        # Standardise Time as HH:mm:ss.
        df = df.withColumn(
            "Time",
            F.date_format(
                F.coalesce(
                    F.to_timestamp("Time", "HH:mm:ss"),
                    F.to_timestamp("Time", "H:mm:ss")
                ),
                "HH:mm:ss"
            )
        )

        # Convert metrics to numbers; invalid values become null.
        numeric_columns = [
            "Avg_VTAT",
            "Avg_CTAT",
            "Booking_Value",
            "Ride_Distance",
            "Driver_Ratings",
            "Customer_Rating",
        ]

        for column in numeric_columns:
            if column in df.columns:
                df = df.withColumn(
                    column,
                    F.regexp_replace(
                        F.col(column).cast("string"), ",", ""
                    ).cast(DoubleType())
                )

        # Standardise categorical fields.
        if "Booking_Status" in df.columns:
            df = df.withColumn(
                "Booking_Status",
                F.initcap(F.trim(F.col("Booking_Status")))
            )

        if "Vehicle Type" in df.columns:
            df = df.withColumn(
                "Vehicle Type", F.trim(F.col("Vehicle Type"))
            )

        if "Payment_Method" in df.columns:
            df = df.withColumn(
                "Payment_Method", F.trim(F.col("Payment_Method"))
            )

        # Add analysis columns.
        df = df.withColumn(
            "Hour",
            F.hour(F.to_timestamp("Time", "HH:mm:ss"))
        )
        df = df.withColumn(
            "Day_Of_Week",
            F.date_format(F.col("Date"), "EEEE")
        )
        df = df.withColumn(
            "Month",
            F.date_format(F.col("Date"), "yyyy-MM")
        )

        input_count = df.count()

        # One row per Booking_ID. Null IDs are retained rather than
        # accidentally treating all missing IDs as one booking.
        if "Booking_ID" in df.columns:
            identified = df.filter(F.col("Booking_ID").isNotNull())
            unidentified = df.filter(F.col("Booking_ID").isNull())
            identified = identified.dropDuplicates(["Booking_ID"])
            df = identified.unionByName(unidentified)

        cleaned_rows = df.collect()
        columns = df.columns

        # Save a single CSV file instead of Spark's part-*.csv directory.
        with open(
            OUTPUT_CSV, "w", newline="", encoding="utf-8-sig"
        ) as file:
            writer = csv.writer(file)
            writer.writerow(columns)

            for row in cleaned_rows:
                writer.writerow([
                    value.isoformat()
                    if hasattr(value, "isoformat")
                    else value
                    for value in row
                ])

        print(f"Rows before duplicate handling: {input_count}")
        print(f"Rows after cleaning: {len(cleaned_rows)}")
        print(f"Cleaned CSV saved at: {OUTPUT_CSV}")

    finally:
        spark.stop()


if __name__ == "__main__":
    clean_dataset()
