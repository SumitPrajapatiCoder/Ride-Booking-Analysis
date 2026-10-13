import os
import sys
import shutil
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = BASE_DIR / "results" / "cleaned_rides.csv"
RESULTS_DIR = BASE_DIR / "results"


def save_csv(df, filename):
    target = RESULTS_DIR / f"_{filename}_temp"
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(target))
    )

    parts = list(target.glob("part-*.csv"))
    if not parts:
        raise RuntimeError(f"No CSV output created for {filename}")

    shutil.copy2(parts[0], RESULTS_DIR / filename)
    shutil.rmtree(target)


def main():
    spark = (
        SparkSession.builder
        .appName("RideBookingAnalysis")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", True).option(
        "inferSchema", True
    ).csv(str(INPUT_FILE))

    required = [
        "Booking_Status", "Vehicle Type", "Payment_Method",
        "Pickup_Location", "Drop_Location", "Booking_ID"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Required columns missing: {missing}")

    # 1. Booking status distribution
    status = df.groupBy("Booking_Status").agg(
        F.count("*").alias("Bookings")
    ).orderBy(F.desc("Bookings"))
    save_csv(status, "booking_status.csv")

    # 2. Vehicle-wise booking status
    vehicle = df.groupBy("Vehicle Type", "Booking_Status").agg(
        F.count("*").alias("Bookings")
    ).orderBy("Vehicle Type", "Booking_Status")
    save_csv(vehicle, "vehicle_status.csv")

    # 3. Cancellation reasons
    if "Incomplete_Rides_Reason" in df.columns:
        reasons = df.filter(
            F.lower(F.coalesce(F.col("Booking_Status"), F.lit("")))
            .contains("cancel")
            | F.lower(F.coalesce(
                F.col("Booking_Status"), F.lit("")
            )).contains("incomplete")
        ).groupBy("Incomplete_Rides_Reason").agg(
            F.count("*").alias("Bookings")
        ).orderBy(F.desc("Bookings"))

        save_csv(reasons, "cancellation_reasons.csv")

    # 4. Daily bookings
    if "Date" in df.columns:
        daily = df.groupBy("Date").agg(
            F.count("*").alias("Bookings")
        ).orderBy("Date")
        save_csv(daily, "daily_bookings.csv")

    # 5. Hourly bookings
    if "Hour" in df.columns:
        hourly = df.groupBy("Hour").agg(
            F.count("*").alias("Bookings")
        ).orderBy("Hour")
        save_csv(hourly, "hourly_bookings.csv")

    # 6. Successful booking payment methods
    successful = df.filter(
        F.lower(F.coalesce(F.col("Booking_Status"), F.lit("")))
        == "success"
    )

    payment = successful.groupBy("Payment_Method").agg(
        F.count("*").alias("Bookings")
    ).orderBy(F.desc("Bookings"))
    save_csv(payment, "payment_methods.csv")

    # 7. Pickup locations
    pickup = df.groupBy("Pickup_Location").agg(
        F.count("*").alias("Bookings")
    ).orderBy(F.desc("Bookings"))
    save_csv(pickup, "pickup_locations.csv")

    print("Analysis CSV files saved in:", RESULTS_DIR)
    spark.stop()


if __name__ == "__main__":
    main()
