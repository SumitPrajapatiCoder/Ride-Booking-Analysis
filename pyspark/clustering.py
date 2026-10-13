
import os
import sys
import csv
from pathlib import Path

# --------------------------------------------------
# 1. ENVIRONMENT SETUP
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]
RESULTS_DIR = BASE_DIR / "results"
INPUT_FILE = RESULTS_DIR / "cleaned_rides.csv"
OUTPUT_FILE = RESULTS_DIR / "ride_clusters.csv"

# Set Python executable before importing Spark.
# Use the raw path; quotes are not required in os.environ.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans


# --------------------------------------------------
# 2. FEATURES USED FOR CLUSTERING
# --------------------------------------------------

FEATURES = [
    "Ride_Distance",
    "Booking_Value",
    "Avg_VTAT",
    "Avg_CTAT",
]


# --------------------------------------------------
# 3. MAIN FUNCTION
# --------------------------------------------------

def main():

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Cleaned dataset not found: {INPUT_FILE}\n"
            "Run: python pyspark/clean.py"
        )

    spark = (
        SparkSession.builder
        .appName("RideBookingKMeans")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # ------------------------------------------
        # 4. LOAD CLEANED DATA
        # ------------------------------------------

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(INPUT_FILE))
        )

        print("\n========== RIDE CLUSTERING ==========")
        print("Input dataset:", INPUT_FILE)
        print("Total input rows:", df.count())

        missing_columns = [
            col for col in FEATURES if col not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing required feature columns: {missing_columns}"
            )

        # ------------------------------------------
        # 5. CONVERT FEATURES TO NUMERIC
        # ------------------------------------------

        for col in FEATURES:
            df = df.withColumn(
                col,
                F.regexp_replace(
                    F.col(col).cast("string"), ",", ""
                ).cast("double")
            )

        # Keep records that have all four valid features.
        valid = df.dropna(subset=FEATURES)

        # Remove NaN and infinite/extreme invalid values.
        for col in FEATURES:
            valid = valid.filter(
                (~F.isnan(F.col(col)))
                & (F.abs(F.col(col)) < F.lit(1e12))
            )

        valid_count = valid.count()

        print("Rows available for clustering:", valid_count)

        if valid_count < 3:
            raise ValueError(
                "K-Means with k=3 requires at least 3 valid records."
            )

        # ------------------------------------------
        # 6. ASSEMBLE FEATURES
        # ------------------------------------------

        assembler = VectorAssembler(
            inputCols=FEATURES,
            outputCol="raw_features",
            handleInvalid="error"
        )

        # Standardize features because their units differ.
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withMean=True,
            withStd=True
        )

        # ------------------------------------------
        # 7. K-MEANS MODEL
        # ------------------------------------------

        kmeans = KMeans(
            featuresCol="features",
            predictionCol="Cluster",
            k=3,
            seed=42,
            maxIter=50
        )

        pipeline = Pipeline(
            stages=[assembler, scaler, kmeans]
        )

        print("\nTraining K-Means model...")
        model = pipeline.fit(valid)

        clustered = model.transform(valid)

        # ------------------------------------------
        # 8. SELECT OUTPUT COLUMNS
        # ------------------------------------------

        identifying_columns = [
            col for col in [
                "Booking_ID",
                "Date",
                "Time",
                "Booking_Status",
                "Vehicle Type",
                "Pickup_Location",
                "Drop_Location",
            ]
            if col in clustered.columns
        ]

        selected = clustered.select(
            *identifying_columns,
            *FEATURES,
            "Cluster"
        )


        print("\nSaving cluster assignments...")

        with open(
            OUTPUT_FILE, "w", newline="", encoding="utf-8"
        ) as file:

            writer = csv.writer(file)
            writer.writerow(selected.columns)

            for row in selected.toLocalIterator():
                writer.writerow(list(row))

        print("Cluster CSV saved:", OUTPUT_FILE)

        # ------------------------------------------
        # 10. CLUSTER SUMMARY
        # ------------------------------------------

        summary = (
            clustered.groupBy("Cluster")
            .agg(
                F.count("*").alias("Ride_Count"),
                F.round(
                    F.avg("Ride_Distance"), 2
                ).alias("Avg_Distance"),
                F.round(
                    F.avg("Booking_Value"), 2
                ).alias("Avg_Booking_Value"),
                F.round(
                    F.avg("Avg_VTAT"), 2
                ).alias("Avg_VTAT"),
                F.round(
                    F.avg("Avg_CTAT"), 2
                ).alias("Avg_CTAT"),
            )
            .orderBy("Cluster")
        )

        print("\n========== CLUSTER SUMMARY ==========")
        summary.show(truncate=False)

        # ------------------------------------------
        # 11. MODEL EVALUATION
        # ------------------------------------------

        fitted_kmeans = model.stages[-1]
        print(
            "K-Means training cost (within-cluster SSE):",
            fitted_kmeans.summary.trainingCost
        )

        print("\nClustering completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
