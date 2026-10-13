
# import csv
# from pathlib import Path
# from collections import Counter
# from django.shortcuts import render
# from django.http import JsonResponse

# BASE_DIR = Path(__file__).resolve().parents[1]
# RESULTS_DIR = BASE_DIR / "results"
# CLEAN_CSV = RESULTS_DIR / "cleaned_rides.csv"


# def read_csv(filename):
#     path = RESULTS_DIR / filename

#     if not path.exists():
#         return []

#     with open(path, newline="", encoding="utf-8-sig") as file:
#         return list(csv.DictReader(file))


# def number(value):
#     try:
#         if value is None or str(value).strip().lower() in {
#             "", "none", "null", "nan"
#         }:
#             return None
#         return float(value)
#     except (ValueError, TypeError):
#         return None


# def filter_rows(rows, params):
#     filters = {
#         "Date": params.get("date"),
#         "Booking_Status": params.get("status"),
#         "Vehicle Type": params.get("vehicle"),
#         "Payment_Method": params.get("payment"),
#         "Pickup_Location": params.get("pickup"),
#         "Drop_Location": params.get("drop"),
#     }

#     result = rows

#     for column, selected in filters.items():
#         if selected:
#             result = [
#                 row for row in result
#                 if str(row.get(column, "")).strip().lower()
#                 == selected.strip().lower()
#             ]

#     selected_hour = params.get("hour")
#     if selected_hour:
#         result = [
#             row for row in result
#             if str(row.get("Hour", "")).strip() == selected_hour
#         ]

#     return result


# def build_kpis(rows):
#     total = len(rows)
#     successful = [
#         row for row in rows
#         if (row.get("Booking_Status") or "").lower() == "success"
#     ]

#     values = [
#         number(row.get("Booking_Value")) for row in successful
#     ]
#     values = [v for v in values if v is not None]

#     distances = [
#         number(row.get("Ride_Distance")) for row in successful
#     ]
#     distances = [v for v in distances if v is not None]

#     driver_ratings = [
#         number(row.get("Driver_Ratings")) for row in successful
#     ]
#     driver_ratings = [v for v in driver_ratings if v is not None]

#     customer_ratings = [
#         number(row.get("Customer_Rating")) for row in successful
#     ]
#     customer_ratings = [v for v in customer_ratings if v is not None]

#     return {
#         "Total Bookings": total,
#         "Successful Bookings": len(successful),
#         "Success Rate": (
#             round(len(successful) * 100 / total, 2) if total else 0
#         ),
#         "Successful Booking Value": round(sum(values), 2),
#         "Average Booking Value": (
#             round(sum(values) / len(values), 2) if values else 0
#         ),
#         "Average Ride Distance": (
#             round(sum(distances) / len(distances), 2)
#             if distances else 0
#         ),
#         "Average Driver Rating": (
#             round(sum(driver_ratings) / len(driver_ratings), 2)
#             if driver_ratings else 0
#         ),
#         "Average Customer Rating": (
#             round(sum(customer_ratings) / len(customer_ratings), 2)
#             if customer_ratings else 0
#         ),
#     }


# def dashboard(request):
#     all_rows = read_csv("cleaned_rides.csv")
#     rows = filter_rows(all_rows, request.GET)

#     context = {
#         "kpis": build_kpis(rows),
#         "filters": request.GET,
#         "dates": sorted({
#             r.get("Date", "") for r in all_rows if r.get("Date")
#         }),
#         "statuses": sorted({
#             r.get("Booking_Status", "")
#             for r in all_rows if r.get("Booking_Status")
#         }),
#         "vehicles": sorted({
#             r.get("Vehicle Type", "")
#             for r in all_rows if r.get("Vehicle Type")
#         }),
#         "payments": sorted({
#             r.get("Payment_Method", "")
#             for r in all_rows if r.get("Payment_Method")
#         }),
#         "pickups": sorted({
#             r.get("Pickup_Location", "")
#             for r in all_rows if r.get("Pickup_Location")
#         }),
#         "drops": sorted({
#             r.get("Drop_Location", "")
#             for r in all_rows if r.get("Drop_Location")
#         }),
#         "status_rows": [
#             {"label": k, "value": v}
#             for k, v in Counter(
#                 r.get("Booking_Status") or "Unknown" for r in rows
#             ).items()
#         ],
#         "payment_rows": [
#             {"label": k, "value": v}
#             for k, v in Counter(
#                 r.get("Payment_Method") or "Unknown"
#                 for r in rows
#                 if (r.get("Booking_Status") or "").lower() == "success"
#             ).items()
#         ],
#         "hourly_rows": [
#             {"label": k, "value": v}
#             for k, v in sorted(Counter(
#                 r.get("Hour") or "Unknown" for r in rows
#             ).items())
#         ],
#         "pickup_rows": [
#             {"label": k, "value": v}
#             for k, v in Counter(
#                 r.get("Pickup_Location") or "Unknown" for r in rows
#             ).most_common(10)
#         ],
#         "vehicle_rows": [
#             {
#                 "vehicle": vehicle,
#                 "status": status,
#                 "count": count,
#             }
#             for (vehicle, status), count in Counter(
#                 (
#                     r.get("Vehicle Type") or "Unknown",
#                     r.get("Booking_Status") or "Unknown",
#                 )
#                 for r in rows
#             ).items()
#         ],
#     }

#     return render(request, "index.html", context)


# def chart_data(request):
#     all_rows = read_csv("cleaned_rides.csv")
#     rows = filter_rows(all_rows, request.GET)

#     return JsonResponse({
#         "kpis": build_kpis(rows),
#         "total_filtered_rows": len(rows),
#         "status": dict(Counter(
#             r.get("Booking_Status") or "Unknown" for r in rows
#         )),
#     })


























import csv
from pathlib import Path
from collections import Counter

from django.shortcuts import render


# Project paths
BASE_DIR = Path(__file__).resolve().parents[1]
RESULTS_DIR = BASE_DIR / "results"

CLEAN_FILE = RESULTS_DIR / "cleaned_rides.csv"
BENCHMARK_FILE = RESULTS_DIR / "benchmark_results.csv"
CLUSTER_FILE = RESULTS_DIR / "ride_clusters.csv"


# --------------------------------------------------
# CSV HELPERS
# --------------------------------------------------

def read_csv_file(path):
    """Read a CSV file and return a list of dictionaries."""
    if not path.exists():
        return []

    try:
        with open(path, newline="", encoding="utf-8-sig") as file:
            return list(csv.DictReader(file))
    except (OSError, csv.Error):
        return []


def numeric(value):
    """Convert a value to float; return None if invalid."""
    try:
        if value is None:
            return None

        text = str(value).strip()

        if text.lower() in {"", "null", "none", "nan", "n/a"}:
            return None

        result = float(text)

        # Ignore invalid infinite values.
        if result != result or abs(result) == float("inf"):
            return None

        return result

    except (ValueError, TypeError):
        return None


def same(value1, value2):
    """Case-insensitive comparison for filter values."""
    return str(value1 or "").strip().casefold() == str(
        value2 or ""
    ).strip().casefold()


def chart_rows(counter):
    """Convert Counter/dictionary data into Chart.js-friendly rows."""
    return [
        {"label": str(key), "value": value}
        for key, value in counter.items()
    ]


# --------------------------------------------------
# DYNAMIC FILTERS
# --------------------------------------------------

def filter_rows(rows, params):
    filter_mapping = {
        "date": "Date",
        "status": "Booking_Status",
        "vehicle": "Vehicle Type",
        "payment": "Payment_Method",
        "pickup": "Pickup_Location",
        "drop": "Drop_Location",
    }

    filtered = rows

    for param, column in filter_mapping.items():
        selected = params.get(param, "").strip()

        if selected:
            filtered = [
                row for row in filtered
                if same(row.get(column), selected)
            ]

    selected_hour = params.get("hour", "").strip()

    if selected_hour:
        try:
            selected_hour = str(int(selected_hour))
            filtered = [
                row for row in filtered
                if numeric(row.get("Hour")) is not None
                and int(float(row["Hour"])) == int(selected_hour)
            ]
        except ValueError:
            filtered = []

    return filtered


# --------------------------------------------------
# KPI CALCULATIONS
# --------------------------------------------------

def calculate_kpis(rows):
    total = len(rows)

    successful = [
        row for row in rows
        if same(row.get("Booking_Status"), "Success")
    ]

    def get_values(column, source):
        return [
            value for row in source
            if (value := numeric(row.get(column))) is not None
        ]

    booking_values = get_values("Booking_Value", successful)
    distances = get_values("Ride_Distance", successful)
    driver_ratings = get_values("Driver_Ratings", successful)
    customer_ratings = get_values("Customer_Rating", successful)

    return {
        "Total Bookings": total,
        "Successful Bookings": len(successful),
        "Success Rate (%)": round(
            len(successful) * 100 / total, 2
        ) if total else 0,
        "Successful Booking Value": round(
            sum(booking_values), 2
        ),
        "Average Booking Value": round(
            sum(booking_values) / len(booking_values), 2
        ) if booking_values else 0,
        "Average Ride Distance": round(
            sum(distances) / len(distances), 2
        ) if distances else 0,
        "Average Driver Rating": round(
            sum(driver_ratings) / len(driver_ratings), 2
        ) if driver_ratings else 0,
        "Average Customer Rating": round(
            sum(customer_ratings) / len(customer_ratings), 2
        ) if customer_ratings else 0,
    }


# --------------------------------------------------
# BENCHMARK RESULTS
# Reads results/benchmark_results.csv
# --------------------------------------------------

def get_benchmark_data():
    rows = read_csv_file(BENCHMARK_FILE)
    result = []

    for row in rows:
        runtime = numeric(row.get("Runtime_Seconds"))

        if runtime is None:
            continue

        result.append({
            "tool": row.get("Tool") or "Unknown",
            "rows": row.get("Rows") or "-",
            "operation": row.get("Operation") or "-",
            "runtime": round(runtime, 4),
        })

    return result


# --------------------------------------------------
# K-MEANS CLUSTER SUMMARY
# Reads results/ride_clusters.csv
# --------------------------------------------------

def get_cluster_summary(params):
    rows = read_csv_file(CLUSTER_FILE)

    # Apply relevant dashboard filters to individual cluster records.
    rows = filter_rows(rows, params)

    grouped = {}

    for row in rows:
        cluster_id = row.get("Cluster")

        if cluster_id in (None, ""):
            continue

        if cluster_id not in grouped:
            grouped[cluster_id] = {
                "cluster": cluster_id,
                "ride_count": 0,
                "distance_sum": 0.0,
                "distance_count": 0,
                "value_sum": 0.0,
                "value_count": 0,
                "vtat_sum": 0.0,
                "vtat_count": 0,
                "ctat_sum": 0.0,
                "ctat_count": 0,
            }

        item = grouped[cluster_id]
        item["ride_count"] += 1

        fields = [
            ("Ride_Distance", "distance_sum", "distance_count"),
            ("Booking_Value", "value_sum", "value_count"),
            ("Avg_VTAT", "vtat_sum", "vtat_count"),
            ("Avg_CTAT", "ctat_sum", "ctat_count"),
        ]

        for column, sum_key, count_key in fields:
            value = numeric(row.get(column))

            if value is not None:
                item[sum_key] += value
                item[count_key] += 1

    summary = []

    for item in grouped.values():
        summary.append({
            "cluster": item["cluster"],
            "ride_count": item["ride_count"],
            "avg_distance": round(
                item["distance_sum"] / item["distance_count"], 2
            ) if item["distance_count"] else 0,
            "avg_booking_value": round(
                item["value_sum"] / item["value_count"], 2
            ) if item["value_count"] else 0,
            "avg_vtat": round(
                item["vtat_sum"] / item["vtat_count"], 2
            ) if item["vtat_count"] else 0,
            "avg_ctat": round(
                item["ctat_sum"] / item["ctat_count"], 2
            ) if item["ctat_count"] else 0,
        })

    return sorted(summary, key=lambda row: str(row["cluster"]))


# --------------------------------------------------
# MAIN DASHBOARD VIEW
# --------------------------------------------------

def dashboard(request):
    all_rows = read_csv_file(CLEAN_FILE)
    rows = filter_rows(all_rows, request.GET)

    # Booking status chart
    status_counts = Counter(
        row.get("Booking_Status") or "Unknown"
        for row in rows
    )

    # Successful rides only for payment analysis
    successful_rows = [
        row for row in rows
        if same(row.get("Booking_Status"), "Success")
    ]

    payment_counts = Counter(
        row.get("Payment_Method") or "Unknown"
        for row in successful_rows
    )

    # Hourly bookings
    hour_counts = Counter()

    for row in rows:
        hour = numeric(row.get("Hour"))

        if hour is not None and 0 <= hour <= 23:
            hour_counts[int(hour)] += 1

    hour_counts = dict(sorted(hour_counts.items()))

    # Top 10 pickup locations
    pickup_counts = Counter(
        row.get("Pickup_Location") or "Unknown"
        for row in rows
    )

    top_pickups = dict(pickup_counts.most_common(10))

    # Vehicle and status combinations
    vehicle_counts = Counter(
        (
            row.get("Vehicle Type") or "Unknown",
            row.get("Booking_Status") or "Unknown",
        )
        for row in rows
    )

    vehicle_rows = [
        {
            "vehicle": vehicle,
            "status": status,
            "count": count,
        }
        for (vehicle, status), count in sorted(vehicle_counts.items())
    ]

    # Populate dropdown options from the complete cleaned dataset.
    def unique_values(column):
        return sorted({
            str(row.get(column)).strip()
            for row in all_rows
            if row.get(column) not in (None, "")
        })

    context = {
        "kpis": calculate_kpis(rows),
        "filters": request.GET,

        "dates": unique_values("Date"),
        "statuses": unique_values("Booking_Status"),
        "vehicles": unique_values("Vehicle Type"),
        "payments": unique_values("Payment_Method"),
        "pickups": unique_values("Pickup_Location"),
        "drops": unique_values("Drop_Location"),

        "status_rows": chart_rows(status_counts),
        "payment_rows": chart_rows(payment_counts),
        "hourly_rows": chart_rows(hour_counts),
        "pickup_rows": chart_rows(top_pickups),
        "vehicle_rows": vehicle_rows,

        "filtered_count": len(rows),
        "data_available": CLEAN_FILE.exists(),

        "benchmark_rows": get_benchmark_data(),
        "cluster_rows": get_cluster_summary(request.GET),
    }

    return render(request, "index.html", context)
