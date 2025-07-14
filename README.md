# NYC Yellow Taxi Data Analysis using PySpark on Databricks

This project performs analytical queries on NYC Yellow Taxi trip data using Apache Spark (PySpark) within an Azure Databricks environment. The objective is to load, process, and analyze yellow cab datasets to derive business insights using real-world transportation data.

---

## 📂 Dataset Source

- **NYC Taxi & Limousine Commission (TLC)**  
- Downloaded from: [NYC TLC Trip Record Data](https://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
- Files used (in Parquet format):
  - `yellow_tripdata_2025_01.parquet`
  - `yellow_tripdata_2025_02.parquet`

---

## 🧱 Project Architecture

- **Environment**: Azure Databricks (Community/Enterprise)
- **Storage**: DBFS (`/FileStore/tables/`)
- **Engine**: Apache Spark (PySpark)
- **File Format**: Parquet

---

## 📌 Tasks Performed

1. Loaded two Yellow Taxi datasets into **DBFS** using PySpark.
2. Inspected schema, verified row counts, and displayed sample records.
3. Combined datasets using `unionByName()` into a single DataFrame.
4. Added a derived **`Revenue`** column.
5. Executed 7 business analytics queries on the dataset.

---

## 🔍 PySpark Queries

| Query No. | Description |
|-----------|-------------|
| Q1 | **Add Revenue Column**: Sum of `fare_amount`, `extra`, `mta_tax`, `improvement_surcharge`, `tip_amount`, `tolls_amount`, and `total_amount`. |
| Q2 | **Total Passengers by Area**: Group by `PULocationID` and sum of `passenger_count`. |
| Q3 | **Vendor-wise Average Fare and Total Earning** using `avg()` on `fare_amount` and `total_amount`. |
| Q4 | **Moving Count of Payments**: Rolling count of trips per `payment_type` using Spark window functions. |
| Q5 | **Top 2 Revenue-Earning Vendors on a Specific Date**: Aggregated metrics (revenue, passenger count, trip distance) by `VendorID`. |
| Q6 | **Route with Most Passengers**: Group by `PULocationID` and `DOLocationID`. |
| Q7 | **Top Pickup Locations in the Last Day**: Filter trips in the last 24 hours from latest timestamp and group by `PULocationID`. |

---

## 💻 Technologies Used

- **Apache Spark** (v3.x)
- **PySpark** (DataFrame API, SQL, Window Functions)
- **Azure Databricks**
- **DBFS (Databricks File System)**

---

## 📁 File Structure
- nyc-yellow-taxi-analytics/
- │
- ├── data/
- │ ├── yellow_tripdata_2025_01.parquet
- │ └── yellow_tripdata_2025_02.parquet
- │
- ├── notebooks/
- │ └── nyc_taxi_analysis_databricks.py # Exported Databricks notebook (optional)
- │
- ├── README.md

---

## ▶️ How to Run This Project

1. Upload the `.parquet` files to DBFS (`/FileStore/tables/`).
2. Open a Databricks workspace and create a new Python notebook.
3. Copy and paste the code cells from the notebook source (see `notebooks/nyc_taxi_analysis_databricks.py`).
4. Run the cells in sequence to execute data loading, processing, and analysis.

---
