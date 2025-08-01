import os
import requests
import zipfile
import shutil
from datetime import datetime
from dateutil.relativedelta import relativedelta
import tempfile
from io import BytesIO
from pyspark.sql.utils import AnalysisException


def _extract_all_csvs(zip_bytes: bytes, download_dir: str):
    """
    Extract outer ZIP bytes. If any entry ends with .csv.zip, open it as
    a second-level ZIP and extract the CSV(s) inside. All real CSV files
    end up directly in download_dir. Any leftover ZIPs are removed.
    """
    # 1️⃣ write outer ZIP to a temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_out:
        tmp_out.write(zip_bytes)
        outer_zip_path = tmp_out.name

    # 2️⃣ open outer ZIP
    with zipfile.ZipFile(outer_zip_path) as outer_zip:
        for inner_name in outer_zip.namelist():
            inner_bytes = outer_zip.read(inner_name)
            if inner_name.lower().endswith(".csv.zip"):
                # 3️⃣ nested ZIP – extract its CSV(s)
                with zipfile.ZipFile(BytesIO(inner_bytes)) as nested_zip:
                    for csv_name in nested_zip.namelist():
                        if csv_name.lower().endswith(".csv"):
                            csv_bytes = nested_zip.read(csv_name)
                            out_csv_path = os.path.join(download_dir, csv_name)
                            with open(out_csv_path, "wb") as f:
                                f.write(csv_bytes)
                            print(f"  └─ extracted {csv_name}")
            elif inner_name.lower().endswith(".csv"):
                # (rare) CSV directly in outer ZIP
                out_csv_path = os.path.join(download_dir, inner_name)
                with open(out_csv_path, "wb") as f:
                    f.write(inner_bytes)
                print(f"  └─ extracted {inner_name}")
            else:
                # unexpected file type – skip
                continue

    # Clean up temp outer ZIP
    os.remove(outer_zip_path)


def ingest_conventional_data(start_month: str, end_month: str):
    """Download Alberta Conventional Volumetric data for the given date range and write to Delta table."""
    dataset_code = "Vol"  # code for conventional volumetric in API URL
    download_dir = "/dbfs/tmp/petrinex_downloads/conventional"  # local directory for downloaded files

    # Prepare download directory (clear it if it exists)
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)

    # Generate list of months from start_month to end_month inclusive
    start_dt = datetime.strptime(start_month, "%Y-%m")
    end_dt = datetime.strptime(end_month, "%Y-%m")
    current = start_dt
    months_list = []
    while current <= end_dt:
        months_list.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)

    print(
        f"Downloading Conventional Volumetric data for {len(months_list)} months: {months_list[0]} to {months_list[-1]}"
    )
    # Loop over each month and download the CSV ZIP
    for month in months_list:
        url = f"https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/{dataset_code}/{month}/CSV"
        file_name = f"AB_{dataset_code}_{month}.zip"
        file_path = os.path.join(download_dir, file_name)
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(
                f"Warning: No data for {month} (HTTP {response.status_code}) – skipping."
            )
            continue

        print(f"Downloaded {dataset_code} {month} – unpacking …")
        _extract_all_csvs(response.content, download_dir)

    # Use Spark to read all extracted CSV files into a DataFrame
    # (All CSVs in download_dir correspond to the requested months)
    csv_path_pattern = "dbfs:/tmp/petrinex_downloads/conventional/*.CSV"
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("pathGlobFilter", "*.CSV")
        .csv(csv_path_pattern)
    )
    # Write the DataFrame to a Delta table (overwrite if exists, and update schema if needed)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    df.write.option("overwriteSchema", "true").mode("overwrite").format(
        "delta"
    ).saveAsTable(full_conventional_table_name)
    print(
        f"Conventional Volumetric data successfully written to {full_conventional_table_name} (Rows: {df.count()})"
    )


def ingest_ngl_data(start_month: str, end_month: str):
    """Download Alberta NGL & Marketable Gas Volumes data for the given date range and write to Delta table."""
    dataset_code = "NGL"  # code for NGL/Marketable Gas in API URL
    download_dir = (
        "/dbfs/tmp/petrinex_downloads/ngl"  # local directory for downloaded files
    )

    # Prepare download directory (clear it if it exists)
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)

    # Generate list of months from start_month to end_month inclusive
    start_dt = datetime.strptime(start_month, "%Y-%m")
    end_dt = datetime.strptime(end_month, "%Y-%m")
    current = start_dt
    months_list = []
    while current <= end_dt:
        months_list.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)

    print(
        f"Downloading NGL & Marketable Gas data for {len(months_list)} months: {months_list[0]} to {months_list[-1]}"
    )
    for month in months_list:
        url = f"https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/{dataset_code}/{month}/CSV"
        file_name = f"AB_{dataset_code}_{month}.zip"
        file_path = os.path.join(download_dir, file_name)
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(
                f"Warning: No data for {month} (HTTP {response.status_code}) – skipping."
            )
            continue

        print(f"Downloaded {dataset_code} {month} – unpacking …")
        _extract_all_csvs(response.content, download_dir)

    # Read all CSVs into Spark DataFrame
    csv_path_pattern = "dbfs:/tmp/petrinex_downloads/ngl/*.CSV"
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("pathGlobFilter", "*.CSV")
        .csv(csv_path_pattern)
    )
    # Write to Delta table
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    df.write.option("overwriteSchema", "true").mode("overwrite").format(
        "delta"
    ).saveAsTable(full_ngl_table_name)
    print(
        f"NGL & Marketable Gas data successfully written to {full_ngl_table_name} (Rows: {df.count()})"
    )
