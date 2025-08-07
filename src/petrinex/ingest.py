import os
import requests
import zipfile
import shutil
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import tempfile
from io import BytesIO
from typing import List

logger = logging.getLogger(__name__)


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
                            logger.debug(f"Extracted {csv_name}")
            elif inner_name.lower().endswith(".csv"):
                # (rare) CSV directly in outer ZIP
                out_csv_path = os.path.join(download_dir, inner_name)
                with open(out_csv_path, "wb") as f:
                    f.write(inner_bytes)
                logger.debug(f"Extracted {inner_name}")
            else:
                # unexpected file type – skip
                continue

    # Clean up temp outer ZIP
    os.remove(outer_zip_path)


def _generate_month_list(start_month: str, end_month: str) -> List[str]:
    """Generate list of months from start_month to end_month inclusive."""
    start_dt = datetime.strptime(start_month, "%Y-%m")
    end_dt = datetime.strptime(end_month, "%Y-%m")
    current = start_dt
    months_list = []
    while current <= end_dt:
        months_list.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)
    return months_list


def download_petrinex_data(config, dataset) -> str:
    """
    Download Petrinex data for the specified dataset and date range.

    Args:
        config: Main configuration object
        dataset: Dataset-specific configuration

    Returns:
        Path to directory containing downloaded CSV files
    """
    # Setup download directory
    download_dir = os.path.join(config.ingest.download_dir, dataset.download_subdir)

    # Prepare download directory (clear it if it exists)
    if os.path.exists(download_dir):
        logger.info(f"Clearing existing download directory: {download_dir}")
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)
    logger.info(f"Created download directory: {download_dir}")

    # Generate list of months to download
    start_month = config.ingest.start_month_yyyy_mm
    end_month = config.ingest.effective_end_month
    months_list = _generate_month_list(start_month, end_month)

    logger.info(
        f"Downloading {dataset.name} data for {len(months_list)} months: "
        f"{months_list[0]} to {months_list[-1]}"
    )

    # Download each month
    successful_downloads = 0
    for month in months_list:
        url = f"{config.ingest.api_base_url}/{dataset.code}/{month}/CSV"

        try:
            logger.debug(f"Downloading {dataset.code} data for {month} from {url}")
            response = requests.get(url, timeout=config.ingest.timeout_seconds)

            if response.status_code != 200:
                logger.warning(
                    f"No data available for {month} (HTTP {response.status_code}) – skipping"
                )
                continue

            logger.warning(f"Downloaded {dataset.code} {month} – extracting files")
            _extract_all_csvs(response.content, download_dir)
            successful_downloads += 1

        except requests.RequestException as e:
            logger.error(f"Failed to download data for {month}: {e}")
            continue

    logger.info(
        f"Successfully downloaded and extracted data for {successful_downloads}/{len(months_list)} months"
    )
    return download_dir
