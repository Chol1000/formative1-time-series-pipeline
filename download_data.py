"""
download_data.py — Automatically downloads the UCI Household Electric Power
Consumption dataset required to run this pipeline.

Usage:
    python download_data.py

The dataset (~127 MB, ~2 million rows) is downloaded from the UCI ML Repository
and saved to the project root as household_power_consumption.txt.
"""

import sys
import urllib.request
import zipfile
from pathlib import Path

DATASET_URL = (
    "https://archive.ics.uci.edu/ml/machine-learning-databases/00235/"
    "household_power_consumption.zip"
)
OUTPUT_DIR  = Path(__file__).resolve().parent
OUTPUT_FILE = OUTPUT_DIR / "household_power_consumption.txt"
ZIP_FILE    = OUTPUT_DIR / "household_power_consumption.zip"


def _progress(block_num: int, block_size: int, total_size: int) -> None:
    downloaded = block_num * block_size
    if total_size > 0:
        pct = min(100, downloaded * 100 / total_size)
        mb  = downloaded / 1_048_576
        total_mb = total_size / 1_048_576
        print(f"\r  Downloading ... {pct:5.1f}%  ({mb:.1f} / {total_mb:.1f} MB)", end="", flush=True)


def main() -> None:
    if OUTPUT_FILE.exists():
        print(f"Dataset already present: {OUTPUT_FILE}")
        print("Nothing to do.")
        return

    print("=" * 60)
    print("  UCI Household Power Consumption — Dataset Downloader")
    print("=" * 60)
    print(f"  Source : {DATASET_URL}")
    print(f"  Target : {OUTPUT_FILE}")
    print()

    # Download the zip
    print("  Step 1/2 — Downloading archive ...")
    try:
        urllib.request.urlretrieve(DATASET_URL, ZIP_FILE, reporthook=_progress)
        print()  # newline after progress bar
    except Exception as exc:
        print(f"\nDownload failed: {exc}")
        print(
            "\nManual alternative:"
            "\n  1. Go to: https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set"
            "\n  2. Download and extract the zip"
            f"\n  3. Place household_power_consumption.txt in: {OUTPUT_DIR}"
        )
        sys.exit(1)

    # Extract
    print("  Step 2/2 — Extracting ...")
    try:
        with zipfile.ZipFile(ZIP_FILE, "r") as z:
            # The zip contains household_power_consumption.txt
            z.extractall(OUTPUT_DIR)
        ZIP_FILE.unlink()  # remove the zip after extraction
        print(f"  Extracted to: {OUTPUT_FILE}")
    except Exception as exc:
        print(f"Extraction failed: {exc}")
        sys.exit(1)

    if not OUTPUT_FILE.exists():
        print("Expected file not found after extraction. Check the zip contents.")
        sys.exit(1)

    size_mb = OUTPUT_FILE.stat().st_size / 1_048_576
    print()
    print("=" * 60)
    print(f"Dataset ready  ({size_mb:.1f} MB)")
    print(f"Location: {OUTPUT_FILE}")
    print()
    print("  You can now run the pipeline:")
    print("    Task 1 : open task1_eda/task1_notebook.ipynb")
    print("    Task 2 : python task2_databases/task2_main.py")
    print("    Task 3 : python task3_api/api.py")
    print("    Task 4 : python task4_prediction/prediction_script.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
