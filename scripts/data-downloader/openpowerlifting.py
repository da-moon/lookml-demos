#!/usr/bin/env python3

import os
import click
import requests
import functools
import pathlib
import shutil
import time
import csv
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Tuple
from tqdm.auto import tqdm

# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def download_and_convert_powerlifting(
    data_dir: str,
    output_format: str = "parquet",
    dryrun: bool = False,
    skip_download: bool = False,
    keep_csv: bool = False,
    no_cleanup: bool = False
) -> Tuple[str, str]:
    """
    Download the Open Powerlifting dataset, convert it to the specified format, and store it in the specified directory.

    :param data_dir: directory to store dataset
    :param output_format: Output format (parquet or csv)
    :param dryrun: dryrun. Setting it to true will cause the function to not download or convert any files
    :param skip_download: Skip download if the file already exists
    :param keep_csv: Keep the original CSV file after conversion
    :param no_cleanup: Do not remove intermediate files and folders
    :return: a tuple with [url of the downloaded file, path of the converted file]
    """
    base_filename = os.path.abspath(
        os.path.join(data_dir, f"openpowerlifting-latest")
    )
    parquet_path = pathlib.Path(f"{base_filename}.parquet").expanduser().resolve()
    csv_path = pathlib.Path(f"{base_filename}.csv").expanduser().resolve()
    
    output_path = csv_path if output_format == 'csv' else parquet_path
    
    url = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"

    if os.path.exists(output_path):
        if skip_download:
            print(f"File {output_path} exists, skipping download and conversion")
            return url, str(output_path)
        else:
            print(f"File {output_path} exists, but will be overwritten")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if dryrun:
        print(f"Dry run: would download from {url} and save to {output_path}")
        return url, str(output_path)

    print(f"Downloading {url}")
    r = requests.get(url, stream=True, allow_redirects=True)
    if r.status_code != 200:
        r.raise_for_status()
        raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
    
    file_size = int(r.headers.get("Content-Length", 0))
    desc = "(Unknown total file size)" if file_size == 0 else ""
    r.raw.read = functools.partial(r.raw.read, decode_content=True)
    
    zip_path = output_path.with_suffix('.zip')
    with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
        with zip_path.open("wb") as f:
            shutil.copyfileobj(r_raw, f)
    
    print("Extracting CSV from zip file...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        csv_file = [f for f in zip_ref.namelist() if f.endswith('.csv')][0]
        zip_ref.extract(csv_file, output_path.parent)
    
    extracted_csv_path = output_path.parent / csv_file
    
    print(f"Converting to {output_format}...")
    df = pd.read_csv(extracted_csv_path, low_memory=False)
    
    # Convert date columns with explicit format
    print("Converting date columns...")
    print("  Converting 'Date' column...")
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce')
    print("  Converting 'BirthYearClass' column...")
    df['BirthYearClass'] = pd.to_datetime(df['BirthYearClass'], format='%Y', errors='coerce')
    
    # Convert numeric columns
    numeric_columns = [
        'Age', 'BodyweightKg', 'WeightClassKg', 
        'Squat1Kg', 'Squat2Kg', 'Squat3Kg', 'Squat4Kg', 'Best3SquatKg',
        'Bench1Kg', 'Bench2Kg', 'Bench3Kg', 'Bench4Kg', 'Best3BenchKg',
        'Deadlift1Kg', 'Deadlift2Kg', 'Deadlift3Kg', 'Deadlift4Kg', 'Best3DeadliftKg',
        'TotalKg', 'Dots', 'Wilks', 'Glossbrenner', 'Goodlift'
    ]
    print("Converting numeric columns...")
    for col in tqdm(numeric_columns, desc="Numeric columns"):
        if col in df.columns:
            print(f"  Converting '{col}' column...")
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert boolean columns
    boolean_columns = ['Tested', 'Sanctioned']
    print("Converting boolean columns...")
    for col in boolean_columns:
        if col in df.columns:
            print(f"  Converting '{col}' column...")
            df[col] = df[col].map({'Yes': True, 'No': False})
    
    print(f"Saving to {output_format} format...")
    if output_format == 'parquet':
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path)
        if keep_csv:
            df.to_csv(csv_path, index=False)
    else:  # csv
        df.to_csv(csv_path, index=False)
    
    # Clean up
    print("Cleaning up...")
    if os.path.exists(zip_path):
        os.remove(zip_path)
    if os.path.exists(extracted_csv_path) and extracted_csv_path != csv_path:
        os.remove(extracted_csv_path)
    if not no_cleanup:
        for item in os.listdir(data_dir):
            item_path = os.path.join(data_dir, item)
            if item_path not in [str(parquet_path), str(csv_path)]:
                if os.path.isfile(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
    print("Cleanup completed.")

    return url, str(output_path)


@click.command()
@click.option(
    "-d",
    "--dir",
    default=os.path.join(SCRIPT_DIR, "data"),
    show_default=True,
    help="Directory to store downloaded data.",
)
@click.option(
    "-f",
    "--format",
    type=click.Choice(["parquet", "csv"], case_sensitive=False),
    default="parquet",
    show_default=True,
    help="Output format for the dataset.",
)
@click.option(
    "--dryrun",
    is_flag=True,
    default=False,
    help="Dry run. Does not download or convert any files",
)
@click.option(
    "--skip-download",
    is_flag=True,
    default=False,
    help="Skip download if the file already exists",
)
@click.option(
    "--keep-csv",
    is_flag=True,
    default=False,
    help="Keep the original CSV file after conversion",
)
@click.option(
    "--no-cleanup",
    is_flag=True,
    default=False,
    help="Do not remove intermediate files and folders",
)
def main(dir: str, format: str, dryrun: bool, skip_download: bool, keep_csv: bool, no_cleanup: bool):
    """
    A script that helps with downloading and converting the Open Powerlifting dataset.

    Source: https://openpowerlifting.gitlab.io/
    """
    fields = ["url", "path"]
    summary_dict = []

    url, path = download_and_convert_powerlifting(
        data_dir=dir,
        output_format=format,
        dryrun=dryrun,
        skip_download=skip_download,
        keep_csv=keep_csv,
        no_cleanup=no_cleanup
    )
    if url and path:
        summary_dict.append({"url": url, "path": path})

    if no_cleanup:
        csv_path = os.path.join(dir, "summary.csv")
        csv_path = pathlib.Path(csv_path).expanduser().resolve()
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(summary_dict)
        print(f"Summary : {csv_path}")
    else:
        print(f"Final output file: {path}")
    
    if keep_csv and format == 'parquet':
        print(f"CSV file kept at: {os.path.splitext(path)[0]}.csv")


if __name__ == "__main__":
    main()
