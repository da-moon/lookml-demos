# Data Downloader

## Overview

The python script in this repo downloads
[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
Parquet files

## Usage

- Create the virtual environment if it has not been created already

```bash
python3 -m venv .venv
```

- Activate the virtual environment

```bash
source ".venv/bin/activate"
```

- install dependant packages

```bash
python3 -m pip install -r requirements.txt
```

- Options

```console
❯ python3 data_downloader.py --help
Usage: data_downloader.py [OPTIONS]

  A script that helps with downloading Parquet files of taxi trip data.

  Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

  Example:

  python3 data_downloader.py -sy 2009 -sm 1 -ey 2009 -em 1

Options:
  -d, --dir TEXT                  Directory to store downloaded data.
                                  [default: /workspace/lookml-ny-taxi/data]
  -ds, --dataset [green|yellow|fhvhv|fhv]
                                  Dataset to download. Some datasets are not
                                  available in earlier years  [default:
                                  yellow]
  -sy, --start_year INTEGER       Starting year of the dataset  [default:
                                  2009]
  -sm, --start_month INTEGER      Starting month of the dataset  [default: 1]
  -ey, --end_year INTEGER         End year of the dataset  [default: 2024]
  -em, --end_month INTEGER        End month of the dataset  [default: 12]
  --dryrun                        Dry run. Does not download any files
  --help                          Show this message and exit.
```
