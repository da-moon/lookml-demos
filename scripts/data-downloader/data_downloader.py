import os, click, requests functools, pathlib, shutil, time, csv
from typing import Tuple
from tqdm.auto import tqdm


def download_parquet(
    data_dir: str
    == os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "data", "parquet")
    ),
    prefix: str = "yellow",
    year: int = 2024,
    month: int = 1,
    dryrun: bool = False,
) -> Tuple[str, str]:
    """
    download_parquet downloads the trip data and stores it in the specified directory.

    :param data_dir: directory to store parquet dataset
    :param prefix: Taxi type. The value is either "green" or "yellow".
    :param year: Year of the dataset
    :param month: Month of the dataset
    :param dryrun: dryrun. Setting it to true will cause the function to not download any files
    :return: a tuple with  [url of the downloaded file,path of the downloaded file]
    """
    filename = os.path.abspath(
        os.path.join(data_dir, f"{prefix}_tripdata_{year}-{month:02d}.parquet"),
    )
    path = pathlib.Path(filename).expanduser().resolve()
    if os.path.exists(path):
        print(f"File {filename} exists, skipping")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{prefix}_tripdata_{year}-{month:02d}.parquet"
    if not dryrun:
        print(f"Downloading {url}")
        r = requests.get(url, stream=True, allow_redirects=True)
        if r.status_code == 403:
            print("rate limit detected, sleeping for 10 minutes")
            time.sleep(600)
            download_parquet(data_dir, prefix, year, month)
            return
        elif r.status_code != 200:
            r.raise_for_status()
            raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
        file_size = int(r.headers.get("Content-Length", 0))
        desc = "(Unknown total file size)" if file_size == 0 else ""
        r.raw.read = functools.partial(r.raw.read, decode_content=True)
        with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
            with path.open("wb") as f:
                shutil.copyfileobj(r_raw, f)

    return url, path


@click.command()
@click.option(
    "-d",
    "--dir",
    default=os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "data")
    ),
    help="Directory to store downloaded data.",
)
@click.option(
    "-ds",
    "--dataset",
    type=click.Choice(["green", "yellow", "fhvhv", "fhv"], case_sensitive=False),
    default="yellow",
    help="Dataset to download. Some datasets are not available in earlier years",
)
@click.option(
    "-sy",
    "--start_year",
    type=int,
    default=2009,
    help="Starting year of the dataset",
)
@click.option(
    "-sm",
    "--start_month",
    type=int,
    default=1,
    help="Starting month of the dataset",
)
@click.option(
    "-ey",
    "--end_year",
    type=int,
    default=2024,
    help="End year of the dataset",
)
@click.option(
    "-em",
    "--end_month",
    type=int,
    default=12,
    help="End month of the dataset",
)
@click.option(
    "--dryrun",
    type=bool,
    is_flag=True,
    default=False,
    help="Dry run. Does not download any files",
)
def main(
    dir: str,
    dataset: str,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    dryrun: bool,
):
    """
    A script that helps with downloading Parquet files of taxi trip data.

    Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

    Example:

    python3 data_downloader.py -sy 2009 -sm 1 -ey 2009 -em 1
    """
    fields = ["url", "path", "year", "month"]
    summary_dict = []

    parquet_dir = os.path.join(dir, "parquet")
    # Download parquet files
    for year in range(start_year, end_year + 1):
        for month in range(start_month, end_month + 1):
            url, path = download_parquet(
                data_dir=parquet_dir,
                prefix=dataset,
                year=year,
                month=month,
                dryrun=dryrun,
            )
            summary_dict.append(
                {"url": url, "path": str(path), "year": str(year), "month": str(month)}
            )
    csv_path = os.path.join(dir, "summary.csv")
    csv_path = pathlib.Path(csv_path).expanduser().resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summary_dict)
    print(f"Summary : {csv_path}")


if __name__ == "__main__":
    main()
