import click


@click.command()
@click.option(
    "--dest_path",
    help="Location where the resulting files will be saved",
)
@click.option(
    "--dataset",
    type=click.Choice(["green", "yellow"], case_sensitive=False),
    default="green",
    help="Dataset to download. Options are green or yellow",
)
def main(dest_path: str, dataset: str = "green"):
    # Load parquet files
    print(f"{dataset}")


if __name__ == "__main__":
    main()
