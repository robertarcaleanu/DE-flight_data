import polars as pl

def load_parquet(data: pl.DataFrame, path: str) -> None:
    """This function loads the transformed data into a CSV.

    Args:
        data (pl.DataFrame): dataframe to save
        path (str): path to save
    """

    data.write_parquet(path)