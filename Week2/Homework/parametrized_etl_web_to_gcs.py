from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1)
)
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Read taxi data from web into pandas DataFrame
    """
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix dtype issues
    """
    for col_name in df.columns:
        if 'datetime' in col_name:
            df[col_name] = pd.to_datetime(df[col_name])
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df



@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Write DataFrame out locally as parquet file.
    """
    path = Path(f'data/{color}/{dataset_file}.parquet')
    path.parent.mkdir(exist_ok=True, parents=True)
    df.to_parquet(path, compression='gzip')
    return path


@task()
def write_gcs(path) -> None:
    """
    Upload local parquet file to GCS.
    """
    gcp_block = GcsBucket.load("zoom-gcs")
    gcp_block.upload_from_path(
        from_path=path,
        to_path=path
    )


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """
    The main ETL function
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_web_to_gcs_parent_flow(
    months: list[int] = [1, 2],
    year: int = 2021,
    color: str = 'yellow'
) -> None:
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == '__main__':
    months = [1]
    year = 2020
    color = 'green'
    etl_parent_flow(months, year, color)