import os
import argparse

from datetime import timedelta
from time import time

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(csv_url):

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if csv_url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {csv_url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin({0}).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin({0}).sum()}")
    return df


@task(log_prints=True, retries=3)
def ingest_data(table_name, data):
    connection_block = SqlAlchemyConnector.load('postgres-connector')
    with connection_block.get_connection(begin=False) as engine:
        if not database_exists(engine.url):
            create_database(engine.url)
        data.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        data.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name='Subflow', log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

@flow(name='Ingest Flow')
def main_flow(table_name):
    csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)


if __name__ == '__main__':
    main_flow('yellow_taxi_data')