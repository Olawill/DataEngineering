#!/usr/bin/env python
# coding: utf-8

import os
import gzip
import shutil
import argparse

from time import time

from sqlalchemy import create_engine
import pandas as pd

# Step 2: Detect if file is GZIP compressed
def is_gzip_file(filepath):
    with open(filepath, 'rb') as f:
        return f.read(2) == b'\x1f\x8b'  # Magic number for GZIP

def main(params):
  user = params.user
  password = params.password
  host = params.host
  port = params.port
  db = params.db
  table_name = params.table_name
  url = params.url
  
  # download csv
  csv_name = 'output.csv'
  temp_file = "temp_download"
  
  os.system(f"wget {url} -O {temp_file}")
  
  print('Downloaded file')
  
  # Step 2: Determine file type and handle accordingly
  if is_gzip_file(temp_file):
    with gzip.open(temp_file, 'rb') as f_in:
        with open(csv_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(temp_file)
    print(f"Decompressed and saved as {csv_name}")
  else:
    os.rename(temp_file, csv_name)
    print(f"Saved as {csv_name}")
  
  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


  df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)


  df = next(df_iter)


  df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
  df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

  df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

  df.to_sql(name=table_name, con=engine, if_exists='append')

  while True:
    try:
      t_start = time()

      df = next(df_iter)

      df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
      df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

      df.to_sql(name=table_name, con=engine, if_exists='append')

      t_end = time()

      print('inserted another chunk..., took %.3f second(s)' % (t_end - t_start))
    except StopIteration:
      print("Finished ingesting data into the postgres database")
      break

if __name__ == "__main__":

  parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

  parser.add_argument("--user", help="user name for postgres")
  parser.add_argument("--password", help="password for postgres")
  parser.add_argument("--host", help="host for postgres")
  parser.add_argument("--port", help="port for postgres")
  parser.add_argument("--db", help="database name for postgres")
  parser.add_argument("--table_name", help="name of the table where we will write the results to")
  parser.add_argument("--url", help="url of the csv file")

  args = parser.parse_args()

  main(args)