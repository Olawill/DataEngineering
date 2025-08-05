import io
import os
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs
1. Install pandas, pyarrow and google-cloud-storage
2. Set GOOGLE_APPLICATION_CREDENTIAL to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "terraform-demo-467020-ny-taxi")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS", "../terraform/keys/my_creds.json"
)
# terraform-demo-467020

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        parquet_file = file_name.replace('.csv.gz', '.parquet')

        try:
          # download it using requests via a pandas df
          request_url = f"{init_url}{service}/{file_name}"
          r = requests.get(request_url)
          r.raise_for_status()  # Raises an error for bad responses
          
          with open(file_name, 'wb') as f:
            f.write(r.content)
          # open(file_name, 'wb').write(r.content)
          print(f"Local: {file_name}")

          # read it back into a parquet file
          df = pd.read_csv(file_name, compression='gzip')
          # file_name = file_name.replace('.csv.gz', '.parquet')
          df.to_parquet(parquet_file, engine='pyarrow')
          print(f"Parquet: {parquet_file}")

          # upload it to gcs 
          upload_to_gcs(BUCKET, f"{service}/{parquet_file}", parquet_file)
          print(f"GCS: {service}/{parquet_file}")
        finally:
          # Cleanup both files
          for f in [file_name, parquet_file]:
            if os.path.exists(f):
              os.remove(f)
              print(f"Deleted local file: {f}")

if __name__ == "__main__":
  for taxi in ['fhv']:
    for year in ['2019', '2020']:
      web_to_gcs(year, taxi)
    