import pandas as pd
import boto3
import os
import zipfile
from io import StringIO


"""Unlock files from zip"""
zip_path = r"D:\VS Code Data\AWS\archive.zip"
with zipfile.ZipFile(zip_path, "r") as z:

    print(z.namelist())

    with z.open("custom_2017_2020.csv") as file:
        df = pd.read_csv(file)

creds = pd.read_csv("./Credentials/vivek-ml_accessKeys.csv")

s3 = boto3.client("s3")

region_name = "us-east-1"

s3 = boto3.resource(
    service_name="s3",
    region_name=region_name,
    aws_access_key_id=creds.iloc[0, 0],
    aws_secret_access_key=creds.iloc[0, 1],
)

"""In Python, os.environ is used to access and manage environment variables. 
When working with AWS credentials, os.environ can be useful for retrieving 
or setting the necessary environment variables, such as AWS access keys, 
secret keys, and region names, to allow your Python application to 
authenticate with AWS services."""

os.environ["AWS_ACCESS_KEY_ID"] = creds.iloc[0, 0]
os.environ["AWS_SECRET_ACCESS_KEY"] = creds.iloc[0, 1]
os.environ["AWS_REGION"] = region_name

"""Print all buckets"""
for bucket in s3.buckets.all():
    print(bucket)

"""print all the objects inside a bucket"""
for obj in s3.Bucket("learntest5").objects.all():
    print(obj)


"""Upload file to s3 bucket"""
s3.Bucket("learntest5").upload_file(Filename="./Data/hs9_jpn.csv", Key="hs9_jpn.csv")

"""Download file from s3 bucket"""
s3.Bucket("learntest5").download_file(Key="hs9_jpn.csv", Filename="hs9_jpn_s3.csv")


"""Load csv file directly into python, this applies to all 
formats like parquet, feather, excel etc"""
obj = s3.Bucket("learntest5").Object("hs9_jpn.csv").get()
df = pd.read_csv(obj["Body"], index_col=0)

# Directly read csv with the path
test = pd.read_csv("s3://learntest5/hs9_jpn.csv", index_col=0)

"""Write dataframe from python to s3, applies to all formats 
like parquet, csv, feather, excel etc"""
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
s3.Bucket("learntest5").put_object(Key="hs9_jpn_df.csv", Body=csv_buffer.getvalue())
