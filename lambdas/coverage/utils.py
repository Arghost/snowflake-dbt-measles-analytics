# File with commong code used for lambdas.
import datetime
import urllib.request
from typing import Tuple
import boto3

s3 = boto3.client("s3")


def today_str() -> str:
    """Return today's date as YYYYMMDD in UTC."""
    return datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")


def download_csv(url: str) -> bytes:
    """Download a CSV file from a public URL and return its raw bytes."""
    with urllib.request.urlopen(url) as resp:
        return resp.read()


def upload_to_s3(bucket: str, key: str, data: bytes) -> Tuple[str, str]:
    """Upload raw bytes to S3 and return (bucket, key)."""
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    return bucket, key