# lambdas/cases/lambda_function.py
import os
from utils import today_str, download_csv, upload_to_s3

BUCKET = os.environ["MEASLES_BUCKET"]
CASES_URL = os.environ["CASES_URL"]

def lambda_handler(event, context):
    run_date = today_str()
    key = f"cases/measles_cases_{run_date}.csv"

    data = download_csv(CASES_URL)
    upload_to_s3(BUCKET, key, data)

    return {
        "status": "ok",
        "bucket": BUCKET,
        "run_date": run_date,
        "cases_key": key,
    }