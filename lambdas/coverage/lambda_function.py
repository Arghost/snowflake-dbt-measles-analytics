import os
from utils import today_str, download_csv, upload_to_s3

BUCKET = os.environ["MEASLES_BUCKET"]
COVERAGE_URL_MCV1 = os.environ["COVERAGE_URL_MCV1"]
COVERAGE_URL_MCV2 = os.environ["COVERAGE_URL_MCV2"]


def lambda_handler(event, context):
    run_date = today_str()

    # 1) Descargar MCV1
    data_mcv1 = download_csv(COVERAGE_URL_MCV1)
    key_mcv1 = f"coverage/run_date={run_date}/measles_coverage_mcv1.csv"
    upload_to_s3(BUCKET, key_mcv1, data_mcv1)

    # 2) Descargar MCV2
    data_mcv2 = download_csv(COVERAGE_URL_MCV2)
    key_mcv2 = f"coverage/run_date={run_date}/measles_coverage_mcv2.csv"
    upload_to_s3(BUCKET, key_mcv2, data_mcv2)

    return {
        "status": "ok",
        "bucket": BUCKET,
        "run_date": run_date,
        "coverage_keys": [key_mcv1, key_mcv2],
    }