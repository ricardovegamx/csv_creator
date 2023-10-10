import csv
import io
import json
import logging
import random
import uuid
from datetime import datetime

import boto3
from cerberus import Validator
from faker import Faker

logger = logging.getLogger()
logger.setLevel(logging.INFO)
fake = Faker()

bucket_name = "raw-csv-public-bucket"


def get_max_account_numbers(amount: int):
    max_account_numbers = 50000
    if amount > max_account_numbers:
        return (amount % max_account_numbers) + max_account_numbers

    return max_account_numbers


def csv_creator(
    amount: int = 1,
    rows_min: int = 10,
    rows_max: int = 300,
    min_transaction_amount: float = -20000.00,
    max_transaction_amount: float = 20000.00,
):
    logger.info("started new csv_creator process…")

    # define the headers of csv file
    csv_headers = [
        "ID",
        "Date",
        "Transaction",
        "Transaction ID",
    ]

    # variables needed for csv generation
    generated_files = 0  # amount of files actually generated (vs requested)
    current_year = datetime.now().year  # the generated files will start from current year
    start_date = datetime(current_year, 1, 1, 0, 0, 0)  # january of current year
    end_date = datetime(current_year, 9, 30, 23, 59, 59)  # september of current year
    csv_files_urls = []  # urls of each generated files

    #  start generating the requested amount of csv files
    while generated_files < amount:
        csv_buffer = io.StringIO()  # the csv will be kept on memory
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(csv_headers)

        # the number of transactions according to provided (or default) input
        fake_rows_num = fake.random_int(rows_min, rows_max)
        csv_data = []
        generated_data_row = 0  # counter of transactions for the file

        # get a random "id_user" from 10000 to max available numbers (50000)
        id_user = fake.unique.random_int(10000, get_max_account_numbers(amount))
        prefix = "4242"  # add prefix so it doesn't start from 10001
        account_number = f"{prefix}{id_user}"

        # start generating rows of current file
        while generated_data_row < fake_rows_num:
            record_id = generated_data_row + 1
            record_date = fake.date_time_between(start_date=start_date, end_date=end_date)
            record_transaction_amount = round(
                random.uniform(min_transaction_amount, max_transaction_amount),
                2,
            )

            csv_data.append(
                [
                    record_id,
                    record_date.strftime("%Y-%m-%d %H:%M:%S"),
                    record_transaction_amount,
                    str(uuid.uuid4()),
                ]
            )

            generated_data_row += 1

        csv_writer.writerows(csv_data)
        logger.info("written transactions to csv file")

        # handle the upload of the file to S3 bucket
        s3 = boto3.client("s3")
        logger.info("started s3 service")

        try:
            key = f"{account_number}_transactions_report.csv"
            s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
            csv_buffer.close()  # close the file

            logger.info(f"saved object {key} to bucket {bucket_name}")
        except Exception as e:
            logger.error(f"unable to upload file: {e}")

        csv_files_urls.append(f"https://{bucket_name}.s3.amazonaws.com/{key}")

        generated_files += 1

    overview = {"csv_generated": generated_files, "csv_files_urls": csv_files_urls}
    logger.info(f"all operations completed: {overview}")

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({"errors": None, "data": overview}),
    }


def lambda_handler(event, context):
    logger.info(f"event received: {event}")
    body = json.loads(event["body"])

    body_schema = {
        "amount": {"type": "integer", "min": 1, "max": 3},
        "rows_min": {"type": "integer", "min": 1},
        "rows_max": {"type": "integer", "max": 300},
        "min_transaction_amount": {"type": "float", "min": -20000.0, "max": 20000.0},
        "max_transaction_amount": {"type": "float", "min": -20000.0, "max": 20000.0},
    }

    v = Validator(body_schema)
    valid_event = v.validate(body)

    if not valid_event:
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({"errors": v.errors, "data": None}),
        }

    amount = body["amount"]
    rows_min = body["rows_min"]
    rows_max = body["rows_max"]
    min_transaction_amount = body["min_transaction_amount"]
    max_transaction_amount = body["max_transaction_amount"]

    return csv_creator(
        amount=amount,
        rows_min=rows_min,
        rows_max=rows_max,
        min_transaction_amount=min_transaction_amount,
        max_transaction_amount=max_transaction_amount,
    )
