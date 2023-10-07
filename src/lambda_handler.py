import csv
import io
import json
import logging
import random
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
	logger.info("started new csv_creator process")

	csv_buffer = io.StringIO()
	csv_writer = csv.writer(csv_buffer)

	csv_headers = [
		"ID",
		"First Name",
		"Last Name",
		"Date",
		"Transaction",
		"Transaction ID",
	]

	csv_writer.writerow(csv_headers)
	logger.info("headers written")

	# setup faker
	generated_files = 0
	current_year = datetime.now().year
	start_date = datetime(current_year, 1, 1, 0, 0, 0)
	end_date = datetime(current_year, 9, 30, 23, 59, 59)
	csv_files_urls = []

	while generated_files < amount:
		fake_rows_num = fake.random_int(rows_min, rows_max)
		csv_data = []
		generated_data_row = 0
		id_user = fake.unique.random_int(10000, get_max_account_numbers(amount))
		prefix = "4242"
		account_number = f"{prefix}-{id_user}"

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
					fake.first_name(),
					fake.last_name(),
					record_date.strftime("%Y-%m-%d %H:%M:%S"),
					record_transaction_amount,
					fake.uuid4(),
				]
			)

			generated_data_row += 1

		csv_writer.writerows(csv_data)
		logger.info("written rows")

		s3 = boto3.client("s3")
		logger.info("started s3 service")
		
		try:
			key = f"{account_number}_transactions_report.csv"
			s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
			logger.info(f"saved object {key} to s3")
		except Exception as e:
			logger.error(f"unable to upload file: {e}")

		csv_files_urls.append(f"https://s3.amazonaws.com/{bucket_name}/{key}")

		generated_files += 1

	overview = {"csv_generated": amount, "csv_files_urls": csv_files_urls}
	logger.info(f"overview generated: {overview}")

	logger.info("about to return success")
	
	return {
		"statusCode": 200,
		"headers": {
			"Content-Type": "application/json",
			"Access-Control-Allow-Origin": "*",
		},
		"body": overview
	}


def lambda_handler(event, context):
	logger.info(f"event received: {event}")

	event_schema = {
		"amount": {"type": "integer", "min": 1, "max": 50},
		"rows_min": {"type": "integer", "min": 1},
		"rows_max": {"type": "integer", "max": 300},
		"min_transaction_amount": {"type": "float", "min": -20000.0, "max": 20000.0},
		"max_transaction_amount": {"type": "float", "min": -20000.0, "max": 20000.0},
	}

	v = Validator(event_schema)
	valid_event = v.validate(event)

	if not valid_event:
		return {
			"statusCode": 400,
			"headers": {
				"Content-Type": "application/json",
				"Access-Control-Allow-Origin": "*",
			},
			"body": json.dumps(valid_event),
		}

	amount = event["amount"]
	rows_min = event["rows_min"]
	rows_max = event["rows_max"]
	min_transaction_amount = event["min_transaction_amount"]
	max_transaction_amount = event["max_transaction_amount"]

	return csv_creator(
		amount=amount,
		rows_min=rows_min,
		rows_max=rows_max,
		min_transaction_amount=min_transaction_amount,
		max_transaction_amount=max_transaction_amount,
	)


if __name__ == "__main__":
	csv_creator(amount=20)
