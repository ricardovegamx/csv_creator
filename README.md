# CSV CREATOR LAMBDA

This lambda is meant to be connected to a API Gateway resource. It will receive a POST request with a JSON body containing the data to be generated. The lambda will process and create CSV files with transactions data and save them to a S3 bucket.

- This microservice is intended to be ran as a lambda function.
- It has integration with API Gateway (Proxy Integration).
- All files are saved to a S3 bucket.
- A Makefile is provided to make deploy operations faster (when working locally).

## EXAMPLE REQUEST:

```bash
  {
    "amount": 1,
    "rows_min": 10,
    "rows_max": 15,
    "min_transaction_amount": -20000,
    "max_transaction_amount": 20000
  }
  ```


Definition of each propery:

- amount: Number of CSV files to be generated. Min 1, Max 3.
- rows_min: Minimum number of rows per CSV file. Min 10, Max 300.
- rows_max: Maximum number of rows per CSV file. Min 10, Max 300.
- min_transaction_amount: Minimum transaction amount. Min -20000, Max 20000.
- max_transaction_amount: Maximum transaction amount. Min -20000, Max 20000.


METHOD: URL

```bash
POST https://qo3djrx06i.execute-api.us-west-2.amazonaws.com/prod/csv
```

In the provided example:

- 1 CSV file will be generated.
- Each CSV file will have between 10 and 15 rows.
- Each row will have a transaction amount between -20000 and 20000.

## EXAMPLE SUCCESS RESPONSE:

```bash
{
	"errors": null,
	"data": {
		"csv_generated": 1,
		"csv_files_urls": [
			"https://raw-csv-public-bucket.s3.amazonaws.com/424245243_transactions_report.csv"
		]
	}
}
```

Definition of each property:

- csv_generated: Number of CSV files generated (should match the requested amount).
- csv_files_urls: The S3 URLs of the generated CSV files. (can be downloaded)

## EXAMPLE ERROR RESPONSE:

```bash
{
	"errors": {
		"amount": [
			"max value is 3"
		],
		"rows_max": [
			"max value is 300"
		]
	},
	"data": null
}
```

Definition of each property:

- errors: A dictionary with the errors found in the request.

## WORKFLOW

1. A request is made to given endpoint.
2. The lambda gets invoked from the API Gateway.
3. The lambda validates the request.
4. The lambda generates the CSV files.
5. The lambda saves the CSV files to a S3 bucket.
6. The lambda returns appropriate response.

## REQUIREMENTS

- Python 3.11
- AWS CLI
- The following environment variables must be set:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_DEFAULT_REGION`
  - `AWS_ACCOUNT_ID`

## DEPLOYMENT

1. Create a virtual environment and install the dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

1. Create the lambda function:

```bash 
make create-lambda
```

4. Upgrate the lambda function (whenever you make changes to the code):

```bash
make update-lambda
```

5. [OPTIONAL] Delete lambda function:
   
```bash
make delete-lambda
```

## CODE STYLE

Use `make lint` to format the code style to sensible defaults.

## PERMISSIONS

The lambda will need the following permissions: S3 access.