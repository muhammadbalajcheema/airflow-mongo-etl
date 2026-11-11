from datetime import datetime, timedelta
import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.decorators import task

MONGO_HOST = "mongodb"
MONGO_PORT = 27017
MONGO_USER = "admin"
MONGO_PASS = "adminpassword"
DB_NAME = "currency_db"
COLLECTION_NAME = "exchange_rates"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 11, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='currency_exchange_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline integrating Frankfurter API rates to MongoDB',
    schedule_interval='@daily',
    catchup=False
) as dag:

    @task(task_id='extract_currency_rates')
    def extract():
        url = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task(task_id='transform_exchange_data')
    def transform(raw_api_data):
        transformed_documents = []
        retrieved_time = datetime.utcnow().isoformat()

        if isinstance(raw_api_data, list):
            for entry in raw_api_data:
                doc = {
                    "base_currency": entry.get("base", "EUR"),
                    "target_currency": entry.get("quote"),
                    "exchange_rate": float(entry.get("rate")),
                    "rate_date": entry.get("date"),
                    "source": "Frankfurter API",
                    "retrieved_at": retrieved_time
                }
                transformed_documents.append(doc)
        elif isinstance(raw_api_data, dict):
            base_currency = raw_api_data.get("base", "EUR")
            rate_date = raw_api_data.get("date")
            rates = raw_api_data.get("rates", {})
            for target_curr, rate_val in rates.items():
                doc = {
                    "base_currency": base_currency,
                    "target_currency": target_curr,
                    "exchange_rate": float(rate_val),
                    "rate_date": rate_date,
                    "source": "Frankfurter API",
                    "retrieved_at": retrieved_time
                }
                transformed_documents.append(doc)

        return transformed_documents

    @task(task_id='load_to_mongodb')
    def load(transformed_data):
        if not transformed_data:
            print("No data available to load.")
            return

        mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
        client = MongoClient(mongo_uri)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        result = collection.insert_many(transformed_data)
        print(f"Successfully inserted {len(result.inserted_ids)} records into MongoDB!")
        client.close()

    extracted_data = extract()
    transformed_records = transform(extracted_data)
    load(transformed_records)