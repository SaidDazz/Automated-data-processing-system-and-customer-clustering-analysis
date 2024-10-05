import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from kafka import KafkaProducer, KafkaConsumer
import logging
from pymongo import MongoClient
from datetime import datetime, timedelta


# MongoDB connection details
MONGO_URI = "mongodb://root:password@mongodb:27017"
MONGO_DATABASE = "Etat_DB"
MONGO_COLLECTION = "mv_db"

def extract(ti, **kwargs):
    try:
        # File path
        file_path = '/opt/airflow/data/Mouvements_par_date.xlsx'
        df = pd.read_excel(file_path)

        # Convert DataFrame to JSON-serializable format
        df = df.astype(str)

        # Push to XCom
        ti.xcom_push(key='extracted_data', value=df.to_dict('records'))
        logging.info("Data extraction completed.")
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

def stream_data(ti, **kwargs):
    try:
        # Initialize Kafka producer
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

        # Retrieve data from XCom
        data = ti.xcom_pull(task_ids='extract_task', key='extracted_data')

        # Stream data to Kafka
        for record in data:
            try:
                producer.send('users_created', json.dumps(record).encode('utf-8'))
            except Exception as e:
                logging.error(f'Error sending record: {record} - {e}')

        producer.flush()
        logging.info("Data streaming completed.")
    except Exception as e:
        logging.error(f'An error occurred during streaming: {e}')
        raise

def consume_and_store_data(**kwargs):
    try:
        # Initialize Kafka consumer
        consumer = KafkaConsumer(
            'users_created',
            bootstrap_servers=['broker:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='airflow-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Initialize MongoDB client
        client = MongoClient('mongodb://root:password@mongodb:27017/', socketTimeoutMS=60000, connectTimeoutMS=60000)
        db = client['Etat_DB']
        collection = db['mv_db']

        # Consume messages from Kafka and insert into MongoDB
        for message in consumer:
            try:
                collection.insert_one(message.value)
                logging.info(f"Record inserted into MongoDB: {message.value}")
            except Exception as e:
                logging.error(f"Error inserting record into MongoDB: {e}")

        logging.info("Data consumption and storage completed.")
    except Exception as e:
        logging.error(f"An error occurred during consumption and storage: {e}")
        raise


def check_mongodb_data(**kwargs):
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    # Fetch data from the collection
    documents = collection.find()

    # Print out the documents for verification
    for doc in documents:
        print(doc)

    # Close the MongoDB connection
    client.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_streaming_dag',
    default_args=default_args,
    description='A data streaming DAG for Excel files',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
    )

    stream_data_task = PythonOperator(
        task_id='stream_data_task',
        python_callable=stream_data,
    )

    consume_and_store_task = PythonOperator(
        task_id='consume_and_store_task',
        python_callable=consume_and_store_data,
    )

    verify_mongo_data = PythonOperator(
        task_id='verify_mongo_data',
        python_callable=check_mongodb_data,
        provide_context=True,
    )

    extract_task >> stream_data_task >> consume_and_store_task >> verify_mongo_data
