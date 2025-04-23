from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import pika
import json
import time
from bson import ObjectId, json_util

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_publish_jobs():
    """
    Fetch jobs with 'new' status from MongoDB AskEcho database and publish them to pdp_scraper_queue.
    After publishing, update their status to 'queued'.
    """
    try:
        print("Starting job fetch and publish process...")
        # MongoDB Connection
        print("Connecting to MongoDB...")
        mongo_client = MongoClient(
            host='mongodb',
            port=27017,
            username='root',
            password='example',
            authSource='admin'
        )
        db = mongo_client['AskEcho']
        jobs_collection = db['Jobs']
        
        # RabbitMQ Connection
        print("Connecting to RabbitMQ...")
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='rabbitmq',
                credentials=credentials
            )
        )
        channel = connection.channel()

        # Ensure the queue exists
        queue_name = 'orchestrator_queue'
        channel.queue_declare(queue=queue_name, durable=True)
        print(f"Declared queue: {queue_name}")

        # Fetch new jobs from MongoDB (limit to 10 jobs per run)
        query = {'status': 'new'}
        print(f"Searching for jobs with query: {query}")
        
        jobs_to_process = jobs_collection.find(query).limit(10)
        jobs_count = jobs_collection.count_documents(query)
        print(f"Found {jobs_count} jobs to process (will process up to 10)")

        jobs_processed = 0
        for job in jobs_to_process:
            try:
                print(f"Processing job: {job.get('job_id', 'unknown')}")
                
                # Create a copy of the job document for publishing
                job_copy = job.copy()
                
                # Convert ObjectId to string
                job_copy['_id'] = str(job['_id'])
                
                # Convert datetime fields to ISO format strings
                for key, value in job_copy.items():
                    if isinstance(value, datetime):
                        job_copy[key] = value.isoformat()
                
                # Publish to RabbitMQ orchestrator_queue
                message_body = json.dumps(job_copy, default=str)  # Use str as fallback serializer
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=message_body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                        priority=job.get('priority', 1),
                        timestamp=int(datetime.utcnow().timestamp())
                    )
                )
                print(f"Published message to orchestrator_queue: {message_body[:100]}...")

                # Update job status to 'queued' and timestamps in MongoDB
                update = {
                    '$set': {
                        'status': 'queued_for_orchestrator',
                        'updated_at': datetime.utcnow(),
                        'queued_at': datetime.utcnow()
                    }
                }
                
                result = jobs_collection.update_one(
                    {'_id': job['_id']},  # Use original ObjectId
                    update
                )
                print(f"Updated job status to queued_for_orchestrator in MongoDB. Modified count: {result.modified_count}")
                
                jobs_processed += 1
                
            except Exception as e:
                print(f"Error processing job {job.get('job_id', 'unknown')}: {str(e)}")
                continue

        # Close connections
        connection.close()
        mongo_client.close()

        print(f"Job processing complete. Successfully processed {jobs_processed} jobs")

    except Exception as e:
        print(f"Error in fetch_and_publish_jobs: {str(e)}")
        raise

# Create the DAG
dag = DAG(
    'job_fetch_operator',
    default_args=default_args,
    description='Fetch new jobs from AskEcho MongoDB and publish to orchestrator_queue',
    schedule_interval=timedelta(seconds=1),  # Run every second
    catchup=False
)

# Create the task
fetch_jobs_task = PythonOperator(
    task_id='fetch_and_publish_jobs',
    python_callable=fetch_and_publish_jobs,
    queue='default',  # Use default queue for Airflow task execution
    dag=dag
)

# Set task dependencies (in this case, we only have one task)
fetch_jobs_task
