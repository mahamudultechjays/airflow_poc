from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import pika
import json

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
    Fetch jobs with 'new' status from MongoDB AskEcho database and publish them to RabbitMQ.
    After publishing, update their status to 'pending'.
    """
    try:
        print("Starting job fetch and publish process...")
        
        # MongoDB Connection
        print("Connecting to MongoDB...")
        mongo_client = MongoClient('mongodb://root:example@mongodb:27017/')
        db = mongo_client['AskEcho']
        jobs_collection = db['Jobs']
        
        # Print total number of jobs in collection
        total_jobs = jobs_collection.count_documents({})
        print(f"Total jobs in collection: {total_jobs}")

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
        queue_name = 'pdp_scraper_queue'
        channel.queue_declare(queue=queue_name, durable=True)
        print(f"Declared queue: {queue_name}")

        # Fetch only new jobs from MongoDB
        query = {'status': 'new'}
        print(f"Searching for jobs with query: {query}")
        
        new_jobs = jobs_collection.find(query)
        new_jobs_count = jobs_collection.count_documents(query)
        print(f"Found {new_jobs_count} new jobs to process")

        jobs_processed = 0
        for job in new_jobs:
            print(f"Processing job: {job.get('job_id', 'unknown')}")
            
            # Convert ObjectId to string for JSON serialization
            job['_id'] = str(job['_id'])
            
            # Create routing key based on job type and marketplace
            routing_key = f"{job.get('job_type', 'unknown')}.{job.get('market_place', 'unknown')}"
            print(f"Using routing key: {routing_key}")
            
            # Publish to RabbitMQ
            message_body = json.dumps(job)
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    priority=job.get('priority', 1),
                    timestamp=int(datetime.utcnow().timestamp()),
                    headers={
                        'job_type': job.get('job_type'),
                        'market_place': job.get('market_place')
                    }
                )
            )
            print(f"Published message to RabbitMQ: {message_body[:100]}...")

            # Update job status to 'pending' and timestamps in MongoDB
            update = {
                '$set': {
                    'status': 'pending',  # Changed from 'queued' to 'pending'
                    'updated_at': datetime.utcnow().isoformat(),
                    'queued_at': datetime.utcnow().isoformat()
                }
            }
            
            result = jobs_collection.update_one(
                {'_id': job['_id']},
                update
            )
            print(f"Updated job status to pending in MongoDB. Modified count: {result.modified_count}")
            
            jobs_processed += 1

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
    description='Fetch new jobs from AskEcho MongoDB and publish to RabbitMQ',
    schedule_interval=timedelta(seconds=1),  # Run every second
    catchup=False
)

# Create the task
fetch_jobs_task = PythonOperator(
    task_id='fetch_and_publish_jobs',
    python_callable=fetch_and_publish_jobs,
    dag=dag
)

# Set task dependencies (in this case, we only have one task)
fetch_jobs_task
