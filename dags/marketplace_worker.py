from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pika
import json
import yaml
import time
import requests
from typing import Dict, Any
from pymongo import MongoClient

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

def load_manifest() -> Dict[str, Any]:
    """Load the manifest file containing marketplace configurations"""
    try:
        with open('/opt/airflow/dags/manifest.yaml', 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"Error loading manifest: {str(e)}")
        return {}

def scrape_product(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Scrape product data from the given URL
    This is a simulated implementation for testing
    """
    try:
        print(f"Starting to scrape URL: {url}")
        
        # For testing, return simulated successful response
        return {
            "url": url,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success",
            "html_length": 50000,  # Simulated HTML length
            "title": "Test Product",
            "price": "99.99",
            "description": "This is a test product description"
        }
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return {
            "url": url,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error",
            "error": str(e)
        }

def process_marketplace_queue(channel, db, marketplace, max_messages):
    """Process jobs from a specific marketplace queue"""
    queue_name = f'worker_queue_{marketplace}'
    
    # Declare queue and get message count in one call
    queue_info = channel.queue_declare(queue=queue_name, durable=True)
    message_count = queue_info.method.message_count
    print(f"Found {message_count} messages in {queue_name}")

    processed_count = 0
    while processed_count < max_messages:
        # Try to get a message
        method_frame, header_frame, body = channel.basic_get(queue_name)
        if not method_frame:
            print(f"No more messages in {queue_name}")
            break

        try:
            # Parse job data
            job_data = json.loads(body)
            job_id = job_data.get('job_id')
            marketplace = job_data.get('marketplace', '').lower()
            
            print(f"Processing job {job_id} for marketplace {marketplace}")
            
            # Get marketplace configuration
            marketplace_config = job_data.get('marketplace_config', {})
            headers = marketplace_config.get('headers', {})
            
            # Update status to processing
            db.Jobs.update_one(
                {'job_id': job_id},
                {
                    '$set': {
                        'status': 'processing',
                        'processing_started_at': datetime.utcnow()
                    }
                }
            )
            
            # Get product URL from payload
            product_url = job_data.get('payload', {}).get('product_url')
            if not product_url:
                raise ValueError("No product URL found in job payload")
            
            # Scrape the product
            scrape_result = scrape_product(product_url, headers)
            
            if scrape_result['status'] == 'success':
                # Update job status to finished with results
                db.Jobs.update_one(
                    {'job_id': job_id},
                    {
                        '$set': {
                            'status': 'success',
                            'finished_at': datetime.utcnow(),
                            'result': scrape_result,
                            'html_length': scrape_result['html_length']
                        }
                    }
                )
                print(f"Successfully processed job {job_id}")
            else:
                # Update job status to failed
                db.Jobs.update_one(
                    {'job_id': job_id},
                    {
                        '$set': {
                            'status': 'failed',
                            'failed_at': datetime.utcnow(),
                            'error': scrape_result['error']
                        },
                        '$inc': {'retry_count': 1}
                    }
                )
                print(f"Failed to process job {job_id}: {scrape_result['error']}")
            
            # Acknowledge the message
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            processed_count += 1
            
        except Exception as e:
            print(f"Error processing job {job_id}: {str(e)}")
            # Update job status to failed
            db.Jobs.update_one(
                {'job_id': job_id},
                {
                    '$set': {
                        'status': 'failed',
                        'failed_at': datetime.utcnow(),
                        'error': str(e)
                    },
                    '$inc': {'retry_count': 1}
                }
            )
            # Negative acknowledge message to requeue
            channel.basic_nack(delivery_tag=method_frame.delivery_tag)
    
    return processed_count

def process_marketplace_jobs():
    """Process jobs from worker queues and update their status"""
    try:
        print("Starting marketplace worker process")
        
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', port=5672)
        )
        channel = connection.channel()
        
        # Connect to MongoDB
        client = MongoClient('mongodb://root:example@mongodb:27017/admin')
        db = client['AskEcho']
        
        # Load marketplace configurations
        manifest = load_manifest()   # Manifest processing is for future use                
        print("Loaded manifest configuration")

        total_processed = 0
        max_messages = 10  # Process up to 10 messages per marketplace queue

        # Process messages from marketplace queues separately
        marketplaces = ['amazon', 'walmart']
        for marketplace in marketplaces:
            processed = process_marketplace_queue(channel, db, marketplace, max_messages)
            total_processed += processed
            print(f"Processed {processed} messages from {marketplace} queue")

        # Close connections
        connection.close()
        client.close()
        
        print(f"Total jobs processed: {total_processed}")

    except Exception as e:
        print(f"Error in marketplace worker: {str(e)}")
        raise

# Create the DAG
dag = DAG(
    'marketplace_worker',
    default_args=default_args,
    description='Generic marketplace worker that processes jobs for all marketplaces',
    schedule_interval=timedelta(seconds=1), # TODO: check sensors to improvise
    catchup=False
)

process_jobs_task = PythonOperator(
    task_id='process_marketplace_jobs',
    python_callable=process_marketplace_jobs,
    dag=dag
)

process_jobs_task 