from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pika
import json
import yaml
import time
from pymongo import MongoClient
from urllib.parse import urlparse

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'queue': 'orchestrator_queue'  # Specify the queue for this DAG's tasks
}

def declare_queues():
    """Declare all required RabbitMQ queues"""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', port=5672)
        )
        channel = connection.channel()
        
        # Declare queues
        queues = [
            'orchestrator_queue',  # Queue for initial job ingestion and Airflow tasks
            'worker_queue_amazon',
            'worker_queue_walmart'
        ]
        
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            print(f"Declared queue: {queue}")
        
        connection.close()
        print("Successfully declared all queues")
    except Exception as e:
        print(f"Error declaring queues: {str(e)}")
        raise

def load_manifest():
    """Load the manifest file containing marketplace configurations"""
    try:
        with open('/opt/airflow/dags/manifest.yaml', 'r') as file:
            manifest = yaml.safe_load(file)
            if isinstance(manifest, dict) and 'marketplaces' in manifest:
                return manifest['marketplaces']
            else:
                print("Warning: Invalid manifest format")
                return {}
    except Exception as e:
        print(f"Error loading manifest: {str(e)}")
        return {}

def process_and_route_jobs():
    """
    Pull jobs from orchestrator_queue and route directly to worker queues
    """
    try:
        print("Starting orchestrator process...")
        
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', port=5672)
        )
        channel = connection.channel()
        
        # Connect to MongoDB
        client = MongoClient('mongodb://root:example@mongodb:27017/admin')
        db = client['AskEcho']
        
        # Load manifest for marketplace configurations
        manifest = load_manifest()
        if not manifest:
            raise ValueError("Failed to load marketplace configurations from manifest")
        print("Loaded manifest configuration")

        # Get message count
        source_queue = 'orchestrator_queue'
        queue_info = channel.queue_declare(queue=source_queue, durable=True, passive=True)
        message_count = queue_info.method.message_count
        print(f"Found {message_count} messages in queue")

        processed_count = 0
        max_messages = 10  # Process up to 10 messages per run

        while processed_count < max_messages:
            # Try to get a message
            method_frame, header_frame, body = channel.basic_get(source_queue)
            if not method_frame:
                print("No more messages in queue")
                break

            try:
                # Parse job data
                job_data = json.loads(body)
                product_url = job_data.get('payload', {}).get('product_url', '')
                
                print(f"Processing job: {job_data.get('job_id')}")
                
                # Determine marketplace from URL
                marketplace = job_data.get('marketplace', 'amazon')
                
                if not marketplace:
                    print(f"Available marketplaces: {list(manifest.keys())}")
                    print(f"Product URL: {product_url}")
                    print(f"Domain: {urlparse(product_url).netloc}")
                    raise ValueError(f"Could not determine marketplace from URL: {product_url}")
                
                # Add marketplace configuration to job data
                job_data['marketplace'] = marketplace
                job_data['marketplace_config'] = manifest[marketplace]
                
                # Declare and publish directly to worker queue
                worker_queue = f'worker_queue_{marketplace.lower()}'
                channel.queue_declare(queue=worker_queue, durable=True)
                
                channel.basic_publish(
                    exchange='',
                    routing_key=worker_queue,
                    body=json.dumps(job_data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    )
                )
                
                # Update job status in MongoDB
                db.Jobs.update_one(
                    {'job_id': job_data['job_id']},
                    {
                        '$set': {
                            'status': 'queued_for_worker',
                            'marketplace': marketplace,
                            'queued_at': datetime.utcnow(),
                            'worker_queue': worker_queue
                        }
                    }
                )
                
                # Acknowledge the message
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                print(f"Successfully routed job {job_data['job_id']} to {worker_queue}")
                processed_count += 1
                
            except Exception as e:
                print(f"Error processing job: {str(e)}")
                # Negative acknowledge message to requeue
                channel.basic_nack(delivery_tag=method_frame.delivery_tag)

        # Close connections
        connection.close()
        client.close()
        
        print(f"Processed {processed_count} messages")

    except Exception as e:
        print(f"Error in orchestrator process: {str(e)}")
        raise

# Create the DAG
with DAG(
    'orchestrator_pipeline',
    default_args=default_args,
    description='Pipeline to orchestrate job processing and routing',
    schedule_interval=timedelta(seconds=1),
    catchup=False,
    tags=['orchestrator']
) as dag:

    # Create tasks
    declare_queues_task = PythonOperator(
        task_id='declare_queues',
        python_callable=declare_queues,
        queue='orchestrator_queue',  # Specify queue for this task
        dag=dag
    )

    process_task = PythonOperator(
        task_id='process_and_route_jobs',
        python_callable=process_and_route_jobs,
        queue='orchestrator_queue',  # Specify queue for this task
        dag=dag
    )

    # Set task dependencies
    declare_queues_task >> process_task 