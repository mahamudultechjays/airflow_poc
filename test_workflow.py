#!/usr/bin/env python3
"""
Test script to verify the complete job processing workflow:
1. Insert a new job with 'new' status
2. Run job_fetch_operator to move the job to the orchestrator_queue
3. Directly publish a message to the worker queue for testing
4. Run marketplace_worker to process the job and update its status
5. Verify the final job status
"""

import json
import time
import subprocess
import pika
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId, json_util

# Custom JSON encoder for MongoDB objects
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(MongoJSONEncoder, self).default(obj)

def insert_test_job():
    """Insert a test job into MongoDB with status 'new'"""
    # Load the job data from the JSON file
    with open('insert_job.json', 'r') as file:
        job_data = json.load(file)

    # Ensure status is new and add timestamps
    job_data['status'] = 'new'
    job_data['created_at'] = datetime.utcnow()
    job_data['updated_at'] = datetime.utcnow()
    
    # Connect to MongoDB
    client = MongoClient('mongodb://root:example@localhost:27017/admin')
    db = client['AskEcho']
    
    # Insert the job
    result = db.Jobs.insert_one(job_data)
    job_id = job_data['job_id']
    
    print(f"Job inserted with ID: {result.inserted_id}")
    print(f"Job ID: {job_id}")
    
    # Verify the job was inserted
    job = db.Jobs.find_one({"job_id": job_id})
    if job:
        print(f"Successfully verified job in database with status: {job['status']}")
    else:
        print("Failed to find job in database. Check your connection and database name.")
    
    client.close()
    return job_id, job_data

def run_airflow_dag(dag_id):
    """Run an Airflow DAG in the Docker container"""
    print(f"\nRunning DAG: {dag_id}")
    cmd = f"docker exec -it airflow_poc-airflow-worker-1 airflow dags test {dag_id} $(date +%Y-%m-%d)"
    subprocess.run(cmd, shell=True)
    print(f"Completed running DAG: {dag_id}")

def check_job_status(job_id):
    """Check the current status of the job in MongoDB"""
    client = MongoClient('mongodb://root:example@localhost:27017/admin')
    db = client['AskEcho']
    
    job = db.Jobs.find_one({"job_id": job_id})
    status = job['status'] if job else 'not_found'
    
    print(f"\nCurrent job status: {status}")
    if job:
        print(f"Job details: {job}")
    
    client.close()
    return status, job

def publish_to_worker_queue(job_data):
    """Manually publish a job to the worker queue"""
    print("\nPublishing job directly to worker queue")
    
    # Create a serializable copy of the job data
    serializable_job = {}
    for key, value in job_data.items():
        if key == '_id':
            serializable_job[key] = str(value)
        elif isinstance(value, datetime):
            serializable_job[key] = value.isoformat()
        else:
            serializable_job[key] = value
    
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672)
    )
    channel = connection.channel()
    
    # Determine marketplace from the job data
    marketplace = serializable_job.get('marketplace', 'amazon').lower()
    worker_queue = f'worker_queue_{marketplace}'
    
    # Ensure the queue exists
    channel.queue_declare(queue=worker_queue, durable=True)
    
    # Add marketplace configuration to job data (simulating what orchestrator would do)
    serializable_job['marketplace_config'] = {
        'headers': {
            'User-Agent': 'Mozilla/5.0',
            'Accept-Language': 'en-US,en;q=0.9'
        }
    }
    
    # Publish to worker queue
    channel.basic_publish(
        exchange='',
        routing_key=worker_queue,
        body=json.dumps(serializable_job),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )
    )
    
    print(f"Published message to {worker_queue}")
    
    # Update job status in MongoDB (simulating what orchestrator would do)
    client = MongoClient('mongodb://root:example@localhost:27017/admin')
    db = client['AskEcho']
    
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
    
    # Close connections
    connection.close()
    client.close()

def main():
    """Execute the full workflow test"""
    # Step 1: Insert a test job
    print("STEP 1: Inserting test job")
    job_id, job_data = insert_test_job()
    
    # Step 2: Get initial job status
    print("\nSTEP 2: Initial job status")
    status, _ = check_job_status(job_id)
    
    # Step 3: Run job_fetch_operator
    print("\nSTEP 3: Running job_fetch_operator")
    run_airflow_dag("job_fetch_operator")
    time.sleep(2)  # Wait for DB update
    status, job = check_job_status(job_id)
    
    # Get updated job data
    if job:
        job_data = job
    
    # Step 4: Publish directly to worker queue
    print("\nSTEP 4: Publishing directly to worker queue")
    publish_to_worker_queue(job_data)
    time.sleep(2)  # Wait for DB update
    status, _ = check_job_status(job_id)
    
    # Step 5: Run marketplace_worker
    print("\nSTEP 5: Running marketplace_worker")
    run_airflow_dag("marketplace_worker")
    time.sleep(2)  # Wait for DB update
    status, _ = check_job_status(job_id)
    
    # Final status check
    print("\nWorkflow test completed.")
    print(f"Final job status: {status}")
    
    if status == 'success':
        print("✅ SUCCESS: Workflow completed successfully!")
    else:
        print("❌ FAILURE: Workflow did not complete as expected.")

if __name__ == "__main__":
    main() 