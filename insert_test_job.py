#!/usr/bin/env python3
import json
from pymongo import MongoClient
from datetime import datetime

# Load the job data from the JSON file
with open('insert_job.json', 'r') as file:
    job_data = json.load(file)

# Add creation timestamp and ensure status is new
job_data['created_at'] = datetime.utcnow()
job_data['updated_at'] = datetime.utcnow()
job_data['status'] = 'new'  # Ensure status is new

try:
    # Connect to MongoDB
    client = MongoClient('mongodb://root:example@localhost:27017/admin')
    db = client['AskEcho']
    
    # Insert the job
    result = db.Jobs.insert_one(job_data)
    
    print(f"Job inserted with ID: {result.inserted_id}")
    print(f"Job ID: {job_data['job_id']}")
    
    # Verify the job was inserted
    job = db.Jobs.find_one({"job_id": job_data['job_id']})
    if job:
        print(f"Successfully verified job in database with status: {job['status']}")
    else:
        print("Failed to find job in database. Check your connection and database name.")
        
    client.close()
    
except Exception as e:
    print(f"Error: {str(e)}") 