"""
Script xóa dữ liệu trong MongoDB (Dùng cẩn thận)
Author: Data Engineering Team
"""

import os
from pymongo import MongoClient

# Get config from env
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
MONGO_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'mongodb_password')
MONGO_DB = os.getenv('MONGO_DB', 'job_db')

try:
    # Connect to MongoDB
    uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
    client = MongoClient(uri)
    db = client[MONGO_DB]

    print(f"🔌 Connected to MongoDB: {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}")
    print("-" * 50)

    # Delete Raw Data
    result_raw = db.raw_jobs.delete_many({})
    print(f"🗑️ Deleted {result_raw.deleted_count} documents from 'raw_jobs'")

    # Delete Processed Data
    result_processed = db.processed_jobs.delete_many({})
    print(f"🗑️ Deleted {result_processed.deleted_count} documents from 'processed_jobs'")

    print("-" * 50)
    print("✅ Data cleanup complete.")

except Exception as e:
    print(f"❌ Error deleting data: {e}")
