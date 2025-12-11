import os
from pymongo import MongoClient
import pprint

# Lấy cấu hình từ biến môi trường (giống như trong container Airflow)
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
MONGO_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'mongodb_password')
MONGO_DB = os.getenv('MONGO_DB', 'job_db')

try:
    # Kết nối MongoDB
    uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"
    client = MongoClient(uri)
    db = client[MONGO_DB]

    print(f"🔌 Đã kết nối đến MongoDB: {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}")
    print("-" * 50)

    # 1. Kiểm tra Raw Data
    raw_count = db.raw_jobs.count_documents({})
    print(f"📊 Số lượng Raw Jobs (raw_jobs): {raw_count}")
    
    if raw_count > 0:
        print("📝 Mẫu dữ liệu Raw (1 document):")
        sample_raw = db.raw_jobs.find_one()
        if sample_raw and '_id' in sample_raw:
            sample_raw['_id'] = str(sample_raw['_id'])
        pprint.pprint(sample_raw)

    # 2. Kiểm tra Processed Data
    processed_count = db.processed_jobs.count_documents({})
    print(f"✅ Số lượng Processed Jobs (processed_jobs): {processed_count}")

    print("-" * 50)

    # 3. In mẫu dữ liệu nếu có
    if processed_count > 0:
        print("📝 Mẫu dữ liệu đã xử lý (1 document):")
        sample = db.processed_jobs.find_one()
        # Loại bỏ _id để in cho đẹp
        if sample and '_id' in sample:
            sample['_id'] = str(sample['_id'])
        pprint.pprint(sample)
    else:
        print("⚠️ Chưa có dữ liệu trong processed_jobs. Hãy kiểm tra lại log của task process_data.")

except Exception as e:
    print(f"❌ Lỗi khi kiểm tra dữ liệu: {e}")
