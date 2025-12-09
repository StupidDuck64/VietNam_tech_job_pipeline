"""
===== ITviec Job Scraper =====
Script cào dữ liệu việc làm từ ITviec.com
Sử dụng: Requests + BeautifulSoup
Lưu vào: MongoDB

Author: Data Engineering Team
Date: December 2025
"""

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, errors
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import time

# ===== Load environment variables =====
load_dotenv()

# ===== Config logging =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===== Environment Variables =====
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'mongodb_password')
MONGO_DB = os.getenv('MONGO_DB', 'job_db')
TARGET_URL = os.getenv('TARGET_URL', 'https://itviec.com/it-jobs/data-engineer')
SCRAPE_DELAY = int(os.getenv('SCRAPE_DELAY', 2))


class ITviecScraper:
    """
    Class để cào dữ liệu từ ITviec.com
    
    Attributes:
        mongo_uri (str): Connection string để kết nối MongoDB
        db_name (str): Tên database trong MongoDB
        collection_name (str): Tên collection để lưu dữ liệu
    """
    
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str = 'raw_jobs'):
        """
        Khởi tạo scraper
        
        Args:
            mongo_uri: MongoDB connection URI
            db_name: Database name
            collection_name: Collection name (default: 'raw_jobs')
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
    def connect_mongodb(self):
        """Kết nối tới MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info(f"✅ Kết nối MongoDB thành công - Database: {self.db_name}")
        except errors.ServerSelectionTimeoutError as e:
            logger.error(f"❌ Không thể kết nối MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Lỗi khi kết nối MongoDB: {e}")
            raise
    
    def disconnect_mongodb(self):
        """Ngắt kết nối MongoDB"""
        if self.client:
            self.client.close()
            logger.info("✅ Ngắt kết nối MongoDB")
    
    def fetch_page(self, url: str, max_retries: int = 3) -> str:
        """
        Lấy HTML từ URL với retry logic
        
        Args:
            url: URL của trang web
            max_retries: Số lần retry tối đa
            
        Returns:
            HTML content (str)
        """
        for attempt in range(max_retries):
            try:
                # Thêm delay ngẫu nhiên để tránh pattern detection
                if attempt > 0:
                    wait_time = (2 ** attempt) + (attempt * 0.5)  # Exponential backoff
                    logger.info(f"⏳ Retry {attempt}/{max_retries} sau {wait_time}s...")
                    time.sleep(wait_time)
                
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                logger.info(f"✅ Lấy dữ liệu từ: {url}")
                return response.text
                
            except requests.HTTPError as e:
                if e.response.status_code == 403:
                    logger.warning(f"⚠️ 403 Forbidden (attempt {attempt + 1}/{max_retries})")
                    if attempt == max_retries - 1:
                        logger.error(f"❌ Hết retry, vẫn bị 403: {url}")
                        raise
                else:
                    raise
            except requests.RequestException as e:
                logger.error(f"❌ Lỗi fetch page (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise
    
    def parse_job_listing(self, job_html) -> dict:
        """
        Parse 1 job listing từ HTML
        
        Args:
            job_html: BeautifulSoup element của 1 job listing
            
        Returns:
            Dict chứa thông tin job
        """
        try:
            job_data = {}
            
            # ===== Tìm job title =====
            job_title_elem = job_html.find('h2', class_='job__title')
            job_data['job_title'] = job_title_elem.get_text(strip=True) if job_title_elem else 'N/A'
            
            # ===== Tìm job URL =====
            job_link_elem = job_html.find('a', class_='job__link')
            job_data['job_url'] = job_link_elem['href'] if job_link_elem else 'N/A'
            
            # ===== Tìm company name =====
            company_elem = job_html.find('span', class_='company-name')
            job_data['company_name'] = company_elem.get_text(strip=True) if company_elem else 'N/A'
            
            # ===== Tìm location =====
            location_elem = job_html.find('span', class_='location')
            job_data['location'] = location_elem.get_text(strip=True) if location_elem else 'N/A'
            
            # ===== Tìm salary =====
            salary_elem = job_html.find('span', class_='salary')
            job_data['salary'] = salary_elem.get_text(strip=True) if salary_elem else 'Not disclosed'
            
            # ===== Tìm job description (short preview) =====
            desc_elem = job_html.find('div', class_='job__description')
            job_data['description_preview'] = desc_elem.get_text(strip=True) if desc_elem else 'N/A'
            
            # ===== Add metadata =====
            job_data['scraped_at'] = datetime.now().isoformat()
            job_data['source'] = 'itviec.com'
            
            return job_data
        
        except Exception as e:
            logger.warning(f"⚠️ Lỗi parse job listing: {e}")
            return None
    
    def scrape_jobs(self, url: str, max_pages: int = 1) -> list:
        """
        Cào dữ liệu từ ITviec.com
        
        Args:
            url: URL trang search
            max_pages: Số trang cần cào (default: 1)
            
        Returns:
            List of job dictionaries
        """
        all_jobs = []
        
        for page in range(1, max_pages + 1):
            try:
                # ===== Tạo URL với pagination =====
                page_url = f"{url}?page={page}" if page > 1 else url
                logger.info(f"📄 Đang cào trang {page}: {page_url}")
                
                # ===== Fetch HTML =====
                html = self.fetch_page(page_url)
                soup = BeautifulSoup(html, 'html.parser')
                
                # ===== Tìm tất cả job listings =====
                job_listings = soup.find_all('div', class_='job-item')
                logger.info(f"🔍 Tìm thấy {len(job_listings)} job listings trên trang {page}")
                
                if not job_listings:
                    logger.warning(f"⚠️ Không tìm thấy job listings trên trang {page}")
                    break
                
                # ===== Parse từng job =====
                for job_html in job_listings:
                    job_data = self.parse_job_listing(job_html)
                    if job_data:
                        all_jobs.append(job_data)
                
                # ===== Delay để tránh bị chặn IP (với random jitter) =====
                import random
                delay = SCRAPE_DELAY + random.uniform(0.5, 2.0)
                logger.info(f"⏳ Chờ {delay:.1f}s trước khi cào trang tiếp...")
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"❌ Lỗi khi cào trang {page}: {e}")
                continue
        
        logger.info(f"✅ Tổng cộng cào được {len(all_jobs)} jobs")
        return all_jobs
    
    def save_to_mongodb(self, jobs: list) -> bool:
        """
        Lưu dữ liệu vào MongoDB
        
        Args:
            jobs: List of job dictionaries
            
        Returns:
            True nếu lưu thành công, False nếu thất bại
        """
        try:
            if not jobs:
                logger.warning("⚠️ Không có dữ liệu để lưu")
                return False
            
            # ===== Xóa dữ liệu cũ (optional) =====
            # self.collection.delete_many({})
            # logger.info("🗑️ Xóa dữ liệu cũ trong collection")
            
            # ===== Insert dữ liệu mới =====
            result = self.collection.insert_many(jobs)
            logger.info(f"✅ Lưu {len(result.inserted_ids)} jobs vào MongoDB")
            
            return True
        
        except errors.DuplicateKeyError:
            logger.warning("⚠️ Một số jobs đã tồn tại trong database")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi lưu vào MongoDB: {e}")
            return False
    
    def get_statistics(self) -> dict:
        """
        Lấy thống kê từ collection
        
        Returns:
            Dict chứa số lượng records, etc
        """
        try:
            total_jobs = self.collection.count_documents({})
            
            # ===== Thống kê công ty =====
            unique_companies = self.collection.distinct('company_name')
            
            # ===== Thống kê location =====
            unique_locations = self.collection.distinct('location')
            
            stats = {
                'total_jobs': total_jobs,
                'unique_companies': len(unique_companies),
                'unique_locations': len(unique_locations),
                'last_scraped': datetime.now().isoformat()
            }
            
            logger.info(f"📊 Thống kê: {json.dumps(stats, indent=2, ensure_ascii=False)}")
            return stats
        
        except Exception as e:
            logger.error(f"❌ Lỗi lấy thống kê: {e}")
            return {}


def main():
    """Main function"""
    
    # ===== Tạo MongoDB URI =====
    mongo_uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    
    # ===== Khởi tạo scraper =====
    scraper = ITviecScraper(
        mongo_uri=mongo_uri,
        db_name=MONGO_DB
    )
    
    try:
        # ===== Kết nối MongoDB =====
        scraper.connect_mongodb()
        
        # ===== Cào dữ liệu =====
        logger.info(f"🚀 Bắt đầu cào từ: {TARGET_URL}")
        jobs = scraper.scrape_jobs(TARGET_URL, max_pages=2)
        
        # ===== Lưu vào MongoDB =====
        success = scraper.save_to_mongodb(jobs)
        
        if success:
            # ===== Hiển thị thống kê =====
            scraper.get_statistics()
            logger.info("✨ Hoàn thành scraping!")
        else:
            logger.error("❌ Scraping thất bại!")
            
    except Exception as e:
        logger.error(f"❌ Lỗi chung: {e}")
    finally:
        # ===== Ngắt kết nối =====
        scraper.disconnect_mongodb()


if __name__ == '__main__':
    main()
