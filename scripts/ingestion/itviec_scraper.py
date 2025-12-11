from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
from bs4 import BeautifulSoup
from pymongo import MongoClient, errors
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import time
import random

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
TARGET_URL = os.getenv('TARGET_URL', 'https://itviec.com/it-jobs')
SCRAPE_DELAY = int(os.getenv('SCRAPE_DELAY', 2))


class ITviecScraper:
    """
    Class để cào dữ liệu từ ITviec.com bằng Selenium
    
    Attributes:
        mongo_uri (str): Connection string để kết nối MongoDB
        db_name (str): Tên database trong MongoDB
        collection_name (str): Tên collection để lưu dữ liệu
    """
    
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str = 'raw_jobs'):
        """
        Khởi tạo scraper với Selenium
        
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
        self.driver = None
        
    def _init_driver(self):
        """Khởi tạo Chrome WebDriver với headless mode"""
        try:
            logger.info("Đang khởi tạo Chrome WebDriver...")
            
            chrome_options = Options()
            # chrome_options.add_argument('--headless')  # Tạm tắt headless để debug (nếu chạy local)
            chrome_options.add_argument('--headless=new') # Chế độ headless mới của Chrome, ít bị detect hơn
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Thêm các arguments để bypass Cloudflare
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--proxy-server='direct://'")
            chrome_options.add_argument("--proxy-bypass-list=*")
            chrome_options.add_argument("--start-maximized")
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_argument('--ignore-certificate-errors')
            
            # Tắt automation flags
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Khởi tạo driver 
            # Chrome đã được cài trong container, dùng default chromedriver
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Apply selenium-stealth
            stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            
            logger.info("Chrome WebDriver đã khởi tạo thành công (với Stealth)")
            
        except Exception as e:
            logger.error(f"Lỗi khởi tạo WebDriver: {e}")
            raise
    
    def _close_driver(self):
        """Đóng WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Đã đóng Chrome WebDriver")
            except Exception as e:
                logger.warning(f"Lỗi khi đóng driver: {e}")
    
    def connect_mongodb(self):
        """Kết nối tới MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info(f"Kết nối MongoDB thành công - Database: {self.db_name}")
        except errors.ServerSelectionTimeoutError as e:
            logger.error(f"Không thể kết nối MongoDB: {e}")
            raise
    
    def disconnect_mongodb(self):
        """Ngắt kết nối MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Ngắt kết nối MongoDB")
    
    def fetch_page(self, url: str, max_retries: int = 3) -> str:
        """
        Lấy HTML từ URL bằng Selenium
        
        Args:
            url: URL của trang web
            max_retries: Số lần retry tối đa
            
        Returns:
            HTML content (str)
        """
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = (2 ** attempt) + random.uniform(1, 3)
                    logger.info(f"Retry {attempt}/{max_retries} sau {wait_time:.1f}s...")
                    time.sleep(wait_time)
                
                logger.info(f"Đang tải: {url}")
                self.driver.get(url)
                
                # Đợi trang load (đợi job listings xuất hiện)
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-content, div[class*='job'], h3"))
                    )
                    logger.info("Trang đã load xong")
                except TimeoutException:
                    logger.warning("Timeout chờ elements, nhưng vẫn tiếp tục...")
                
                # Thêm delay ngẫu nhiên để giống người dùng thật
                time.sleep(random.uniform(3, 5))
                
                # Lấy page source
                html = self.driver.page_source
                
                # Kiểm tra xem có phải trang Cloudflare challenge không
                if "Just a moment" in html or "Checking your browser" in html:
                    logger.warning("Gặp Cloudflare challenge, đợi...")
                    time.sleep(10)  # Đợi Cloudflare solve
                    html = self.driver.page_source
                
                logger.info(f"Lấy được {len(html)} bytes HTML")
                return html
                
            except WebDriverException as e:
                logger.error(f"Lỗi WebDriver (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
            except Exception as e:
                logger.error(f"Lỗi fetch page (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
    
    def parse_job_listing(self, job_html) -> dict:
        """
        Parse 1 job listing từ HTML
        
        Args:
            job_html: BeautifulSoup element chứa thông tin 1 job
            
        Returns:
            dict chứa thông tin job
        """
        try:
            # Job Title & URL
            # Title nằm trong thẻ h3, URL nằm trong attribute data-url của h3
            title_elem = job_html.find('h3')
            if title_elem:
                title = title_elem.get_text(strip=True)
                job_url = title_elem.get('data-url')
                if not job_url:
                    # Fallback nếu không có data-url
                    a_tag = title_elem.find('a')
                    if a_tag:
                        job_url = a_tag.get('href')
            else:
                title = "N/A"
                job_url = "N/A"
            
            # Chuẩn hóa URL
            if job_url and job_url != "N/A" and not job_url.startswith('http'):
                job_url = f"https://itviec.com{job_url}"

            # Company Name
            # Tìm tất cả thẻ a có href chứa /companies/
            company_links = job_html.find_all('a', href=lambda x: x and '/companies/' in x)
            company = "N/A"
            for link in company_links:
                text = link.get_text(strip=True)
                if text:
                    company = text
                    break
            
            # Location
            # Tìm div có các class này (dùng CSS selector cho chính xác)
            location_elem = job_html.select_one('.text-rich-grey.text-truncate.text-nowrap')
            location = location_elem.get_text(strip=True) if location_elem else "N/A"
            
            # Salary
            salary_elem = job_html.find('div', class_='salary')
            salary = salary_elem.get_text(strip=True) if salary_elem else "N/A"
            
            # Skills (Tags)
            skills = []
            tag_list = job_html.find('div', attrs={'data-controller': 'responsive-tag-list'})
            if tag_list:
                skills = [tag.get_text(strip=True) for tag in tag_list.find_all('a')]

            return {
                "title": title,
                "url": job_url,
                "company": company,
                "location": location,
                "salary": salary,
                "skills": skills,
                "scraped_at": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Lỗi parse job: {e}")
            return None

            return None
    
    def scrape_jobs(self, base_url: str, max_pages: int = 5) -> list:
        """
        Cào danh sách jobs theo từng Keyword (Skill) để tránh Pagination của Cloudflare.
        Thay vì duyệt ?page=1,2,3 (bị block), ta duyệt /it-jobs/java, /it-jobs/python...
        
        Args:
            base_url: URL gốc (không dùng nhiều trong strategy này)
            max_pages: Số lượng keyword muốn cào (tạm dùng tham số này)
            
        Returns:
            List các job dicts
        """
        all_jobs = []
        
        # Danh sách keywords phổ biến để cào (Mở rộng để lấy nhiều job)
        keywords = [
            # --- Languages ---
            "java", "python", "javascript", "typescript", "c-sharp", "php", "golang", "ruby", "c-plus-plus", "swift", "kotlin", "rust", "scala",
            # --- Frameworks/Libs ---
            "reactjs", "angular", "vuejs", "node-js", "spring-boot", "django", "laravel", "flutter", "react-native", ".net",
            # --- Roles ---
            "backend-developer", "frontend-developer", "fullstack-developer", "mobile-developer", "ios", "android", 
            "devops-engineer", "automation-tester", "manual-tester", "qa-qc", 
            "business-analyst", "project-manager", "product-owner", "product-manager", 
            "data-engineer", "data-analyst", "data-scientist", "ai-engineer",
            "system-admin", "network-engineer", "solution-architect", "ui-ux-designer",
            # --- Cloud/Tools ---
            "aws", "azure", "google-cloud", "docker", "kubernetes"
        ]
        
        
        target_keywords = keywords
        if 0 < max_pages < 5: 
             target_keywords = keywords[:max_pages]
        
        logger.info(f"Sẽ cào dữ liệu với {len(target_keywords)} keywords: {target_keywords}")
        
        # Khởi tạo driver
        self._init_driver()
        
        try:
            for index, keyword in enumerate(target_keywords):
               
                if index > 0:
                    try:
                        logger.info("Đang khởi động lại WebDriver để tránh bị block...")
                        self._close_driver()
                        
                        delay = random.uniform(20, 40)
                        logger.info(f"Nghỉ {delay:.1f}s trước khi tạo session mới...")
                        time.sleep(delay)
                        
                        self._init_driver()
                    except Exception as e:
                        logger.error(f"Lỗi khi restart driver: {e}")
                        # Nếu lỗi restart, cố gắng init lại nếu chưa có
                        if not self.driver:
                            self._init_driver()

                # Xây dựng URL theo keyword
                url = f"https://itviec.com/it-jobs/{keyword}"
                
                logger.info(f"[{index+1}/{len(target_keywords)}] Đang xử lý keyword: {keyword} -> {url}")
                
                try:
                    # Lấy HTML của trang
                    html = self.fetch_page(url)
                    
                    # Kiểm tra Cloudflare Challenge trong HTML
                    if "Verify you are human" in html or "Just a moment" in html:
                        # Nếu file lớn (>100KB), có thể là false positive?
                        if len(html) > 100000:
                             logger.info(f"Phát hiện từ khóa Cloudflare nhưng HTML lớn ({len(html)} bytes) -> False Positive. Tiếp tục parse...")
                        else:
                            logger.warning(f"Cloudflare Challenge detected for {keyword}. Saving HTML for debug...")
                            try:
                                with open(f"/tmp/debug_{keyword}_challenge.html", "w", encoding="utf-8") as f:
                                    f.write(html)
                            except Exception as e:
                                logger.warning(f"Không thể lưu file debug: {e}")
                                
                            logger.warning("Waiting 60s...")
                            time.sleep(60)
                            html = self.fetch_page(url) # Retry 1 lần

                    # Kiểm tra nếu bị block (HTML quá ngắn)
                    if len(html) < 30000:
                        logger.warning(f"HTML quá ngắn ({len(html)} bytes), có thể bị block. Đợi 30s và thử lại...")
                        time.sleep(30)
                        html = self.fetch_page(url) # Retry 1 lần
                    
                    # Parse với BeautifulSoup
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Tìm tất cả job listings
                    # Selector cập nhật: Tìm div có class chứa 'job' và bên trong có h3
                    job_cards = []
                    
                    # Strategy 1: Tìm theo class cụ thể (thường là job-card hoặc job-content)
                    candidates = soup.select('div[class*="job"]')
                    
                    for card in candidates:
                        # Filter: Phải có H3 (Title) và không phải là "Job Alert" hay quảng cáo
                        if card.find('h3') and not card.find('form'): 
                            # Kiểm tra độ dài text để loại bỏ các div rác
                            if len(card.get_text()) > 50:
                                job_cards.append(card)
                    
                    # Loại bỏ duplicate cards (do cấu trúc lồng nhau)
                    # Chỉ giữ lại card con nhất hoặc card cha chuẩn
                    # Đơn giản hóa: Nếu tìm thấy quá nhiều, chỉ lấy 20-30 cái đầu tiên (thường là số job trên 1 trang)
                    if len(job_cards) > 40:
                        job_cards = job_cards[:40]
                    
                    logger.info(f"Keyword '{keyword}': Tìm thấy {len(job_cards)} thẻ jobs tiềm năng")
                    
                    # Parse jobs
                    current_page_jobs = []
                    for job_card in job_cards:
                        job_data = self.parse_job_listing(job_card)
                        if job_data:
                            # Validate dữ liệu rác
                            if job_data['title'] != "N/A" and job_data['url'] != "N/A":
                                current_page_jobs.append(job_data)
                    
                    # Lọc duplicate (so với các keyword trước)
                    seen_urls = {job['url'] for job in all_jobs}
                    new_jobs = [job for job in current_page_jobs if job['url'] not in seen_urls]
                    
                    all_jobs.extend(new_jobs)
                    logger.info(f"Keyword '{keyword}': Thêm {len(new_jobs)} jobs mới (Tổng: {len(all_jobs)})")
                    
                    # Delay giữa các keyword
                    if index < len(target_keywords) - 1:
                        delay = random.uniform(5, 8)
                        logger.info(f"Nghỉ {delay:.1f}s trước keyword tiếp theo...")
                        time.sleep(delay)
                        
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý keyword '{keyword}': {e}")
                    continue
            
            logger.info(f"Tổng cộng cào được {len(all_jobs)} jobs từ {len(target_keywords)} keywords")
            return all_jobs
            
        finally:
            # Đảm bảo đóng driver
            self._close_driver()
    
    def save_to_mongodb(self, jobs: list) -> bool:
        """
        Lưu danh sách jobs vào MongoDB
        
        Args:
            jobs: List các job dicts
            
        Returns:
            True nếu thành công, False nếu thất bại
        """
        if not jobs:
            logger.warning("Không có dữ liệu để lưu")
            return False
        
        try:
            # Xóa dữ liệu cũ (optional - có thể comment out nếu muốn append)
            # self.collection.delete_many({})
            # logger.info("Đã xóa dữ liệu cũ")
            
            # Insert mới
            result = self.collection.insert_many(jobs)
            logger.info(f"Đã lưu {len(result.inserted_ids)} jobs vào MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu vào MongoDB: {e}")
            return False
    
    def get_statistics(self) -> dict:
        """
        Lấy thống kê dữ liệu trong MongoDB
        
        Returns:
            Dict chứa các thống kê
        """
        try:
            total_jobs = self.collection.count_documents({})
            
            # Thống kê theo company
            companies = self.collection.distinct('company')
            
            # Thống kê theo location
            locations = self.collection.distinct('location')
            
            stats = {
                'total_jobs': total_jobs,
                'unique_companies': len(companies),
                'unique_locations': len(locations),
                'last_scraped': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Thống kê: {json.dumps(stats, indent=2, ensure_ascii=False)}")
            return stats
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy thống kê: {e}")
            return {}


# ===== Main execution (for testing) =====
if __name__ == "__main__":
    # MongoDB URI
    mongo_uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    
    # Khởi tạo scraper
    scraper = ITviecScraper(
        mongo_uri=mongo_uri,
        db_name=MONGO_DB
    )
    
    try:
        # Kết nối MongoDB
        scraper.connect_mongodb()
        
        # Cào dữ liệu
        logger.info(f"Bắt đầu cào từ: {TARGET_URL}")
        jobs = scraper.scrape_jobs(TARGET_URL, max_pages=3)
        
        # Lưu vào MongoDB
        success = scraper.save_to_mongodb(jobs)
        
        # Lấy thống kê
        stats = scraper.get_statistics()
        
        logger.info("Hoàn thành!")
        
    except Exception as e:
        logger.error(f"Lỗi: {e}")
        
    finally:
        # Ngắt kết nối
        scraper.disconnect_mongodb()
