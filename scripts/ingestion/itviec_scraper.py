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
    Class to scrape data from ITviec.com using Selenium
    
    Attributes:
        mongo_uri (str): Connection string to connect to MongoDB
        db_name (str): Database name in MongoDB
        collection_name (str): Collection name to save data
    """
    
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str = 'raw_jobs'):
        """
        Initialize scraper with Selenium
        
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
        """Initialize Chrome WebDriver with headless mode"""
        try:
            logger.info("Initializing Chrome WebDriver...")
            
            chrome_options = Options()
            # chrome_options.add_argument('--headless')  # Temporarily disable headless for debug (if running local)
            chrome_options.add_argument('--headless=new') # New Chrome headless mode, less detectable
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Add arguments to bypass Cloudflare
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--proxy-server='direct://'")
            chrome_options.add_argument("--proxy-bypass-list=*")
            chrome_options.add_argument("--start-maximized")
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_argument('--ignore-certificate-errors')
            
            # Disable automation flags
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Initialize driver 
            # Chrome is installed in container, use default chromedriver
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
            
            logger.info("Chrome WebDriver initialized successfully (with Stealth)")
            
        except Exception as e:
            logger.error(f"WebDriver initialization error: {e}")
            raise
    
    def _close_driver(self):
        """Close WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Chrome WebDriver closed")
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")
    
    def connect_mongodb(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info(f"MongoDB connection successful - Database: {self.db_name}")
        except errors.ServerSelectionTimeoutError as e:
            logger.error(f"Cannot connect to MongoDB: {e}")
            raise
    
    def disconnect_mongodb(self):
        """Disconnect MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Disconnect MongoDB")
    
    def fetch_page(self, url: str, max_retries: int = 3) -> str:
        """
        Get HTML from URL using Selenium
        
        Args:
            url: URL of the website
            max_retries: Maximum number of retries
            
        Returns:
            HTML content (str)
        """
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = (2 ** attempt) + random.uniform(1, 3)
                    logger.info(f"Retry {attempt}/{max_retries} after {wait_time:.1f}s...")
                    time.sleep(wait_time)
                
                logger.info(f"Loading: {url}")
                self.driver.get(url)
                
                # Wait for page load (wait for job listings to appear)
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-content, div[class*='job'], h3"))
                    )
                    logger.info("Page loaded")
                except TimeoutException:
                    logger.warning("Timeout waiting for elements, but continuing...")
                
                # Add random delay to mimic real user
                time.sleep(random.uniform(3, 5))
                
                # Get page source
                html = self.driver.page_source
                
                # Check if it is a Cloudflare challenge page
                if "Just a moment" in html or "Checking your browser" in html:
                    logger.warning("Cloudflare challenge encountered, waiting...")
                    time.sleep(10)  # Wait for Cloudflare solve
                    html = self.driver.page_source
                
                logger.info(f"Got {len(html)} bytes HTML")
                return html
                
            except WebDriverException as e:
                logger.error(f"WebDriver error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
            except Exception as e:
                logger.error(f"Fetch page error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
    
    def parse_job_listing(self, job_html) -> dict:
        """
        Parse 1 job listing from HTML
        
        Args:
            job_html: BeautifulSoup element containing job info
            
        Returns:
            dict containing job info
        """
        try:
            # Job Title & URL
            # Title is in h3 tag, URL is in data-url attribute of h3
            title_elem = job_html.find('h3')
            if title_elem:
                title = title_elem.get_text(strip=True)
                job_url = title_elem.get('data-url')
                if not job_url:
                    # Fallback if no data-url
                    a_tag = title_elem.find('a')
                    if a_tag:
                        job_url = a_tag.get('href')
            else:
                title = "N/A"
                job_url = "N/A"
            
            # Normalize URL
            if job_url and job_url != "N/A" and not job_url.startswith('http'):
                job_url = f"https://itviec.com{job_url}"

            # Company Name
            # Find all a tags with href containing /companies/
            company_links = job_html.find_all('a', href=lambda x: x and '/companies/' in x)
            company = "N/A"
            for link in company_links:
                text = link.get_text(strip=True)
                if text:
                    company = text
                    break
            
            # Location
            # Find div with these classes (use CSS selector for accuracy)
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
            logger.error(f"Parse job error: {e}")
            return None

            return None
    
    def scrape_jobs(self, base_url: str, max_pages: int = 5) -> list:
        """
        Scrape job list by Keyword (Skill) to avoid Cloudflare Pagination.
        Instead of iterating ?page=1,2,3 (blocked), we iterate /it-jobs/java, /it-jobs/python...
        
        Args:
            base_url: Base URL (not used much in this strategy)
            max_pages: Number of keywords to scrape (temporarily use this parameter)
            
        Returns:
            List of job dicts
        """
        all_jobs = []
        
        # List of popular keywords to scrape (Expand to get more jobs)
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
        
        logger.info(f"Will scrape data with {len(target_keywords)} keywords: {target_keywords}")
        
        # Initialize driver
        self._init_driver()
        
        try:
            for index, keyword in enumerate(target_keywords):
               
                if index > 0:
                    try:
                        logger.info("Restarting WebDriver to avoid being blocked...")
                        self._close_driver()
                        
                        delay = random.uniform(20, 40)
                        logger.info(f"Resting {delay:.1f}s before creating new session...")
                        time.sleep(delay)
                        
                        self._init_driver()
                    except Exception as e:
                        logger.error(f"Error restarting driver: {e}")
                        # If restart error, try to init again if not exists
                        if not self.driver:
                            self._init_driver()

                # Build URL by keyword
                url = f"https://itviec.com/it-jobs/{keyword}"
                
                logger.info(f"[{index+1}/{len(target_keywords)}] Processing keyword: {keyword} -> {url}")
                
                try:
                    # Get HTML of page
                    html = self.fetch_page(url)
                    
                    # Check Cloudflare Challenge in HTML
                    if "Verify you are human" in html or "Just a moment" in html:
                        # If file is large (>100KB), maybe false positive?
                        if len(html) > 100000:
                             logger.info(f"Cloudflare keyword detected but HTML large ({len(html)} bytes) -> False Positive. Continue parsing...")
                        else:
                            logger.warning(f"Cloudflare Challenge detected for {keyword}. Saving HTML for debug...")
                            try:
                                with open(f"/tmp/debug_{keyword}_challenge.html", "w", encoding="utf-8") as f:
                                    f.write(html)
                            except Exception as e:
                                logger.warning(f"Cannot save debug file: {e}")
                                
                            logger.warning("Waiting 60s...")
                            time.sleep(60)
                            html = self.fetch_page(url) # Retry 1 lần

                    # Check if blocked (HTML too short)
                    if len(html) < 30000:
                        logger.warning(f"HTML too short ({len(html)} bytes), maybe blocked. Wait 30s and retry...")
                        time.sleep(30)
                        html = self.fetch_page(url) # Retry 1 lần
                    
                    # Parse with BeautifulSoup
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # Find all job listings
                    # Updated selector: Find div with class containing 'job' and inside has h3
                    job_cards = []
                    
                    # Strategy 1: Find by specific class (usually job-card or job-content)
                    candidates = soup.select('div[class*="job"]')
                    
                    for card in candidates:
                        # Filter: Must have H3 (Title) and not "Job Alert" or ads
                        if card.find('h3') and not card.find('form'): 
                            # Check text length to remove junk divs
                            if len(card.get_text()) > 50:
                                job_cards.append(card)
                    
                    # Remove duplicate cards (due to nested structure)
                    # Keep only the innermost card or standard parent card
                    # Simplify: If too many found, only take first 20-30 (usually number of jobs on 1 page)
                    if len(job_cards) > 40:
                        job_cards = job_cards[:40]
                    
                    logger.info(f"Keyword '{keyword}': Found {len(job_cards)} potential job cards")
                    
                    # Parse jobs
                    current_page_jobs = []
                    for job_card in job_cards:
                        job_data = self.parse_job_listing(job_card)
                        if job_data:
                            # Validate junk data
                            if job_data['title'] != "N/A" and job_data['url'] != "N/A":
                                current_page_jobs.append(job_data)
                    
                    # Filter duplicate (compared to previous keywords)
                    seen_urls = {job['url'] for job in all_jobs}
                    new_jobs = [job for job in current_page_jobs if job['url'] not in seen_urls]
                    
                    all_jobs.extend(new_jobs)
                    logger.info(f"Keyword '{keyword}': Added {len(new_jobs)} new jobs (Total: {len(all_jobs)})")
                    
                    # Delay between keywords
                    if index < len(target_keywords) - 1:
                        delay = random.uniform(5, 8)
                        logger.info(f"Resting {delay:.1f}s before next keyword...")
                        time.sleep(delay)
                        
                except Exception as e:
                    logger.error(f"Error processing keyword '{keyword}': {e}")
                    continue
            
            logger.info(f"Total scraped {len(all_jobs)} jobs from {len(target_keywords)} keywords")
            return all_jobs
            
        finally:
            # Ensure driver is closed
            self._close_driver()
    
    def save_to_mongodb(self, jobs: list) -> bool:
        """
        Save job list to MongoDB
        
        Args:
            jobs: List of job dicts
            
        Returns:
            True if successful, False if failed
        """
        if not jobs:
            logger.warning("No data to save")
            return False
        
        try:
            # Delete old data (optional - can comment out if want to append)
            # self.collection.delete_many({})
            # logger.info("Old data deleted")
            
            # Insert new
            result = self.collection.insert_many(jobs)
            logger.info(f"Saved {len(result.inserted_ids)} jobs to MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to MongoDB: {e}")
            return False
    
    def get_statistics(self) -> dict:
        """
        Get data statistics in MongoDB
        
        Returns:
            Dict containing statistics
        """
        try:
            total_jobs = self.collection.count_documents({})
            
            # Statistics by company
            companies = self.collection.distinct('company')
            
            # Statistics by location
            locations = self.collection.distinct('location')
            
            stats = {
                'total_jobs': total_jobs,
                'unique_companies': len(companies),
                'unique_locations': len(locations),
                'last_scraped': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Statistics: {json.dumps(stats, indent=2, ensure_ascii=False)}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}


# ===== Main execution (for testing) =====
if __name__ == "__main__":
    # MongoDB URI
    mongo_uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    
    # Initialize scraper
    scraper = ITviecScraper(
        mongo_uri=mongo_uri,
        db_name=MONGO_DB
    )
    
    try:
        # Connect to MongoDB
        scraper.connect_mongodb()
        
        # Scrape data
        logger.info(f"Start scraping from: {TARGET_URL}")
        jobs = scraper.scrape_jobs(TARGET_URL, max_pages=3)
        
        # Save to MongoDB
        success = scraper.save_to_mongodb(jobs)
        
        # Get statistics
        stats = scraper.get_statistics()
        
        logger.info("Completed!")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        
    finally:
        # Disconnect
        scraper.disconnect_mongodb()
