
import logging
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_html_sample():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # Anti-detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    
    try:
        url = "https://itviec.com/it-jobs/java"
        logger.info(f"Fetching {url}...")
        driver.get(url)
        time.sleep(5) # Wait for load
        
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find job cards
        # Try generic selectors first to see what we get
        job_cards = soup.find_all('div', class_='job')
        if not job_cards:
            job_cards = soup.find_all('div', class_='job-card')
        if not job_cards:
            # Try finding by some known text or structure if classes changed
            logger.info("Standard classes not found, dumping body structure...")
            print(soup.body.prettify()[:2000]) # Print first 2000 chars of body
            return

        logger.info(f"Found {len(job_cards)} job cards.")
        
        if job_cards:
            first_card = job_cards[0]
            print("=== FIRST JOB CARD HTML ===")
            print(first_card.prettify())
            print("===========================")
            
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    get_html_sample()
