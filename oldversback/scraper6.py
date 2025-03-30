import time
import os
import logging
import re
import random
from urllib.parse import urlparse, quote_plus
from typing import List, Dict, Any, Optional, Set, Tuple
from collections import defaultdict
import asyncio # Although not used for async search in this version, kept for potential future use

# --- Scrapy Imports ---
import scrapy
from scrapy.crawler import CrawlerProcess, Crawler
from scrapy.utils.project import get_project_settings
from scrapy.spiders import Spider
from scrapy import signals
from scrapy.exceptions import CloseSpider, IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy.settings import Settings # To easily override settings

# --- Selenium Imports ---
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import WebDriverException, TimeoutException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    # Define dummy classes if Selenium is not installed, to avoid NameErrors later
    class WebDriverException(Exception): pass
    class TimeoutException(Exception): pass


# --- Other Libraries ---
from dotenv import load_dotenv
# --- REMOVED: from duckduckgo_search import DDGS --- # No longer needed
import trafilatura
from newspaper import Article, ArticleException
import requests
from bs4 import BeautifulSoup
import urllib.parse

# --- Начальная настройка ---
load_dotenv()

# Настройка логирования
logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
logging.getLogger('scrapy').propagate = False
# logging.getLogger('scrapy').setLevel(logging.INFO) # Will be set by runner function
# logging.getLogger('duckduckgo_search').setLevel(logging.INFO) # No longer needed
logging.getLogger('urllib3').propagate = False
logging.getLogger('trafilatura').setLevel(logging.WARNING)
logging.getLogger('newspaper').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING) # Less verbose requests logs

# Наш основной логгер
logger = logging.getLogger('search_yielding_spider_logger') # Changed name slightly
logger.setLevel(logging.INFO) # INFO for general flow, DEBUG for details
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# --- Configuration (Using Selenium for Search) ---
SEARCH_RESULTS_PER_QUERY = 10 # Target results per task
SEARCH_DELAY_BETWEEN_VARIATIONS = 4.0 # Increased delay between query variations when using Selenium search
SEARCH_DELAY_AFTER_TASK = 6.0   # Increased delay after finishing all variations for one task
MIN_CONTENT_LENGTH = 150
DIVERSE_QUERY_COUNT = 1 # Fewer variations initially

# --- Selenium Configuration ---
USE_SELENIUM_FALLBACK = True # Enable Selenium for both search and potential parsing fallback
WEBDRIVER_PATH = None # Set to your path (e.g., '/path/to/chromedriver') or leave as None if in PATH
SELENIUM_BROWSER = 'chrome' # or 'firefox'
SELENIUM_WAIT_TIMEOUT = 15 # Max time Selenium waits for elements during search/parse (seconds)
SELENIUM_PAGE_LOAD_TIMEOUT = 30 # Max time Selenium waits for driver.get() (seconds)

USER_AGENTS = [ # Diverse user agents
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36', # Updated Chrome
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15', # Updated Safari
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0', # Updated Firefox
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
]
SELENIUM_USER_AGENT = random.choice(USER_AGENTS) # User agent for Selenium requests

# --- Helper Functions ---

def generate_alternative_queries(original_query: str) -> List[str]:
    """Генерирует альтернативные формулировки исходного запроса."""
    query_templates = [
        "{} подробное объяснение",
        "что такое {}",
        "{} руководство",
        "{} документация",
        "{} примеры использования",
        "{} tutorial",
        "{} how to",
        "{} explained",
        "understanding {}",
        "{} guide",
        "{} best practices",
        "{} introduction"
    ]
    num_to_select = min(DIVERSE_QUERY_COUNT, len(query_templates))
    selected_templates = random.sample(query_templates, num_to_select)
    alternative_queries = [template.format(original_query) for template in selected_templates]
    if original_query not in alternative_queries:
        alternative_queries.insert(0, original_query)
    return alternative_queries[:DIVERSE_QUERY_COUNT + 1] # Limit total number

def is_valid_url(url: Optional[str]) -> bool:
    """Проверяет, является ли URL подходящим для парсинга (более строгая версия)."""
    if not url or not isinstance(url, str):
        return False
    if not (url.startswith('http://') or url.startswith('https://')):
        return False

    try:
        parsed_url = urlparse(url)
    except Exception as e:
        logger.debug(f"URL parsing failed for validation: {url} - {e}")
        return False # Cannot validate if cannot parse

    # 1. Check Scheme
    if parsed_url.scheme not in ('http', 'https'):
        return False

    # 2. Check Netloc (domain) - must exist
    if not parsed_url.netloc:
        return False

    # 3. Check File Extensions in Path
    excluded_extensions = ('.pdf', '.docx', '.xlsx', '.pptx', '.zip', '.rar', '.jpg', '.jpeg', '.png', '.gif', '.bmp',
                           '.mp3', '.wav', '.mp4', '.avi', '.mov', '.wmv', '.flv',
                           '.exe', '.dmg', '.iso', '.xml', '.json', '.css', '.js', '.svg', '.webp', '.ico', '.ttf', '.woff', '.woff2')
    path = parsed_url.path.lower()
    if path and path.endswith(excluded_extensions):
        # logger.debug(f"Excluding URL due to extension: {url}")
        return False

    # 4. Check Excluded Domains (more comprehensive list)
    excluded_domains = (
        'facebook.com', 'twitter.com', 'instagram.com', 'youtube.com', 'tiktok.com', 'pinterest.com', 'linkedin.com',
        't.me', 'telegram.org', 'vk.com', 'ok.ru', # Social media
        'wikipedia.org', # Often too general or requires specific handling
        'wikihow.com',
        'quora.com', 'reddit.com', 'stackexchange.com', 'stackoverflow.com', # Q&A sites
        'amazon.', 'ebay.', 'aliexpress.', 'walmart.com', 'target.com', # E-commerce
        'google.com', 'yandex.ru', 'bing.com', 'duckduckgo.com', # Search engines themselves
        'slideshare.net', 'scribd.com', 'academia.edu', 'researchgate.net', # Document sharing (often require login)
        'github.com', 'gitlab.com', # Code repositories (unless specifically targeting code)
        'codepen.io', 'jsfiddle.net', 'replit.com', # Code playgrounds
        'archive.org', # Web archive - handle separately if needed
        'goo.gl', 'bit.ly', 't.co', # URL shorteners
        'microsoft.com', # Often support/product pages, less tutorial content unless specific subdomain
        'apple.com',
        'adobe.com',
        'play.google.com', 'apps.apple.com', # App stores
        # Add any other domains consistently giving poor results
    )
    domain = parsed_url.netloc.lower()
    # Remove 'www.' prefix for matching
    if domain.startswith('www.'):
        domain = domain[4:]
    # Check if the domain *ends with* or *is* an excluded domain root
    if domain and any(domain == bad_domain or domain.endswith('.' + bad_domain) for bad_domain in excluded_domains if '.' in bad_domain) or \
       domain and any(domain == bad_domain for bad_domain in excluded_domains if '.' not in bad_domain): # Match TLDs like t.me
        # logger.debug(f"Excluding URL due to domain: {url}")
        return False

    # 5. Check for Common Search/Filter/Action Patterns in Path/Query
    path_lower = parsed_url.path.lower()
    query_lower = parsed_url.query.lower()
    fragment_lower = parsed_url.fragment.lower() # Check fragment too

    # Path patterns
    if any(p in path_lower for p in ['/search', '/find', '/query', '/login', '/register', '/signin', '/signup', '/cart', '/checkout', '/tag/', '/category/', '/author/']):
        # logger.debug(f"Excluding URL due to path pattern: {url}")
        return False
    # Query patterns (more specific checks)
    if any(qp + '=' in query_lower for qp in ['q', 'query', 'search', 'keyword', 'term', 'text', 's', 'find', 'sort', 'filter', 'order', 'page', 'paged', 'limit', 'offset']):
        # Be careful with 'page', might exclude valid multi-page articles if too broad
        # Check if it's likely just pagination vs. a primary search
        if 'page=' in query_lower or 'paged=' in query_lower:
            # Allow if other significant parameters are missing (might be pagination)
            other_params = query_lower.replace('page=', '').replace('paged=', '')
            if 'q=' in other_params or 'query=' in other_params or 'search=' in other_params:
                 # logger.debug(f"Excluding URL due to search query param with pagination: {url}")
                 return False
            # else: logger.debug(f"Allowing URL potentially using pagination: {url}") # Allow simple pagination
        else:
             # logger.debug(f"Excluding URL due to likely search/filter query param: {url}")
             return False # Exclude other search/filter params
    # Fragment patterns (less common for exclusion, but possible)
    if any(f in fragment_lower for f in ['search', 'login', 'register']):
         # logger.debug(f"Excluding URL due to fragment pattern: {url}")
         return False

    # 6. Basic check for excessive parameters (might indicate tracking or complex state)
    try:
         params = urllib.parse.parse_qs(parsed_url.query)
         if len(params) > 7: # Arbitrary threshold
              # logger.debug(f"Excluding URL due to excessive query parameters ({len(params)}): {url}")
              return False
    except Exception:
         pass # Ignore errors parsing query string

    return True

# --- Fallback Search Functions (Using requests - KEPT AS BACKUP/REFERENCE, but not used by default) ---
# These are kept here but the primary search mechanism in the spider is now _search_with_selenium
def fallback_search_yandex_requests(query: str, num_results: int = 10) -> List[Dict[str, str]]:
    # ... (Implementation using requests from previous examples) ...
    # ... (Includes improved headers and HTML saving for debugging) ...
    # --- THIS FUNCTION IS NOT CALLED BY DEFAULT IN THE SearchYieldingSpider ---
    results = []
    encoded_query = quote_plus(query); search_url = f"https://yandex.ru/search/?text={encoded_query}&lr=213"
    headers = { 'User-Agent': random.choice(USER_AGENTS), 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7', 'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8', 'Cache-Control': 'max-age=0', 'Connection': 'keep-alive', 'DNT': '1', 'Referer': 'https://yandex.ru/', 'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"', 'Sec-Ch-Ua-Mobile': '?0', 'Sec-Ch-Ua-Platform': '"Windows"', 'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'same-origin', 'Sec-Fetch-User': '?1', 'Upgrade-Insecure-Requests': '1', }
    html_filename = f"debug_yandex_req_{re.sub(r'[^a-z0-9]+', '_', query.lower())[:30]}.html"
    try:
        logger.debug(f"[Requests] Requesting Yandex: {search_url}")
        response = requests.get(search_url, headers=headers, timeout=15)
        logger.info(f"[Requests] Yandex response status code for '{query}': {response.status_code}")
        try:
            with open(html_filename, "w", encoding="utf-8") as f: f.write(f"<!-- URL: {search_url} -->\n<!-- Status Code: {response.status_code} -->\n\n{response.text}")
            logger.info(f"[Requests] Saved Yandex HTML response to '{html_filename}'")
        except Exception as save_err: logger.error(f"Failed to save Yandex debug HTML: {save_err}")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        selector = 'li.serp-item ul.serp-list li.serp-item h2 a[href]' # CHECK THIS SELECTOR
        links = soup.select(selector)
        if not links: selector = 'a.Link.OrganicTitle-Link[href]'; links = soup.select(selector) # Fallback selector
        if not links: logger.warning(f"[Requests] Yandex: No links found using selectors for query '{query}'. Check '{html_filename}'."); return []
        logger.debug(f"[Requests] Yandex: Found {len(links)} potential links.")
        found_count = 0; processed_urls = set()
        for link in links:
            url = link.get('href'); title = link.get_text(strip=True)
            if url and url.startswith('http') and 'yandex.ru/clck/' not in url:
                if url not in processed_urls and is_valid_url(url):
                    results.append({'href': url, 'title': title}); processed_urls.add(url); found_count += 1
                    if found_count >= num_results: break
    except requests.exceptions.Timeout: logger.error(f"[Requests] Yandex Search Timeout for query '{query}'")
    except requests.exceptions.RequestException as e: logger.error(f"[Requests] Yandex Search Request Error: {e}")
    except Exception as e: logger.error(f"[Requests] Unexpected Yandex Search Error: {e}", exc_info=True)
    logger.info(f"[Requests] Yandex fallback for '{query}' returning {len(results)} results.")
    return results

def fallback_search_bing_requests(query: str, num_results: int = 10) -> List[Dict[str, str]]:
    # ... (Implementation using requests from previous examples) ...
    # ... (Includes improved headers and HTML saving for debugging) ...
    # --- THIS FUNCTION IS NOT CALLED BY DEFAULT IN THE SearchYieldingSpider ---
    results = []
    encoded_query = quote_plus(query); search_url = f"https://www.bing.com/search?q={encoded_query}"
    headers = { 'User-Agent': random.choice(USER_AGENTS), 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7', 'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8', 'Cache-Control': 'max-age=0', 'Connection': 'keep-alive', 'DNT': '1', 'Referer': 'https://www.bing.com/', 'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"', 'Sec-Ch-Ua-Mobile': '?0', 'Sec-Ch-Ua-Platform': '"Windows"', 'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'same-origin', 'Sec-Fetch-User': '?1', 'Upgrade-Insecure-Requests': '1', }
    html_filename = f"debug_bing_req_{re.sub(r'[^a-z0-9]+', '_', query.lower())[:30]}.html"
    try:
        logger.debug(f"[Requests] Requesting Bing: {search_url}")
        response = requests.get(search_url, headers=headers, timeout=15)
        logger.info(f"[Requests] Bing response status code for '{query}': {response.status_code}")
        try:
            with open(html_filename, "w", encoding="utf-8") as f: f.write(f"<!-- URL: {search_url} -->\n<!-- Status Code: {response.status_code} -->\n\n{response.text}")
            logger.info(f"[Requests] Saved Bing HTML response to '{html_filename}'")
        except Exception as save_err: logger.error(f"Failed to save Bing debug HTML: {save_err}")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        selector = 'li.b_algo h2 a' # CHECK THIS SELECTOR
        links = soup.select(selector)
        if not links: logger.warning(f"[Requests] Bing: No links found using selector '{selector}' for query '{query}'. Check '{html_filename}'."); return []
        logger.debug(f"[Requests] Bing: Found {len(links)} potential links.")
        found_count = 0; processed_urls = set()
        for link in links:
            url = link.get('href'); title = link.get_text(strip=True)
            if url and url not in processed_urls and is_valid_url(url):
                results.append({'href': url, 'title': title}); processed_urls.add(url); found_count += 1
                if found_count >= num_results: break
    except requests.exceptions.Timeout: logger.error(f"[Requests] Bing Search Timeout for query '{query}'")
    except requests.exceptions.RequestException as e: logger.error(f"[Requests] Bing Search Request Error: {e}")
    except Exception as e: logger.error(f"[Requests] Unexpected Bing Search Error: {e}", exc_info=True)
    logger.info(f"[Requests] Bing fallback for '{query}' returning {len(results)} results.")
    return results


# --- Spider Class (Using Selenium for Search, Yielding from start_requests) ---

class SearchYieldingSpider(Spider):
    name = 'search_yielding_spider' # Spider name used by Scrapy

    # --- Class Attributes ---
    # Structure to hold task information and progress
    task_data: Dict[Tuple[str, str], Dict[str, Any]]
    # Set to keep track of all URLs for which requests have been yielded globally
    all_urls_yielded: Set[str]
    # Selenium WebDriver instance (initialized in spider_opened)
    selenium_driver = None
    # Flag indicating if Selenium should be used
    use_selenium: bool
    # Target number of results per query task
    results_per_query: int

    def __init__(self, search_tasks: List[Dict[str, Any]] = None, results_per_query: int = 3, *args, **kwargs):
        """Initializes the spider with search tasks and configuration."""
        super(SearchYieldingSpider, self).__init__(*args, **kwargs)
        if not search_tasks:
            raise ValueError("'search_tasks' list cannot be empty.")
        self.results_per_query = results_per_query
        self.task_data = {}
        self.all_urls_yielded = set()
        self.selenium_driver = None
        self.use_selenium = USE_SELENIUM_FALLBACK and SELENIUM_AVAILABLE

        # --- Prepare task data structure ---
        for i, task_info in enumerate(search_tasks):
            if not task_info.get('query'):
                 self.logger.warning(f"Skipping task with empty query: {task_info}")
                 continue
            # Generate default IDs if missing
            plan_item_id = task_info.get('plan_item_id', f'task_{i}')
            query_id = task_info.get('query_id', 'q_0')
            task_key = (plan_item_id, query_id)
            # Warn about duplicate task keys (plan_item_id, query_id combination)
            if task_key in self.task_data:
                 self.logger.warning(f"Duplicate task key detected: {task_key}. Check plan_item_id/query_id uniqueness. Overwriting previous task info.")
            # Store task details
            self.task_data[task_key] = {
                'info': task_info,          # Original task dictionary
                'target': results_per_query,# Target number of URLs for this task
                'found_urls': set(),       # URLs found specifically for this task (for summary)
                'yielded_count': 0         # Number of requests yielded for this task
            }

        # Log Selenium status clearly at initialization
        if self.use_selenium:
            self.logger.info("Selenium use is ENABLED (for search and potentially parsing fallback).")
        elif USE_SELENIUM_FALLBACK and not SELENIUM_AVAILABLE:
            self.logger.warning("Selenium use requested but 'selenium' library not found. DISABLING Selenium features.")
        else:
            self.logger.info("Selenium use is DISABLED.")


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Factory method used by Scrapy to create the spider instance."""
        spider = super(SearchYieldingSpider, cls).from_crawler(crawler, *args, **kwargs)
        # Connect spider methods to Scrapy signals for setup and teardown
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        """Called when the spider is opened. Initializes Selenium WebDriver if enabled."""
        if not self.use_selenium:
            self.logger.info("Spider opened. Selenium is disabled.")
            return

        self.logger.info(f"Spider opened. Initializing Selenium WebDriver ({SELENIUM_BROWSER})...")
        try:
            service = None
            options = None

            # --- Configure Chrome Options ---
            if SELENIUM_BROWSER.lower() == 'chrome':
                options = ChromeOptions()
                options.add_argument("--headless") # Run in headless mode (no GUI)
                options.add_argument("--disable-gpu") # Often needed for headless stability
                options.add_argument("--no-sandbox") # Often needed in Docker/Linux environments
                options.add_argument("--disable-dev-shm-usage") # Overcome shared memory limits
                options.add_argument(f"user-agent={SELENIUM_USER_AGENT}") # Set specific User-Agent
                # Attempts to make Selenium less detectable
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
                options.add_experimental_option('useAutomationExtension', False)
                # Performance optimization: disable images
                options.add_experimental_option("prefs", {
                    "profile.managed_default_content_settings.images": 2,
                    # "profile.managed_default_content_settings.stylesheets": 2 # Disabling CSS can break sites
                })

                # --- Initialize WebDriver (Chrome) ---
                if WEBDRIVER_PATH:
                    self.logger.info(f"Using specified ChromeDriver path: {WEBDRIVER_PATH}")
                    service = ChromeService(executable_path=WEBDRIVER_PATH)
                    self.selenium_driver = webdriver.Chrome(service=service, options=options)
                else: # Assume chromedriver is in the system's PATH
                    self.logger.info("Using ChromeDriver from system PATH.")
                    # If chromedriver is in PATH, Service is not explicitly needed for basic cases
                    self.selenium_driver = webdriver.Chrome(options=options)

            # --- Configure Firefox Options ---
            elif SELENIUM_BROWSER.lower() == 'firefox':
                options = FirefoxOptions()
                options.add_argument("--headless")
                options.add_argument("--disable-gpu")
                options.set_preference("general.useragent.override", SELENIUM_USER_AGENT)
                # Disable images in Firefox
                options.set_preference("permissions.default.image", 2)
                # options.set_preference("permissions.default.stylesheet", 2) # Disabling CSS

                 # --- Initialize WebDriver (Firefox) ---
                if WEBDRIVER_PATH:
                    self.logger.info(f"Using specified GeckoDriver path: {WEBDRIVER_PATH}")
                    service = FirefoxService(executable_path=WEBDRIVER_PATH)
                    self.selenium_driver = webdriver.Firefox(service=service, options=options)
                else: # Assume geckodriver is in PATH
                    self.logger.info("Using GeckoDriver from system PATH.")
                    self.selenium_driver = webdriver.Firefox(options=options)

            # --- Unsupported Browser ---
            else:
                self.logger.error(f"Unsupported Selenium browser configured: {SELENIUM_BROWSER}. Supported: 'chrome', 'firefox'. Disabling Selenium.")
                self.use_selenium = False
                return

            # --- Set Timeouts ---
            self.selenium_driver.set_page_load_timeout(SELENIUM_PAGE_LOAD_TIMEOUT)
            # Implicit waits are generally discouraged; use explicit WebDriverWait instead.
            # self.selenium_driver.implicitly_wait(5) # Avoid if possible

            self.logger.info("Selenium WebDriver initialized successfully.")

        # --- Handle Initialization Errors ---
        except WebDriverException as e:
            self.logger.error(f"Failed to initialize Selenium WebDriver: {e}", exc_info=True)
            self.logger.error("Ensure the correct WebDriver executable (e.g., chromedriver, geckodriver) matching your browser version is installed and accessible via system PATH or the 'WEBDRIVER_PATH' setting in the script.")
            self.selenium_driver = None # Ensure driver is None if init fails
            self.use_selenium = False # Disable Selenium features if driver failed
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during Selenium initialization: {e}", exc_info=True)
            self.selenium_driver = None
            self.use_selenium = False


    def spider_closed(self, spider):
        """Called when the spider finishes. Closes the Selenium WebDriver and logs summary."""
        # --- Close Selenium Driver ---
        if self.selenium_driver:
            self.logger.info("Closing Selenium WebDriver...")
            try:
                self.selenium_driver.quit() # Closes the browser and ends the WebDriver process
                self.logger.info("Selenium WebDriver closed.")
            except Exception as e:
                self.logger.error(f"Error occurred while closing Selenium WebDriver: {e}")
        else:
             self.logger.info("Spider closed. Selenium was not active.")

        # --- Log Final Summary ---
        # Called here ensures summary logs after all potential items are processed/errors handled.
        self.log_summary()

    def log_summary(self):
        """Logs summary statistics about the crawl results at the end."""
        self.logger.info("--- Final Crawl Summary ---")
        total_yielded = len(self.all_urls_yielded)
        tasks_failed_to_find_urls = 0
        tasks_met_target = 0
        tasks_below_target = 0

        # Analyze results per task
        for key, data in self.task_data.items():
            found_count = len(data['found_urls'])   # How many unique valid URLs were found for this task
            yielded_count = data['yielded_count'] # How many requests were actually yielded for this task
            target = data['target']             # The target number of URLs for this task

            if yielded_count == 0 and found_count == 0:
                 # This task truly failed to find any relevant URLs during search
                 tasks_failed_to_find_urls += 1
                 self.logger.warning(f"Task {key}: Found 0 URLs for query '{data['info']['query']}'")
            elif yielded_count >= target:
                 # Task met or exceeded the target number of yielded requests
                 tasks_met_target += 1
                 self.logger.info(f"Task {key}: Met/Exceeded Target. Found {found_count} URLs, Yielded {yielded_count} (Target: {target})")
            else: # yielded_count < target
                 # Task yielded some requests but did not meet the target
                 tasks_below_target += 1
                 self.logger.info(f"Task {key}: Below Target. Found {found_count} URLs, Yielded {yielded_count} (Target: {target})")

        # Log overall statistics
        self.logger.info(f"Total unique URLs yielded for scraping across all tasks: {total_yielded}")
        self.logger.info(f"Tasks that met/exceeded URL target: {tasks_met_target}")
        self.logger.info(f"Tasks below URL target: {tasks_below_target}")
        if tasks_failed_to_find_urls > 0:
             self.logger.warning(f"Total tasks where *no* valid URLs were found during search: {tasks_failed_to_find_urls}")


    def start_requests(self):
        """
        The entry point for Scrapy. Manages the search process and yields Scrapy Requests.
        This method is a generator. It iterates through tasks and query variations,
        performs searches (using Selenium in this version), and yields requests
        for found URLs immediately to the Scrapy engine.
        """
        self.logger.info(f"--- Starting Search & Yield Phase for {len(self.task_data)} tasks ---")
        if not self.task_data:
             self.logger.warning("No tasks loaded into spider. Nothing to search.")
             return # Exit generator if no tasks

        # --- Iterate through each task defined in __init__ ---
        task_counter = 0
        for task_key, data in self.task_data.items():
            task_counter += 1
            task_info = data['info']
            base_query = task_info['query']
            self.logger.info(f"\n--- Processing Task {task_counter}/{len(self.task_data)} (ID: {task_key}): Base Query = '{base_query}' ---")
            alternative_queries = generate_alternative_queries(base_query)

            # --- Iterate through query variations for the current task ---
            for query_index, query in enumerate(alternative_queries):
                # Check if target for this task has already been met before starting search
                if self.task_data[task_key]['yielded_count'] >= self.task_data[task_key]['target']:
                    self.logger.info(f"Target of {self.task_data[task_key]['target']} yielded URLs reached for task {task_key}. Skipping remaining variations.")
                    break # Stop trying variations for THIS task, move to the next task

                self.logger.info(f"Task {task_key}: Trying query variation {query_index+1}/{len(alternative_queries)}: '{query}'")

                # --- Perform Search using selected method ---
                search_results : List[Dict[str, str]] = [] # List to hold {'href': '...', 'title': '...'}
                try:
                    # Calculate how many more URLs are needed for this task right now
                    needed_urls = self.task_data[task_key]['target'] - self.task_data[task_key]['yielded_count']

                    # --- Primary Search Method: Selenium ---
                    if needed_urls > 0:
                        self.logger.debug(f"Task {task_key}: Calling Selenium search (Yandex, Need: {needed_urls})...")
                        # Use _search_with_selenium, requesting slightly more results for buffer
                        yandex_search_results = self._search_with_selenium(query, 'yandex', needed_urls + 3, task_key)
                        search_results.extend(yandex_search_results)
                        # Add a pause after using Selenium for one engine
                        if yandex_search_results:
                             time.sleep(random.uniform(1.5, 3.0))

                    # Recalculate needed URLs after the first search engine attempt
                    needed_urls = self.task_data[task_key]['target'] - self.task_data[task_key]['yielded_count']
                    if needed_urls > 0:
                         self.logger.debug(f"Task {task_key}: Calling Selenium search (Bing, Need: {needed_urls})...")
                         bing_search_results = self._search_with_selenium(query, 'bing', needed_urls + 3, task_key)
                         search_results.extend(bing_search_results)
                         # Optional shorter pause after the second engine
                         if bing_search_results:
                              time.sleep(random.uniform(0.5, 1.5))

                    # --- End Primary Search Method ---

                    # --- ALTERNATIVE: Fallback using requests (Uncomment to use instead of Selenium for search) ---
                    # if needed_urls > 0:
                    #     self.logger.debug(f"Task {task_key}: Calling fallback_search_yandex_requests (Need: {needed_urls})...")
                    #     search_results.extend(fallback_search_yandex_requests(query, needed_urls + 3))
                    #     time.sleep(random.uniform(0.5, 1.0)) # Short pause between requests fallbacks
                    # needed_urls = self.task_data[task_key]['target'] - self.task_data[task_key]['yielded_count']
                    # if needed_urls > 0:
                    #      self.logger.debug(f"Task {task_key}: Calling fallback_search_bing_requests (Need: {needed_urls})...")
                    #      search_results.extend(fallback_search_bing_requests(query, needed_urls + 3))
                    # --- End ALTERNATIVE ---

                    self.logger.debug(f"Task {task_key}: Search variation '{query}' returned {len(search_results)} total potential results.")

                    # --- Process search results and yield Scrapy Requests ---
                    if search_results:
                        processed_in_variation = 0
                        # Use a set to avoid yielding duplicate URLs found within the *same* search variation batch
                        yielded_in_variation = set()
                        for r in search_results:
                            # Check task target again inside the loop
                            if self.task_data[task_key]['yielded_count'] >= self.task_data[task_key]['target']:
                                break # Stop processing results if target met

                            result_url = r.get('href')
                            # Check if we already processed this specific URL in this batch
                            if result_url and result_url not in yielded_in_variation:
                                # Use 'yield from' because _yield_request_if_needed is a generator
                                # It will handle global uniqueness and task counts internally
                                yield from self._yield_request_if_needed(result_url, task_key, task_info, "search_results")
                                yielded_in_variation.add(result_url) # Mark as processed for this batch
                                processed_in_variation += 1

                        self.logger.debug(f"Task {task_key}: Processed {processed_in_variation} unique results from variation '{query}'.")
                    else:
                        # Log if a specific search variation yielded nothing
                        self.logger.warning(f"Task {task_key}: Search variation '{query}' returned no results from any engine.")

                except Exception as e:
                     # Catch errors during the search execution itself
                     self.logger.error(f"Task {task_key}: Error during search execution/processing for query '{query}': {e}", exc_info=True)

                # --- Delay between query variations for the same task ---
                # Apply delay only if target not met and there are more variations left
                if self.task_data[task_key]['yielded_count'] < self.task_data[task_key]['target'] and query_index < len(alternative_queries) - 1:
                     # Use a potentially longer delay since Selenium search is intensive
                     delay_variation = SEARCH_DELAY_BETWEEN_VARIATIONS * random.uniform(1.0, 1.5)
                     self.logger.debug(f"Pausing {delay_variation:.1f}s before next query variation...")
                     time.sleep(delay_variation)

            # --- Delay after finishing all variations for a task ---
            self.logger.debug(f"Finished all search variations for Task {task_key}.")
            # Apply delay before starting the *next task* in the outer loop
            if task_counter < len(self.task_data): # Only pause if not the last task
                delay_after_task = SEARCH_DELAY_AFTER_TASK * random.uniform(1.0, 1.5)
                self.logger.debug(f"Pausing {delay_after_task:.1f}s before starting next task...")
                time.sleep(delay_after_task)

        self.logger.info("--- Search & Yield Phase Completed for all tasks ---")
        # The generator naturally ends here after iterating through all tasks


    def _search_with_selenium(self, query: str, search_engine: str, num_results: int, task_key: Tuple[str,str]) -> List[Dict[str, str]]:
        """
        Performs a search using Selenium for the specified engine.

        Args:
            query: The search query string.
            search_engine: 'yandex' or 'bing'.
            num_results: The desired number of results (requests slightly more).
            task_key: The key identifying the current task (for logging).

        Returns:
            A list of dictionaries [{'href': url, 'title': title}] or an empty list on failure.
        """
        # Check if Selenium is available and initialized
        if not self.use_selenium or not self.selenium_driver:
            self.logger.warning(f"Task {task_key}: Selenium search requested for {search_engine} but Selenium not available/initialized.")
            return []

        results: List[Dict[str, str]] = []
        encoded_query = quote_plus(query)
        search_url = ""
        # --- !!! CRITICAL: UPDATE THESE SELECTORS BASED ON MANUAL INSPECTION !!! ---
        selector_yandex = 'li.serp-item ul.serp-list li.serp-item h2 a[href]' # Example Yandex Selector (Likely needs update)
        selector_bing = 'li.b_algo h2 a'                     # Example Bing Selector (Likely needs update)
        # --- !!! END CRITICAL SELECTOR SECTION !!! ---
        selector = ""

        # Configure URL and Selector based on the search engine
        if search_engine == 'yandex':
            search_url = f"https://yandex.ru/search/?text={encoded_query}&lr=213" # lr=213 Moscow
            selector = selector_yandex
        elif search_engine == 'bing':
            search_url = f"https://www.bing.com/search?q={encoded_query}&num={num_results+5}" # Ask Bing for more results
            selector = selector_bing
        else:
            self.logger.error(f"Task {task_key}: Unsupported search engine for Selenium search: {search_engine}")
            return []

        self.logger.debug(f"Task {task_key}: Selenium search ({search_engine}) executing for '{query}' at {search_url}")

        try:
            # Navigate to the search URL
            self.selenium_driver.get(search_url)

            # --- Wait for results to appear (Crucial Step) ---
            # Wait for the *first element* matching the main selector part to be present.
            # This is a basic wait; more robust waits might target a container div.
            wait_selector = selector.split(" ")[0] # e.g., 'li.serp-item' or 'li.b_algo'
            self.logger.debug(f"Task {task_key}: Waiting for element matching '{wait_selector}'...")
            WebDriverWait(self.selenium_driver, SELENIUM_WAIT_TIMEOUT + 5).until( # Allow slightly longer wait
                 EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
            )
            self.logger.debug(f"Task {task_key}: Initial element found. Pausing briefly for potential JS rendering...")
            # Short explicit pause can help ensure dynamic content loads after initial element appears
            time.sleep(random.uniform(2.0, 4.0)) # Adjust pause as needed

            # --- Get Page Source and Parse ---
            page_source = self.selenium_driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            links = soup.select(selector)

            # --- Log and Handle No Results ---
            if not links:
                 self.logger.warning(f"Task {task_key}: Selenium search ({search_engine}) found 0 links using selector '{selector}' for query '{query}'.")
                 self.logger.warning(f"Task {task_key}: Check if the selector is correct or if the page indicates blocking/CAPTCHA.")
                 # Save HTML for debugging when selectors fail
                 html_filename = f"debug_selenium_{search_engine}_{re.sub(r'[^a-z0-9]+', '_', query.lower())[:25]}.html"
                 try:
                     with open(html_filename, "w", encoding="utf-8") as f:
                          f.write(f"<!-- URL: {search_url} -->\n")
                          f.write(f"<!-- Query: {query} -->\n")
                          f.write(f"<!-- Selector Used: {selector} -->\n\n")
                          f.write(page_source)
                     self.logger.info(f"Task {task_key}: Saved potentially problematic Selenium {search_engine} HTML to '{html_filename}'")
                 except Exception as save_err:
                     self.logger.error(f"Task {task_key}: Failed to save debug HTML: {save_err}")
                 return [] # Return empty list if no links found

            self.logger.debug(f"Task {task_key}: Selenium search ({search_engine}) found {len(links)} potential link elements using '{selector}'.")

            # --- Extract Valid URLs ---
            found_count = 0
            processed_urls = set() # Track URLs found in this specific search instance
            for i, link in enumerate(links):
                url = link.get('href')
                title = link.get_text(strip=True)
                # self.logger.debug(f"  Raw Link {i+1}: Title='{title}', URL='{url}'") # Very verbose

                # Basic URL cleaning and validation
                if url and url.startswith('http'):
                     # Skip Yandex click-tracking URLs
                     if search_engine == 'yandex' and 'yandex.ru/clck/' in url:
                          # self.logger.debug(f"    -> Skipping Yandex click URL: {url}")
                          continue
                     # Check global validity and if already processed in this search
                     if url not in processed_urls and is_valid_url(url):
                         results.append({'href': url, 'title': title})
                         processed_urls.add(url)
                         found_count += 1
                         # self.logger.debug(f"    -> Added valid URL #{found_count} from Selenium {search_engine}")
                         # Stop if we've found enough valid results for this specific request
                         if found_count >= num_results:
                             self.logger.debug(f"Task {task_key}: Reached target ({num_results}) for Selenium {search_engine} search.")
                             break
                     # elif url in processed_urls: self.logger.debug(f"    -> Skipping duplicate URL within this search: {url}")
                     # else: self.logger.debug(f"    -> Skipping invalid URL (is_valid_url=False): {url}")
                # else: self.logger.debug(f"    -> Skipping URL with invalid scheme or None: {url}")


        # --- Handle Selenium Errors ---
        except TimeoutException:
            self.logger.error(f"Task {task_key}: Selenium search ({search_engine}) TIMED OUT waiting for page elements for query '{query}'. URL: {search_url}")
        except WebDriverException as e:
             # Catch broad Selenium errors (navigation, browser crashes, etc.)
             self.logger.error(f"Task {task_key}: Selenium WebDriverException during {search_engine} search for '{query}': {e}", exc_info=False) # Log less detail by default
        except Exception as e:
            # Catch any other unexpected errors during the Selenium process
            self.logger.error(f"Task {task_key}: Unexpected error during Selenium {search_engine} search for '{query}': {e}", exc_info=True) # Log full traceback

        # --- Log Final Result Count and Return ---
        self.logger.info(f"Task {task_key}: Selenium {search_engine} search for '{query}' returning {len(results)} valid results.")
        return results


    def _yield_request_if_needed(self, url: Optional[str], task_key: Tuple[str, str], task_info: Dict[str, Any], source: str):
        """
        Checks URL validity, global uniqueness, task limits, and yields Scrapy request.
        This is a generator method.
        """
        if not url:
            # self.logger.debug(f"Task {task_key}: Received None URL from {source}. Skipping.")
            return

        task_stats = self.task_data[task_key]

        # 1. Check if task target already met
        if task_stats['yielded_count'] >= task_stats['target']:
            return # Target met, don't yield more for this task

        # 2. Check URL validity using the helper function
        if not is_valid_url(url):
            # self.logger.debug(f"Task {task_key}: Invalid URL skipped ({source}): {url}")
            return

        # 3. Check if URL has already been yielded *globally* across all tasks
        if url in self.all_urls_yielded:
            # self.logger.debug(f"Task {task_key}: URL already yielded globally, skipping: {url} (From {source})")
            return

        # --- If all checks pass, proceed to yield ---
        self.logger.info(f"Task {task_key}: Yielding request [{task_stats['yielded_count']+1}/{task_stats['target']}] from {source}: {url}")

        # Mark URL as yielded globally *before* yielding
        self.all_urls_yielded.add(url)
        # Track found URL specifically for this task (for summary)
        task_stats['found_urls'].add(url)
        # Increment yielded count for this specific task
        task_stats['yielded_count'] += 1

        # Yield the Scrapy request to the engine
        yield scrapy.Request(
            url,
            callback=self.parse_article,
            errback=self.handle_error,
            meta={
                'task_info': task_info,         # Original task dictionary
                'task_key': task_key,           # Identifier for the task
                'handle_httpstatus_list': [403, 404, 500, 503, 429, 502, 504], # Handle these codes in parse_article
                'download_timeout': 35,         # Timeout for this specific download
                'retry_times': 0                # Scrapy retry middleware uses this
            },
            headers={'User-Agent': random.choice(USER_AGENTS)}, # Vary user agent per request
            # dont_filter=False # Let Scrapy's default duplicate filter run, our set handles cross-task uniqueness
        )


    def parse_article(self, response):
        """
        Handles the downloaded response from Scrapy. Extracts content or triggers Selenium fallback *for parsing*.
        Yields a success or failure item dictionary.
        """
        url = response.url
        task_info = response.meta.get('task_info', {})
        task_key = response.meta.get('task_key')
        status = response.status

        self.logger.debug(f"Processing response from: {url} (Status: {status}, Task: {task_key})")

        # 1. Check HTTP Status Code - handle_httpstatus_list brings errors here
        if status != 200:
             self.logger.warning(f"Task {task_key}: Received non-200 status {status} for {url}. Skipping content parsing.")
             # Optionally, you could trigger Selenium parsing fallback for specific errors like 403 here
             # if status == 403 and self.use_selenium and self.selenium_driver:
             #    self.logger.warning(f"Task {task_key}: Got 403 for {url}, attempting Selenium parsing fallback...")
             #    yield from self._parse_with_selenium_fallback(url, task_info, "N/A") # No initial title
             # else:
             #    yield self._create_failure_item(task_info, url, f"http_error_{status}")
             yield self._create_failure_item(task_info, url, f"http_error_{status}")
             return

        # 2. Check Content-Type
        content_type = response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore').lower()
        if not ('html' in content_type or 'text' in content_type or 'xml' in content_type): # Allow XML too
            self.logger.warning(f"Task {task_key}: Skipping non-HTML/text/XML content: {url} (Type: {content_type})")
            yield self._create_failure_item(task_info, url, "non_html_content")
            return

        # 3. Initial Content Extraction Attempt (using libraries on Scrapy response)
        try:
            # Call the helper function to try different libraries
            extracted_text, extraction_method, title = self._extract_content_from_html(
                response.body, response.encoding, url
            )
        except Exception as e:
            # Catch unexpected errors during the extraction process itself
            self.logger.error(f"Task {task_key}: Error during initial content extraction processing for {url}: {e}", exc_info=True)
            yield self._create_failure_item(task_info, url, "initial_extraction_error")
            return

        # 4. Decide whether to use Selenium *parsing* fallback
        # Stricter trigger: Only if initial extraction failed badly (very short or no text)
        should_try_selenium_parsing = (
            self.use_selenium and self.selenium_driver and
            (extracted_text is None or len(extracted_text) < 50) # Threshold for significant failure
        )

        # 5. Yield result or trigger Selenium parsing fallback
        if extracted_text and len(extracted_text) >= MIN_CONTENT_LENGTH:
            # Success with initial libraries
            self.logger.info(f"✅ Success (Libs): {url} (Task: {task_key}, Method: {extraction_method}, Length: {len(extracted_text)})")
            yield self._create_item(task_info, url, title, extracted_text, extraction_method)

        elif should_try_selenium_parsing:
            # Initial extraction failed, attempt Selenium parsing fallback
            self.logger.warning(f"Task {task_key}: Initial extraction insufficient for {url}. Attempting Selenium PARSING fallback...")
            # Yield result(s) from the Selenium parsing fallback generator
            yield from self._parse_with_selenium_fallback(url, task_info, title) # Pass initial title if found

        else:
            # Initial extraction failed/short, and Selenium parsing fallback not triggered/used
            fail_reason = "extraction_failed_short" if extracted_text else "extraction_failed_none"
            self.logger.warning(f"❌ Failed (Libs): {url} (Task: {task_key}, Reason: {fail_reason}, Selenium parsing not used)")
            yield self._create_failure_item(task_info, url, fail_reason, title=title)


    def _extract_content_from_html(self, html_content: bytes, encoding: str, url: str) -> Tuple[Optional[str], Optional[str], str]:
        """
        Helper: Extracts content using Trafilatura, Newspaper3k, and BeautifulSoup from HTML bytes.
        Returns: (extracted_text, method_used, title)
        """
        extracted_text: Optional[str] = None
        extraction_method: Optional[str] = None
        title: str = ""
        decoded_html: str = ""

        # --- Decode HTML (Best Effort) ---
        try:
            # Use Scrapy's HtmlResponse to handle encoding detection better if possible
            # Creating a temporary response might be slightly overhead, but robust
            temp_response_for_decode = HtmlResponse(url=url, body=html_content, encoding=encoding)
            decoded_html = temp_response_for_decode.text # Use Scrapy's decoded text
            detected_encoding = temp_response_for_decode.encoding # Store detected encoding
        except Exception as e:
             self.logger.debug(f"Failed to decode HTML robustly for {url}: {e}. Falling back to simple decode.")
             try:
                 # Fallback simple decode
                 decoded_html = html_content.decode(encoding or 'utf-8', errors='ignore')
                 detected_encoding = encoding or 'utf-8'
             except Exception as e2:
                  self.logger.warning(f"Further decode attempt failed for {url}: {e2}")
                  decoded_html = "" # Proceed with empty string if all fails
                  detected_encoding = 'unknown'

        # --- 0. Extract Title (using Scrapy selector on temp response) ---
        try:
             # Reuse temp response if created, otherwise create again
             temp_response = temp_response_for_decode if 'temp_response_for_decode' in locals() else HtmlResponse(url=url, body=html_content, encoding=detected_encoding)
             page_title = temp_response.css('title::text').get()
             title = page_title.strip() if page_title else ""
             # Try H1 if title is missing or generic
             generic_titles = {"home", "index", "blog", "article", "untitled document", "search results", "error"}
             if not title or title.lower() in generic_titles:
                  h1_text = temp_response.css('h1::text').get()
                  # Check if h1_text is meaningful
                  if h1_text and len(h1_text.strip()) > 3 and h1_text.strip().lower() not in generic_titles:
                      title = h1_text.strip()
        except Exception as e:
             self.logger.debug(f"Could not extract title using Scrapy selectors for {url}: {e}")
             title = ""

        # --- 1. Trafilatura (Primary) ---
        try:
            # Pass original bytes, Trafilatura handles encoding detection well
            text = trafilatura.extract(html_content,
                                       include_comments=False, include_tables=True,
                                       include_formatting=True, include_links=False,
                                       output_format='text', url=url,
                                       favor_recall=True) # Try favoring recall slightly
            if text:
                text = text.strip()
                if len(text) >= MIN_CONTENT_LENGTH:
                    extracted_text = text
                    extraction_method = "trafilatura"
                    # self.logger.debug(f"Success: Trafilatura ({len(text)} chars) for {url}")
                # else: self.logger.debug(f"Skipped: Trafilatura text too short ({len(text)} chars) for {url}")
            # else: self.logger.debug(f"Skipped: Trafilatura returned no text for {url}")
        except Exception as e:
            self.logger.debug(f"Error: Trafilatura failed for {url}: {e}")

        # --- 2. Newspaper3k (Fallback 1) ---
        if not extracted_text:
            # self.logger.debug(f"Trying Newspaper3k fallback for {url}")
            try:
                if decoded_html: # Newspaper needs decoded string
                    lang = 'ru' if '.ru/' in url or '.рф/' in url else 'en' # Basic language hint
                    article = Article(url=url, language=lang)
                    article.download(input_html=decoded_html)
                    article.parse()
                    if article.text:
                        text = article.text.strip()
                        if len(text) >= MIN_CONTENT_LENGTH:
                            extracted_text = text
                            extraction_method = "newspaper3k"
                            # self.logger.debug(f"Success: Newspaper3k ({len(text)} chars) for {url}")
                            # Update title if newspaper found a better one
                            if (not title or title.lower() in generic_titles) and article.title and article.title.strip().lower() not in generic_titles:
                                title = article.title.strip()
                        # else: self.logger.debug(f"Skipped: Newspaper3k text too short ({len(text)} chars) for {url}")
                    # else: self.logger.debug(f"Skipped: Newspaper3k parsed no text for {url}")
                # else: self.logger.debug(f"Skipped: Newspaper3k requires decoded HTML, which failed for {url}")
            except Exception as e: # Catch ArticleException and others
                self.logger.debug(f"Error: Newspaper3k failed for {url}: {e}")

        # --- 3. Simple HTML (BeautifulSoup - Fallback 2, only if others found *nothing*) ---
        if extracted_text is None: # Run only if previous attempts yielded absolutely nothing
            # self.logger.debug(f"Trying Simple HTML (BSoup) fallback for {url}")
            try:
                soup = BeautifulSoup(html_content, 'lxml') # Use lxml
                # More targeted content selectors
                content_selectors = ['main', 'article', '[role="main"]',
                                     '.content', '.entry-content', '.post-content', '.article-body', '.main-content', # Common class names
                                     '#content', '#main', # Common IDs
                                     ]
                main_content = None
                for selector in content_selectors:
                     main_content = soup.select_one(selector)
                     if main_content:
                          # self.logger.debug(f"BSoup found content block with selector: {selector}")
                          break # Use the first one found
                if not main_content:
                     # self.logger.debug("BSoup did not find specific content block, falling back to body.")
                     main_content = soup.body # Fallback to body if no main content found

                if main_content:
                    # Remove noise elements more aggressively
                    for element in main_content.select(
                        'script, style, link, meta, iframe, form, button, input, select, textarea, ' # Form elements, metadata
                        'nav, header, footer, aside, .sidebar, #sidebar, .widget, #widget, ' # Navigation/Sidebars
                        '.comments, #comments, .related, #related, .pagination, .breadcrumb, ' # Comments/Related/Nav aids
                        '.social, .share, .ads, .advertisement, .banner, [class*="ad-"], [id*="ad-"], ' # Social/Ads
                        '[aria-hidden="true"], .sr-only, .visually-hidden, .noprint, .printonly, ' # Accessibility/Print
                        'figure > figcaption' # Often redundant captions within figures
                    ):
                        element.extract()

                    # Extract text primarily from common block elements, join with newlines
                    text_blocks = main_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'pre', 'code', 'td', 'th', 'blockquote', 'dd', 'dt', 'div']) # Include blockquote, definition lists, divs
                    text_parts = []
                    min_block_len = 10 # Minimum characters for a block to be considered meaningful
                    for block in text_blocks:
                         # Heuristic: Skip blocks that likely contain only noise (e.g., short, only links, parent is noise)
                         parent_tags = {p.name for p in block.find_parents()}
                         if not any(noise in parent_tags for noise in ['nav', 'header', 'footer', 'aside', 'form']):
                              block_text = block.get_text(separator=' ', strip=True)
                              if len(block_text) >= min_block_len:
                                   text_parts.append(block_text)

                    raw_text = '\n\n'.join(text_parts)

                    # Cleaning
                    clean_text = re.sub(r'[ \t\r\f\v]+', ' ', raw_text).strip() # Collapse whitespace
                    clean_text = re.sub(r'\n{3,}', '\n\n', clean_text) # Collapse newlines

                    # Check length (use lower threshold for this last resort)
                    if len(clean_text) >= MIN_CONTENT_LENGTH:
                         extracted_text = clean_text
                         extraction_method = "simple_html"
                         # self.logger.debug(f"Success: Simple HTML (>=MIN_LEN, {len(clean_text)} chars) for {url}")
                    elif len(clean_text) >= 50: # Threshold for "maybe useful short text"
                         extracted_text = clean_text
                         extraction_method = "simple_html_short"
                         # self.logger.debug(f"Success: Simple HTML (short, {len(clean_text)} chars) for {url}")
                    # else: self.logger.debug(f"Skipped: Simple HTML text too short ({len(clean_text)} chars) for {url}")
                # else: self.logger.debug(f"Skipped: BSoup could not find suitable content block for {url}")
            except Exception as e:
                self.logger.debug(f"Error: Simple HTML (BSoup) failed for {url}: {e}")

        # Return final results
        return extracted_text, extraction_method, title


    def _parse_with_selenium_fallback(self, url: str, task_info: Dict[str, Any], initial_title: str = ""):
        """
        Fallback using Selenium to *re-fetch and parse* a page if initial libraries failed significantly.
        Yields a success or failure item. This is a generator.
        """
        # Check if Selenium is enabled and driver is ready
        if not self.use_selenium or not self.selenium_driver:
             self.logger.error(f"Task {task_info.get('plan_item_id', 'N/A')}: Selenium PARSING fallback requested for {url} but Selenium is disabled or not initialized.")
             yield self._create_failure_item(task_info, url, "selenium_disabled", title=initial_title)
             return # Stop generator

        self.logger.info(f"🚀 Attempting Selenium PARSING fallback for: {url}")
        page_source: Optional[str] = None
        selenium_title: Optional[str] = None

        try:
            # --- Navigate and Wait ---
            self.selenium_driver.get(url)
            # Wait for body, could potentially wait longer or for a specific element if known
            WebDriverWait(self.selenium_driver, SELENIUM_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            # Optional brief pause for JS execution after body is present
            # time.sleep(random.uniform(1.0, 2.0))

            # --- Get Rendered Source and Title ---
            page_source = self.selenium_driver.page_source
            selenium_title = self.selenium_driver.title # Title after JS execution

            # Check if source was retrieved
            if not page_source:
                 self.logger.warning(f"Task {task_info.get('plan_item_id', 'N/A')}: Selenium got empty page source during PARSING fallback for {url}")
                 yield self._create_failure_item(task_info, url, "selenium_empty_source", title=(selenium_title or initial_title))
                 return

            self.logger.info(f"Selenium fetch successful. Re-attempting extraction on rendered source for {url}...")

            # --- Re-run Standard Extraction on Selenium's Source ---
            extracted_text, extraction_method, _ = self._extract_content_from_html(
                page_source.encode('utf-8', errors='ignore'), # Encode back to bytes
                'utf-8', # Assume UTF-8 from Selenium
                url
            )

            # Determine final title (prefer Selenium's, fallback to initial)
            final_title = selenium_title or initial_title or "No Title Found"

            # --- Check Result and Yield ---
            if extracted_text and len(extracted_text) >= MIN_CONTENT_LENGTH:
                 # Success after Selenium parsing fallback
                 self.logger.info(f"✅ Success (Selenium Parse): {url} (Task: {task_info.get('plan_item_id', 'N/A')}, Method: {extraction_method}-selenium, Length: {len(extracted_text)})")
                 yield self._create_item(task_info, url, final_title, extracted_text, f"{extraction_method or 'unknown'}-selenium")
            else:
                 # Failed even after Selenium parsing fallback
                 fail_reason = "selenium_extract_short" if extracted_text else "selenium_extract_none"
                 self.logger.warning(f"❌ Failed (Selenium Parse): {url} (Task: {task_info.get('plan_item_id', 'N/A')}, Reason: {fail_reason} after Selenium fallback)")
                 yield self._create_failure_item(task_info, url, fail_reason, title=final_title)

        # --- Handle Selenium Errors during Fallback ---
        except TimeoutException:
            self.logger.error(f"Task {task_info.get('plan_item_id', 'N/A')}: Selenium PARSING fallback TIMED OUT for {url}")
            yield self._create_failure_item(task_info, url, "selenium_timeout_parse", title=initial_title)
        except WebDriverException as e:
            self.logger.error(f"Task {task_info.get('plan_item_id', 'N/A')}: Selenium WebDriverException during PARSING fallback for {url}: {e}", exc_info=False)
            yield self._create_failure_item(task_info, url, "selenium_webdriver_error_parse", title=initial_title)
        except Exception as e:
            self.logger.error(f"Task {task_info.get('plan_item_id', 'N/A')}: Unexpected error during Selenium PARSING fallback for {url}: {e}", exc_info=True)
            yield self._create_failure_item(task_info, url, "selenium_unexpected_error_parse", title=initial_title)


    def _create_item(self, task_info, url, title, text, method):
        """Helper: Creates a dictionary for a successful scrape result."""
        cleaned_text = text.strip()
        cleaned_text = re.sub(r'[ \t\r\f\v]+', ' ', cleaned_text) # Normalize whitespace
        cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)    # Normalize newlines
        return {
            'query': task_info.get('query'),
            'plan_item': task_info.get('plan_item'),
            'plan_item_id': task_info.get('plan_item_id'),
            'query_id': task_info.get('query_id'),
            'url': url,
            'title': title.strip() if title else "No Title Found",
            'text': cleaned_text,
            'extraction_method': method,
            'content_length': len(cleaned_text),
            'status': 'success' # Indicate success
        }

    def _create_failure_item(self, task_info, url, failure_reason, title="N/A"):
         """Helper: Creates a dictionary for a failed URL processing attempt."""
         safe_task_info = task_info if isinstance(task_info, dict) else {} # Ensure task_info is a dict
         return {
            'query': safe_task_info.get('query'),
            'plan_item': safe_task_info.get('plan_item'),
            'plan_item_id': safe_task_info.get('plan_item_id'),
            'query_id': safe_task_info.get('query_id'),
            'url': url,
            'title': title.strip() if title else "N/A",
            'text': "", # No text on failure
            'extraction_method': "failed",
            'content_length': 0,
            'status': 'failure', # Indicate failure
            'failure_reason': failure_reason # Provide reason
        }


    def handle_error(self, failure):
        """
        Handles errors during the Scrapy download phase (network errors, DNS errors, etc.).
        Yields a failure item.
        """
        request = failure.request
        url = request.url
        error_type = failure.type.__name__ if failure.type else 'UnknownDownloadError'

        # Log concisely
        self.logger.error(f"🕷️ Scrapy Download failed: {url} (Error: {error_type}, Value: {failure.value})", exc_info=False)

        # Yield a failure item
        if request.meta:
            task_info = request.meta.get('task_info', {})
            retry_times = request.meta.get('retry_times', 0) # Get Scrapy retry count
            yield self._create_failure_item(task_info, url, f"scrapy_download_error_{error_type}_retry{retry_times}")
        else:
             # Should always have meta, but handle just in case
             yield self._create_failure_item({}, url, f"scrapy_download_error_{error_type}_no_meta")



# --- Runner Function (Uses SearchYieldingSpider) ---

def run_search_yielding_scrape(search_tasks: List[Dict[str, Any]], results_per_query: int) -> List[Dict[str, Any]]:
    """
    Runs the scraping process using the SearchYieldingSpider.

    Args:
        search_tasks: List of task dictionaries (must contain 'query').
        results_per_query: Target number of URLs to find and process per task.

    Returns:
        A list containing all yielded items (dictionaries for successes and failures).
    """
    # --- Input Validation ---
    if not search_tasks:
        logger.error("No search tasks provided. Exiting.")
        return []
    if not isinstance(search_tasks, list) or not all(isinstance(task, dict) and 'query' in task for task in search_tasks):
        logger.error("Invalid 'search_tasks' format. Must be a list of dicts, each with a 'query' key.")
        return []
    if not isinstance(results_per_query, int) or results_per_query <= 0:
        logger.error("'results_per_query' must be a positive integer.")
        return []

    # --- Log Setup Messages ---
    if USE_SELENIUM_FALLBACK:
        logger.info("Selenium use is ENABLED (for search and potentially parsing fallback).")
        if not SELENIUM_AVAILABLE:
             logger.warning("However, 'selenium' library is NOT installed. Selenium features will be disabled.")
    else:
        logger.info("Selenium use is DISABLED.")

    logger.info(f"\n=== Starting Search-Yielding Scrape Run for {len(search_tasks)} tasks ===")
    logger.info(f"Target URLs per task: {results_per_query}")
    start_time = time.time()

    # --- Scraped Items Collector ---
    scraped_items: List[Dict[str, Any]] = [] # List to hold all items yielded by the spider

    # --- Scrapy Settings ---
    # Use MODERATE settings as Selenium search is slower and more resource-intensive
    settings = Settings()
    project_settings = get_project_settings() # Load scrapy.cfg settings if available
    settings.setdict(project_settings.copy(), priority='project')

    # Override specific settings
    settings.set('LOG_LEVEL', 'INFO') # Default log level
    settings.set('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    settings.set('LOG_DATEFORMAT', '%Y-%m-%d %H:%M:%S')
    settings.set('ROBOTSTXT_OBEY', False) # Be responsible if setting to False
    settings.set('COOKIES_ENABLED', False) # Disable cookies

    # Moderate Concurrency & Throttling
    settings.set('CONCURRENT_REQUESTS', 16) # Lower total concurrent Scrapy requests
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 2) # Limit requests per domain
    settings.set('AUTOTHROTTLE_ENABLED', True)
    settings.set('AUTOTHROTTLE_START_DELAY', 1.0) # Start with a small delay
    settings.set('DOWNLOAD_DELAY', 0.25) # Base delay, AutoThrottle adjusts
    settings.set('AUTOTHROTTLE_MAX_DELAY', 25.0) # Allow higher max delay if needed
    settings.set('AUTOTHROTTLE_TARGET_CONCURRENCY', 4.0) # Aim for lower average concurrency
    settings.set('AUTOTHROTTLE_DEBUG', False) # Set True to debug throttling

    # Timeouts & Retries
    settings.set('DOWNLOAD_TIMEOUT', 45) # Allow more time for downloads
    settings.set('DNS_TIMEOUT', 20)
    settings.set('RETRY_ENABLED', True) # Enable retries for transient errors
    settings.set('RETRY_TIMES', 2) # Number of retries
    settings.set('RETRY_HTTP_CODES', [500, 502, 503, 504, 522, 524, 408, 429]) # Retry on these codes

    # Other Settings
    settings.set('USER_AGENT', random.choice(USER_AGENTS)) # Default User-Agent
    settings.set('REDIRECT_ENABLED', True) # Follow redirects
    settings.set('REACTOR_THREADPOOL_MAXSIZE', 20) # For DNS lookups etc.
    # settings.set('HTTPCACHE_ENABLED', True) # Useful for development to avoid re-downloading

    # --- Scrapy Process Setup ---
    process = CrawlerProcess(settings)

    # --- Signal Handlers ---
    def item_scraped_handler(item, response, spider):
        """Appends yielded items to the list."""
        if item and isinstance(item, dict):
             scraped_items.append(dict(item)) # Append a copy

    def spider_closed_handler(spider, reason):
        """Logs when the spider closes."""
        # Summary is now logged by the spider's log_summary method
        logger.info(f"Spider closed signal received. Reason: {reason}")

    # --- Create and Schedule Crawler ---
    # Use the correct spider class name
    crawler = process.create_crawler(SearchYieldingSpider)

    # Connect signals
    crawler.signals.connect(item_scraped_handler, signal=signals.item_scraped)
    crawler.signals.connect(spider_closed_handler, signal=signals.spider_closed)

    # --- Start the Crawl ---
    logger.info("--- Starting CrawlerProcess (Search-Yielding Spider, Moderate Settings) ---")
    # Pass arguments to the spider's __init__ method
    process.crawl(crawler, search_tasks=search_tasks, results_per_query=results_per_query)

    # Start the Scrapy event loop (blocking)
    try:
        process.start()
        logger.info("--- CrawlerProcess finished naturally ---")
    except Exception as e:
        logger.error(f"--- CrawlerProcess terminated with an error: {e} ---", exc_info=True)

    # --- Post-Crawl ---
    end_time = time.time()
    logger.info(f"\n=== Scrape Run Complete ===")
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds.")
    logger.info(f"Total items yielded by spider (includes successes and failures): {len(scraped_items)}")
    # Detailed task summary logged by spider.log_summary()

    return scraped_items # Return the list of all collected items


# --- Example Usage (__main__ block) ---
if __name__ == '__main__':
    # Define the list of search tasks
    test_tasks = [
        {'query': "применение трансформеров в NLP", 'plan_item': "Обзор трансформеров", 'plan_item_id': "plan_0", 'query_id': "q_0_0"},
        {'query': "React component lifecycle hooks", 'plan_item': "React Lifecycle", 'plan_item_id': "plan_1", 'query_id': "q_1_0"},
        {'query': "python dynamic content loading example", 'plan_item': "Dynamic Content", 'plan_item_id': "plan_4", 'query_id': "q_4_0"},
        {'query': "методы кластеризации данных", 'plan_item': "Кластеризация", 'plan_item_id': "plan_2", 'query_id': "q_2_0"},
        {'query': "fastapi background tasks tutorial", 'plan_item': "FastAPI Tasks", 'plan_item_id': "plan_5", 'query_id': "q_5_0"},
        {'query': "asyncio python web scraping", 'plan_item': "Asyncio Scraping", 'plan_item_id': "plan_6", 'query_id': "q_6_0"},
        {'query': "что такое vector database", 'plan_item': "Vector DB Intro", 'plan_item_id': "plan_7", 'query_id': "q_7_0"},
        {'query': "несуществующая чепуха абракадабра xyzzy фываолдж", 'plan_item': "Тест ошибки", 'plan_item_id': "plan_3", 'query_id': "q_3_0"}
    ]
    # Define how many URLs we ideally want per task
    num_sites_to_parse_per_query = 10 # Increased target

    print("\n--- Starting Search-Yielding Scrape Test ---")

    # --- Run the Scraper ---
    # Call the runner function that uses the SearchYieldingSpider
    all_items_collected = run_search_yielding_scrape(
        search_tasks=test_tasks,
        results_per_query=num_sites_to_parse_per_query
    )

    print(f"\n--- Scraping Process Finished ---")

    # --- Process and Save Results ---
    if all_items_collected:
        # Separate successful items from failed URL processing attempts
        successful_items = [item for item in all_items_collected if item.get('status') == 'success']
        failed_items = [item for item in all_items_collected if item.get('status') == 'failure']

        print(f"\n--- Results Summary ---")
        print(f"Total items processed (success + failure): {len(all_items_collected)}")
        print(f"  Successfully scraped items: {len(successful_items)}")
        print(f"  Failed URL processing attempts: {len(failed_items)}")

        # Save all collected items to a JSON file
        output_filename = "scraped_yield_search_all.json"
        try:
            import json
            # Use utf-8 encoding and ensure_ascii=False for non-Latin characters
            with open(output_filename, "w", encoding="utf-8") as f:
                json.dump(all_items_collected, f, ensure_ascii=False, indent=2)
            print(f"\nFull results (success + failure) saved to '{output_filename}'")

            # Optionally save successful items to a separate file
            # output_success_filename = "scraped_yield_search_successful.json"
            # with open(output_success_filename, "w", encoding="utf-8") as f:
            #     json.dump(successful_items, f, ensure_ascii=False, indent=2)
            # print(f"Successful results saved separately to '{output_success_filename}'")

        except Exception as e:
            print(f"\nError saving results to JSON: {e}")
    else:
        # This message appears if the spider yielded absolutely nothing
        print("\n--- No items were collected during the scrape run. ---")
        print("--- This might indicate persistent search failures or immediate closure. Check logs. ---")

    print("\n--- End of Script ---")