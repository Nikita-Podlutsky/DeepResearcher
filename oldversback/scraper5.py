import time
import os
import logging
import re
import random
from urllib.parse import urlparse, quote_plus
from typing import List, Dict, Any, Optional, Set, Tuple

# --- Scrapy Imports ---
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.spiders import Spider
from scrapy import signals
from scrapy.exceptions import CloseSpider, IgnoreRequest
from scrapy.http import HtmlResponse # To create response object from Selenium source

# --- Selenium Imports ---
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService # Use Service object
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
from duckduckgo_search import DDGS
import trafilatura
from newspaper import Article, ArticleException

# --- Дополнительные библиотеки для резервного поиска ---
import requests
from bs4 import BeautifulSoup
import urllib.parse

# --- Начальная настройка ---
load_dotenv()

# Настройка логирования
# (Keep previous logging setup)
logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING) # Less verbose Selenium logs
logging.getLogger('scrapy').propagate = False
logging.getLogger('duckduckgo_search').setLevel(logging.INFO)
logging.getLogger('urllib3').propagate = False
logging.getLogger('trafilatura').setLevel(logging.WARNING)
logging.getLogger('newspaper').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

logger = logging.getLogger('search_spider')
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# --- Конфигурация скрапинга ---
SEARCH_RESULTS_PER_QUERY = 8
SEARCH_DELAY = 4.0
MIN_CONTENT_LENGTH = 150
MAX_RETRIES = 2
DIVERSE_QUERY_COUNT = 2

# --- Selenium Configuration ---
USE_SELENIUM_FALLBACK = True # Set to False to disable Selenium
# Specify the path to your WebDriver executable if it's not in PATH
# e.g., '/path/to/your/chromedriver' or 'C:/path/to/your/chromedriver.exe'
WEBDRIVER_PATH = None # Set to your path or leave as None if in PATH
SELENIUM_BROWSER = 'chrome' # or 'firefox'
SELENIUM_WAIT_TIMEOUT = 20 # Max time Selenium waits for page elements (seconds)
SELENIUM_PAGE_LOAD_TIMEOUT = 30 # Max time Selenium waits for driver.get() (seconds)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/110.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
]
# Use a consistent User Agent for Selenium if needed, or random
SELENIUM_USER_AGENT = random.choice(USER_AGENTS)


# --- Вспомогательные функции (generate_alternative_queries, is_valid_url, fallback searches remain the same) ---
# ... (keep the helper functions from the previous version) ...
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
    return alternative_queries[:DIVERSE_QUERY_COUNT + 1]

def is_valid_url(url: Optional[str]) -> bool:
    """Проверяет, является ли URL подходящим для парсинга."""
    if not url or not isinstance(url, str): return False
    if not (url.startswith('http://') or url.startswith('https://')): return False
    excluded_extensions = ('.pdf', '.docx', '.xlsx', '.pptx', '.zip', '.rar', '.jpg', '.png', '.gif', '.mp3', '.mp4',
                          '.avi', '.exe', '.dmg', '.iso', '.xml', '.json', '.css', '.js', '.svg', '.webp', '.ico')
    try:
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        if path and path.endswith(excluded_extensions): return False
    except Exception: return False
    excluded_domains = ('facebook.com', 'twitter.com', 'instagram.com', 'youtube.com', 'tiktok.com', 'pinterest.com',
                       'linkedin.com', 't.me', 'telegram.org', 'vk.com', 'ok.ru', 'quora.com', 'reddit.com',
                       'amazon.', 'ebay.', 'aliexpress.', 'google.com/search', 'yandex.ru/search', 'bing.com/search',
                       'slideshare.net', 'scribd.com', 'academia.edu', 'researchgate.net',
                       'codepen.io', 'jsfiddle.net')
    try:
        domain = parsed_url.netloc.lower()
        if domain.startswith('www.'): domain = domain[4:]
        if domain and any(bad_domain in domain for bad_domain in excluded_domains): return False
    except Exception: pass
    if 'search' in url.lower() or 'find' in url.lower() or '?' in url and ('q=' in url or 'query=' in url): return False
    return True

def fallback_search_yandex(query: str, num_results: int = 10) -> List[Dict[str, str]]:
    """Резервный поиск через Yandex."""
    results = []
    encoded_query = quote_plus(query)
    search_url = f"https://yandex.ru/search/?text={encoded_query}&lr=213"
    headers = {'User-Agent': random.choice(USER_AGENTS), 'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3'}
    try:
        response = requests.get(search_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.select('li.serp-item h2 a[href]')
        found_count = 0
        for link in links:
            url = link.get('href')
            if url and url.startswith('http') and 'yandex.ru/clck/' not in url and is_valid_url(url):
                title = link.get_text(strip=True)
                results.append({'href': url, 'title': title})
                found_count += 1
                if found_count >= num_results: break
    except requests.exceptions.RequestException as e: logger.error(f"Ошибка Yandex Search ({type(e).__name__}): {e}")
    except Exception as e: logger.error(f"Неожиданная ошибка Yandex Search: {e}")
    # logger.info(f"Yandex fallback for '{query}' -> {len(results)} valid results.")
    return results

def fallback_search_bing(query: str, num_results: int = 10) -> List[Dict[str, str]]:
    """Резервный поиск через Bing."""
    results = []
    encoded_query = quote_plus(query)
    search_url = f"https://www.bing.com/search?q={encoded_query}"
    headers = {'User-Agent': random.choice(USER_AGENTS), 'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8'}
    try:
        response = requests.get(search_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.select("li.b_algo h2 a")
        found_count = 0
        for link in links:
            url = link.get('href')
            if url and is_valid_url(url):
                title = link.get_text(strip=True)
                results.append({'href': url, 'title': title})
                found_count += 1
                if found_count >= num_results: break
    except requests.exceptions.RequestException as e: logger.error(f"Ошибка Bing Search ({type(e).__name__}): {e}")
    except Exception as e: logger.error(f"Неожиданная ошибка Bing Search: {e}")
    # logger.info(f"Bing fallback for '{query}' -> {len(results)} valid results.")
    return results


# --- Улучшенный Spider с поддержкой Selenium ---

class EnhancedArticleSpider(Spider):
    name = 'enhanced_article_spider'

    # (Keep variables from previous version)
    urls_found_for_task: Dict[Tuple[str, str], Set[str]]
    urls_to_scrape: Dict[str, Dict[str, Any]]
    failed_searches: List[Dict[str, Any]]
    processed_urls: Set[str]
    visited_urls: Set[str]
    selenium_driver = None # Initialize Selenium driver variable

    def __init__(self, search_tasks: List[Dict[str, Any]] = None, results_per_query: int = 3, *args, **kwargs):
        super(EnhancedArticleSpider, self).__init__(*args, **kwargs)
        # (Initialization logic remains the same)
        if search_tasks is None or not isinstance(search_tasks, list): raise ValueError("...")
        if not search_tasks: raise ValueError("...")
        self.search_tasks = search_tasks
        self.results_per_query = results_per_query
        self.urls_found_for_task = {}
        self.urls_to_scrape = {}
        self.failed_searches = []
        self.processed_urls = set()
        self.visited_urls = set()
        self.selenium_driver = None # Ensure it's None initially
        self.use_selenium = USE_SELENIUM_FALLBACK and SELENIUM_AVAILABLE

        if self.use_selenium:
            self.logger.info("Selenium fallback is ENABLED.")
        elif USE_SELENIUM_FALLBACK and not SELENIUM_AVAILABLE:
            self.logger.warning("Selenium fallback requested but 'selenium' library not found. Disabling fallback.")
        else:
            self.logger.info("Selenium fallback is DISABLED.")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(EnhancedArticleSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        """Initialize Selenium WebDriver when spider starts."""
        if not self.use_selenium:
            return

        self.logger.info(f"Initializing Selenium WebDriver ({SELENIUM_BROWSER})...")
        try:
            service = None
            options = None

            if SELENIUM_BROWSER.lower() == 'chrome':
                options = ChromeOptions()
                options.add_argument("--headless")
                options.add_argument("--disable-gpu") # Often needed for headless mode
                options.add_argument("--no-sandbox") # Often needed in Docker/Linux environments
                options.add_argument("--disable-dev-shm-usage") # Overcome limited resource problems
                options.add_argument(f"user-agent={SELENIUM_USER_AGENT}") # Set user agent
                options.add_argument("--disable-blink-features=AutomationControlled") # Try to appear less like a bot
                options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation']) # Further hide automation
                options.add_experimental_option('useAutomationExtension', False)
                # Disable images for faster loading
                # options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

                if WEBDRIVER_PATH:
                    service = ChromeService(executable_path=WEBDRIVER_PATH)
                    self.selenium_driver = webdriver.Chrome(service=service, options=options)
                else: # Assume chromedriver is in PATH
                    self.selenium_driver = webdriver.Chrome(options=options) # Service auto-detects if in PATH

            elif SELENIUM_BROWSER.lower() == 'firefox':
                options = FirefoxOptions()
                options.add_argument("--headless")
                options.add_argument("--disable-gpu")
                options.set_preference("general.useragent.override", SELENIUM_USER_AGENT)
                # options.set_preference("permissions.default.image", 2) # Disable images

                if WEBDRIVER_PATH:
                    service = FirefoxService(executable_path=WEBDRIVER_PATH)
                    self.selenium_driver = webdriver.Firefox(service=service, options=options)
                else: # Assume geckodriver is in PATH
                    self.selenium_driver = webdriver.Firefox(options=options)

            else:
                self.logger.error(f"Unsupported Selenium browser: {SELENIUM_BROWSER}")
                self.use_selenium = False # Disable if browser is wrong
                return

            self.selenium_driver.set_page_load_timeout(SELENIUM_PAGE_LOAD_TIMEOUT)
            # Implicit wait can sometimes be problematic, prefer explicit waits
            # self.selenium_driver.implicitly_wait(5)
            self.logger.info("Selenium WebDriver initialized successfully.")

        except WebDriverException as e:
            self.logger.error(f"Failed to initialize Selenium WebDriver: {e}", exc_info=True)
            self.logger.error("Ensure the correct WebDriver is installed and its path is specified correctly (if needed).")
            self.selenium_driver = None # Ensure it's None if failed
            self.use_selenium = False # Disable Selenium usage if init fails
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during Selenium initialization: {e}", exc_info=True)
            self.selenium_driver = None
            self.use_selenium = False


    def spider_closed(self, spider):
        """Quit Selenium WebDriver when spider closes."""
        if self.selenium_driver:
            self.logger.info("Closing Selenium WebDriver...")
            try:
                self.selenium_driver.quit()
                self.logger.info("Selenium WebDriver closed.")
            except Exception as e:
                self.logger.error(f"Error closing Selenium WebDriver: {e}")

    # --- start_requests, _add_url_if_valid, _search_with_ddg, _search_with_fallback ---
    # These methods remain unchanged from the previous version.
    # Copy them here.
    def start_requests(self):
        self.logger.info("--- Starting Search Phase ---")
        search_requests_made = 0
        total_urls_collected = 0
        for task_index, task_info in enumerate(self.search_tasks):
            base_query = task_info.get('query')
            plan_item_id = task_info.get('plan_item_id', f'task_{task_index}')
            query_id = task_info.get('query_id', 'q_0')
            task_key = (plan_item_id, query_id)
            if not base_query:
                self.logger.warning(f"Skipping task with empty query: {task_info}")
                continue
            self.logger.info(f"\n--- Processing Task {task_index+1}/{len(self.search_tasks)} (ID: {task_key}): Base Query = '{base_query}' ---")
            self.urls_found_for_task[task_key] = set()
            alternative_queries = generate_alternative_queries(base_query)
            self.logger.info(f"Generated {len(alternative_queries)} query variations: {alternative_queries}")
            task_urls_found_count = 0
            attempted_queries = 0
            for query_index, query in enumerate(alternative_queries):
                if task_urls_found_count >= self.results_per_query:
                    self.logger.info(f"Target of {self.results_per_query} URLs reached for task {task_key}, stopping search variations.")
                    break
                self.logger.info(f"Trying query variation {query_index+1}/{len(alternative_queries)}: '{query}'")
                attempted_queries += 1
                new_urls_from_ddg = []
                try:
                    new_urls_from_ddg = self._search_with_ddg(query, task_info, task_key)
                    task_urls_found_count += len(new_urls_from_ddg)
                    search_requests_made += 1
                    self.logger.info(f"DDG added {len(new_urls_from_ddg)} new URLs. Total for task {task_key}: {task_urls_found_count}")
                except Exception as e: self.logger.error(f"Unexpected error during DDG search for '{query}': {e}")
                needs_pause = (query_index < len(alternative_queries) - 1) or (task_urls_found_count < self.results_per_query)
                if needs_pause:
                    delay = SEARCH_DELAY * (0.8 + 0.4 * random.random())
                    self.logger.info(f"Pausing for {delay:.2f}s before next search action")
                    time.sleep(delay)
                if not new_urls_from_ddg and query_index == 0 and task_urls_found_count < self.results_per_query:
                    self.logger.info(f"DDG found no new URLs for the primary query variation, trying fallback search...")
                    if task_urls_found_count < self.results_per_query:
                        try:
                            new_urls_from_yandex = self._search_with_fallback(query, task_info, task_key, 'yandex')
                            task_urls_found_count += len(new_urls_from_yandex)
                            self.logger.info(f"Yandex added {len(new_urls_from_yandex)} new URLs. Total for task {task_key}: {task_urls_found_count}")
                            if new_urls_from_yandex: time.sleep(SEARCH_DELAY * 0.5)
                        except Exception as e: self.logger.error(f"Unexpected error during Yandex fallback for '{query}': {e}")
                    if task_urls_found_count < self.results_per_query:
                        try:
                            new_urls_from_bing = self._search_with_fallback(query, task_info, task_key, 'bing')
                            task_urls_found_count += len(new_urls_from_bing)
                            self.logger.info(f"Bing added {len(new_urls_from_bing)} new URLs. Total for task {task_key}: {task_urls_found_count}")
                            if new_urls_from_bing: time.sleep(SEARCH_DELAY * 0.5)
                        except Exception as e: self.logger.error(f"Unexpected error during Bing fallback for '{query}': {e}")
            self.logger.info(f"--- Task {task_key} Search Summary ---")
            self.logger.info(f"Attempted {attempted_queries} query variations.")
            self.logger.info(f"Collected {task_urls_found_count} unique valid URLs for this task.")
            total_urls_collected += task_urls_found_count
            if task_urls_found_count == 0:
                self.failed_searches.append(task_info)
                self.logger.warning(f"❌ FAILED TASK: No URLs found for task {task_key} (query: '{base_query}') after all attempts.")
            if task_index < len(self.search_tasks) - 1:
                delay = SEARCH_DELAY * 1.2 * (0.9 + 0.2 * random.random())
                self.logger.info(f"--- Pausing for {delay:.2f}s before next task ---")
                time.sleep(delay)
        allowed_domains_set = set()
        for url in self.urls_to_scrape.keys():
            try:
                domain = urlparse(url).netloc
                if domain:
                    if domain.startswith('www.'): domain = domain[4:]
                    allowed_domains_set.add(domain)
            except Exception as e: self.logger.warning(f"Could not parse domain from URL '{url}': {e}")
        self.allowed_domains = list(allowed_domains_set)
        # self.logger.info(f"Configured allowed_domains: {len(self.allowed_domains)} domains") # Less verbose
        self.logger.info(f"\n--- Search Phase Complete ---")
        self.logger.info(f"Total unique URLs collected across all tasks: {len(self.urls_to_scrape)}")
        self.logger.info(f"Total search engine requests made (approx): {search_requests_made} (excluding fallbacks)")
        if self.failed_searches: self.logger.warning(f"Found {len(self.failed_searches)} tasks with zero results.")
        if not self.urls_to_scrape:
            self.logger.warning("No valid URLs found to scrape after all searches. Stopping spider.")
            return
        self.logger.info(f"\n--- Starting Scrapy Download Phase for {len(self.urls_to_scrape)} URLs ---")
        request_count = 0
        for url, task_info in self.urls_to_scrape.items():
            if url not in self.processed_urls:
                request_count += 1
                self.logger.debug(f"Yielding request {request_count}/{len(self.urls_to_scrape)}: {url}")
                self.processed_urls.add(url)
                yield scrapy.Request(url, callback=self.parse_article, errback=self.handle_error,
                    meta={'task_info': task_info, 'handle_httpstatus_list': [403, 404, 500, 503, 429, 502, 504], 'download_timeout': 30, 'retry_times': 0},
                    headers={'User-Agent': random.choice(USER_AGENTS)}
                )
            else: self.logger.debug(f"Skipping already processed URL: {url}")

    def _add_url_if_valid(self, url: str, task_info: Dict[str, Any], task_key: Tuple[str, str], source: str) -> bool:
        task_urls = self.urls_found_for_task.setdefault(task_key, set())
        if len(task_urls) >= self.results_per_query: return False
        if is_valid_url(url):
            if url not in self.urls_to_scrape:
                self.urls_to_scrape[url] = task_info
                task_urls.add(url)
                self.logger.debug(f"  [+] Added URL from {source}: {url} (Task: {task_key})")
                return True
            else:
                if url not in task_urls:
                    task_urls.add(url)
                    self.logger.debug(f"  [=] Added existing URL to task {task_key}: {url} (From: {source})")
                    return False
                else: return False
        else: return False

    def _search_with_ddg(self, query: str, task_info: Dict[str, Any], task_key: Tuple[str, str]) -> List[str]:
        newly_added_urls = []
        retry_count = 0
        task_urls = self.urls_found_for_task.setdefault(task_key, set())
        results_needed_for_task = self.results_per_query - len(task_urls)
        if results_needed_for_task <= 0: return newly_added_urls
        while retry_count < MAX_RETRIES:
            try:
                max_results_to_fetch = results_needed_for_task + 8
                self.logger.info(f"DDG search for '{query}' (Task: {task_key}, Attempt: {retry_count+1}/{MAX_RETRIES}, Need: {results_needed_for_task}, Fetching: {max_results_to_fetch})")
                with DDGS(headers={'User-Agent': random.choice(USER_AGENTS)}, timeout=20) as ddgs:
                    results_iterator = ddgs.text(query, max_results=max_results_to_fetch)
                    results_processed = 0
                    if results_iterator:
                        for r in results_iterator:
                            if len(self.urls_found_for_task[task_key]) >= self.results_per_query: break
                            if r and isinstance(r, dict) and 'href' in r:
                                url = r.get('href')
                                results_processed += 1
                                if self._add_url_if_valid(url, task_info, task_key, "DDG"):
                                    newly_added_urls.append(url)
                            else: self.logger.debug(f"  [DDG Invalid Result Format]: {r}")
                        self.logger.debug(f"DDG processed {results_processed} results for '{query}'.")
                        break
                    else:
                        self.logger.warning(f"DDG returned no results iterator for query '{query}'")
            except Exception as e: self.logger.error(f"Error during DDG search for '{query}' (Attempt {retry_count+1}): {type(e).__name__} - {e}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                retry_delay = SEARCH_DELAY * (0.5 + retry_count * 0.3)
                self.logger.info(f"Retrying DDG search in {retry_delay:.2f}s...")
                time.sleep(retry_delay)
            else: self.logger.warning(f"Max retries reached for DDG search on '{query}'.")
        return newly_added_urls

    def _search_with_fallback(self, query: str, task_info: Dict[str, Any], task_key: Tuple[str, str], search_engine: str) -> List[str]:
        newly_added_urls = []
        task_urls = self.urls_found_for_task.setdefault(task_key, set())
        results_needed_for_task = self.results_per_query - len(task_urls)
        if results_needed_for_task <= 0: return newly_added_urls
        self.logger.info(f"Trying fallback search via {search_engine.capitalize()} for '{query}' (Task: {task_key}, Need: {results_needed_for_task})")
        try:
            results = []
            if search_engine == 'yandex': results = fallback_search_yandex(query, num_results=results_needed_for_task + 5)
            elif search_engine == 'bing': results = fallback_search_bing(query, num_results=results_needed_for_task + 5)
            else: self.logger.error(f"Unknown fallback search engine: {search_engine}"); return newly_added_urls
            if results:
                results_processed = 0
                for r in results:
                    if len(self.urls_found_for_task[task_key]) >= self.results_per_query: break
                    url = r.get('href')
                    results_processed += 1
                    if self._add_url_if_valid(url, task_info, task_key, search_engine.capitalize()):
                        newly_added_urls.append(url)
                self.logger.debug(f"{search_engine.capitalize()} processed {results_processed} results for '{query}'.")
            else: self.logger.warning(f"No results from {search_engine.capitalize()} fallback for query '{query}'")
        except Exception as e: self.logger.error(f"Error during {search_engine.capitalize()} fallback search for '{query}': {type(e).__name__} - {e}")
        return newly_added_urls

    # --- parse_article (MODIFIED to include Selenium fallback) ---
    def parse_article(self, response):
        url = response.url
        task_info = response.meta.get('task_info', {})
        status = response.status
        self.visited_urls.add(url)

        self.logger.info(f"Processing response from: {url} (Status: {status})")

        if status >= 400:
             self.logger.warning(f"Received non-2xx status {status} for {url}. Skipping content parsing.")
             # Potentially trigger Selenium here for specific errors like 403?
             # if status == 403 and self.use_selenium:
             #    return self._parse_with_selenium(url, task_info) # Return the generator
             return # Skip for other errors

        content_type = response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore').lower()
        if 'html' not in content_type and 'text' not in content_type:
            self.logger.warning(f"Skipping non-HTML content: {url} (Type: {content_type})")
            return

        # Initial extraction attempt using Scrapy's response body
        extracted_text, extraction_method, title = self._extract_content_from_html(
            response.body, response.encoding, url
        )

        # Check if extraction was successful enough
        if extracted_text and len(extracted_text) >= MIN_CONTENT_LENGTH:
            self.logger.info(f"✅ Successfully extracted text via Scrapy/Libraries from: {url} (Method: {extraction_method}, Length: {len(extracted_text)})")
            yield self._create_item(task_info, url, title, extracted_text, extraction_method)

        # If Scrapy+Libraries failed or got too little text, try Selenium fallback
        elif self.use_selenium and self.selenium_driver:
            self.logger.warning(f"Initial extraction failed or yielded short text ({len(extracted_text or '')} chars) for {url}. Attempting Selenium fallback...")
            # Use 'yield from' if _parse_with_selenium becomes a generator
            # yield from self._parse_with_selenium(url, task_info)
            # Since _parse_with_selenium will yield the item directly, just call it
            try:
                # Directly yield the result from the selenium parsing attempt
                yield from self._parse_with_selenium(url, task_info)
            except Exception as e:
                 self.logger.error(f"Error occurred during Selenium fallback processing for {url}: {e}", exc_info=True)
                 # Optionally yield a failure item or just log
                 yield self._create_failure_item(task_info, url, title, "selenium_error")

        # If Selenium is disabled or failed, and initial extraction failed
        else:
            self.logger.warning(f"❌ Failed to extract significant text content from: {url} after all attempts (Selenium disabled or failed).")
            yield self._create_failure_item(task_info, url, title, "extraction_failed")


    def _extract_content_from_html(self, html_content: bytes, encoding: str, url: str) -> Tuple[Optional[str], Optional[str], str]:
        """Helper function to extract content using libraries from HTML source."""
        extracted_text = None
        extraction_method = None
        title = ""
        decoded_html = html_content.decode(encoding, errors='ignore')

        # 0. Extract Title (from initial HTML)
        try:
             temp_response = HtmlResponse(url=url, body=html_content, encoding=encoding)
             title = temp_response.css('title::text').get() or ""
             title = title.strip()
             if not title or title.lower() in ["home", "index", "blog", "article"]:
                  h1_text = temp_response.css('h1::text').get()
                  if h1_text: title = h1_text.strip()
        except Exception: pass # Ignore errors here

        # 1. Trafilatura
        try:
            text = trafilatura.extract(html_content, include_comments=False, include_tables=True,
                                       include_formatting=True, include_links=False, output_format='text', url=url)
            if text:
                text = text.strip()
                if len(text) >= MIN_CONTENT_LENGTH:
                    extracted_text = text
                    extraction_method = "trafilatura"
                    # self.logger.debug(f"  _extract: Trafilatura success (~{len(text)} chars)")
        except Exception as e: self.logger.debug(f"  _extract: Trafilatura failed for {url}: {e}")

        # 2. Newspaper3k
        if not extracted_text:
            try:
                article = Article(url=url, language='ru' if '.ru/' in url or '.рф/' in url else 'en')
                article.download(input_html=decoded_html) # Use decoded HTML
                article.parse()
                if article.text:
                     text = article.text.strip()
                     if len(text) >= MIN_CONTENT_LENGTH:
                        extracted_text = text
                        extraction_method = "newspaper3k"
                        # self.logger.debug(f"  _extract: Newspaper3k success (~{len(text)} chars)")
                        if not title and article.title: title = article.title.strip() # Update title if needed
            except Exception as e: self.logger.debug(f"  _extract: Newspaper3k failed for {url}: {e}")

        # 3. Simple HTML (BeautifulSoup) - Only if others failed significantly
        if not extracted_text and MIN_CONTENT_LENGTH > 50: # Avoid if min length is very small
             try:
                 soup = BeautifulSoup(html_content, 'lxml')
                 main_content = soup.find('main') or soup.find('article') or soup.find('div', role='main') or soup.find('div', class_=re.compile(r'(content|main|body|post|entry)', re.I)) or soup.body
                 if main_content:
                     for element in main_content.select('script, style, nav, footer, header, aside, form, iframe, noscript, .sidebar, #sidebar, .comments, #comments, .related-posts, .social-links, .ad, [aria-hidden="true"]'):
                         element.extract()
                     paragraphs = main_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'pre', 'code', 'td', 'th'])
                     text_parts = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
                     raw_text = '\n\n'.join(text_parts)
                     clean_text = re.sub(r'\s{2,}', ' ', raw_text).strip()
                     clean_text = re.sub(r'\n{3,}', '\n\n', clean_text)
                     if len(clean_text) >= MIN_CONTENT_LENGTH:
                         extracted_text = clean_text
                         extraction_method = "simple_html"
                         # self.logger.debug(f"  _extract: Simple HTML success (~{len(clean_text)} chars)")
             except Exception as e: self.logger.debug(f"  _extract: Simple HTML parsing failed for {url}: {e}")


        # Return extracted text, method used, and title found
        return extracted_text, extraction_method, title


    def _parse_with_selenium(self, url: str, task_info: Dict[str, Any]):
        """Fetches page with Selenium and attempts extraction again. Yields item on success."""
        if not self.use_selenium or not self.selenium_driver:
             self.logger.error(f"Selenium parsing called for {url} but Selenium is disabled or driver not initialized.")
             yield self._create_failure_item(task_info, url, "N/A", "selenium_disabled")
             return # Explicitly return instead of yielding None

        self.logger.info(f"🚀 Attempting Selenium fetch for: {url}")
        page_source = None
        selenium_title = None

        try:
            self.selenium_driver.get(url)

            # Wait for the body element to be present, indicating basic page load
            # A better wait might target a specific content container if known
            WebDriverWait(self.selenium_driver, SELENIUM_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            # Optional: Add a small explicit wait for JS rendering if needed
            # time.sleep(3)

            page_source = self.selenium_driver.page_source
            selenium_title = self.selenium_driver.title # Get title from Selenium

            if not page_source:
                 self.logger.warning(f"Selenium got empty page source for {url}")
                 yield self._create_failure_item(task_info, url, selenium_title, "selenium_empty_source")
                 return

            self.logger.info(f"Selenium fetch successful for {url}. Re-attempting extraction...")

            # Re-run extraction on Selenium's page source
            extracted_text, extraction_method, _ = self._extract_content_from_html(
                page_source.encode('utf-8'), 'utf-8', url # Assume UTF-8 from Selenium source
            )

            final_title = selenium_title or "No Title Found" # Use Selenium's title

            if extracted_text and len(extracted_text) >= MIN_CONTENT_LENGTH:
                 self.logger.info(f"✅ Successfully extracted text via Selenium from: {url} (Method: {extraction_method}-selenium, Length: {len(extracted_text)})")
                 yield self._create_item(task_info, url, final_title, extracted_text, f"{extraction_method}-selenium")
            else:
                 self.logger.warning(f"❌ Selenium fallback did not yield sufficient text ({len(extracted_text or '')} chars) for: {url}")
                 yield self._create_failure_item(task_info, url, final_title, "selenium_extraction_failed")


        except TimeoutException:
            self.logger.error(f"Selenium timed out waiting for page elements on: {url} (Timeout: {SELENIUM_WAIT_TIMEOUT}s)")
            yield self._create_failure_item(task_info, url, "N/A", "selenium_timeout")
        except WebDriverException as e:
            # Catch broader Selenium errors (e.g., navigation errors, crashes)
            self.logger.error(f"Selenium WebDriverException occurred for {url}: {e}")
            yield self._create_failure_item(task_info, url, "N/A", "selenium_webdriver_error")
        except Exception as e:
            self.logger.error(f"Unexpected error during Selenium processing for {url}: {e}", exc_info=True)
            yield self._create_failure_item(task_info, url, "N/A", "selenium_unexpected_error")


    def _create_item(self, task_info, url, title, text, method):
        """Helper to create a standard result item."""
        cleaned_text = re.sub(r'\s{2,}', ' ', text.strip())
        cleaned_text = re.sub(r'(\r\n|\r|\n){2,}', '\n\n', cleaned_text)
        return {
            'query': task_info.get('query'),
            'plan_item': task_info.get('plan_item'),
            'plan_item_id': task_info.get('plan_item_id'),
            'query_id': task_info.get('query_id'),
            'url': url,
            'title': title or "No Title Found",
            'text': cleaned_text,
            'extraction_method': method,
            'content_length': len(cleaned_text),
            'status': 'success' # Add status field
        }

    def _create_failure_item(self, task_info, url, title, failure_reason):
         """Helper to create an item indicating failure for a specific URL."""
         return {
            'query': task_info.get('query'),
            'plan_item': task_info.get('plan_item'),
            'plan_item_id': task_info.get('plan_item_id'),
            'query_id': task_info.get('query_id'),
            'url': url,
            'title': title or "N/A",
            'text': "",
            'extraction_method': "failed",
            'content_length': 0,
            'status': 'failure', # Add status field
            'failure_reason': failure_reason # Explain why it failed
        }


    # --- handle_error (Keep previous version or enhance) ---
    def handle_error(self, failure):
        request = failure.request
        url = request.url
        self.visited_urls.add(url)

        error_type = failure.type.__name__ if failure.type else 'Unknown Error'
        error_message = str(failure.value)

        self.logger.error(f"🕷️ Scrapy Request failed for URL: {url}")
        self.logger.error(f"  Error Type: {error_type}")
        self.logger.error(f"  Error Message: {error_message}")

        if request.meta:
            task_info = request.meta.get('task_info', {})
            retry_times = request.meta.get('retry_times', 0)
            self.logger.error(f"  Associated Query: '{task_info.get('query', 'N/A')}'")
            self.logger.error(f"  Scrapy Retry attempt: {retry_times}")

            # --- OPTIONAL: Selenium Fallback on Scrapy Error ---
            # Decide if you want to try Selenium even if the initial Scrapy request fails
            # For example, try Selenium on a timeout or a specific HTTP error like 403
            # Be careful, this can significantly slow down the process if many requests fail
            # should_try_selenium_on_error = (
            #     self.use_selenium and self.selenium_driver and
            #     (failure.check(IgnoreRequest) is None) and # Don't retry if we explicitly ignored it
            #     (failure.check(scrapy.spidermiddlewares.httperror.HttpError) and failure.value.response.status == 403) # Example: Retry on 403
            #     # or failure.check(twisted.internet.error.TimeoutError, twisted.internet.defer.TimeoutError) # Example: Retry on timeout
            # )
            #
            # if should_try_selenium_on_error:
            #     self.logger.warning(f"Scrapy request failed ({error_type}), attempting Selenium fallback for {url}...")
            #     yield from self._parse_with_selenium(url, task_info)
            # else:
            #      # Yield a failure item if not retrying with Selenium
            yield self._create_failure_item(task_info, url, "N/A", f"scrapy_error_{error_type}")

        else:
            # Yield failure if no meta
             yield self._create_failure_item({}, url, "N/A", f"scrapy_error_{error_type}_no_meta")


# --- Функция для запуска Scrapy из скрипта (run_enhanced_scrape) ---
# Needs minor adjustments to mention WebDriver setup

def run_enhanced_scrape(search_tasks: List[Dict[str, Any]], results_per_query: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Запускает улучшенный процесс поиска и скрапинга с опциональным Selenium fallback.

    Args:
        search_tasks: Список словарей с поисковыми задачами ('query' required).
        results_per_query: Целевое количество URL на задачу.

    Returns:
        Кортеж: (успешно/неуспешно спарсенные источники, задачи без найденных URL)
    """
    # (Input validation remains the same)
    if not search_tasks: logger.error("Нет задач."); return [], []
    if not isinstance(search_tasks, list) or not all(isinstance(task, dict) and 'query' in task for task in search_tasks):
        logger.error("Ошибка: 'search_tasks' должен быть списком словарей с ключом 'query'."); return [], []
    if not isinstance(results_per_query, int) or results_per_query <= 0:
        logger.error("Ошибка: 'results_per_query' должен быть > 0."); return [], []

    if USE_SELENIUM_FALLBACK:
        logger.info("--- Selenium Fallback Notes ---")
        if SELENIUM_AVAILABLE:
             logger.info("Selenium library found.")
             logger.info(f"Using browser: {SELENIUM_BROWSER}")
             if WEBDRIVER_PATH:
                 logger.info(f"Using WebDriver path: {WEBDRIVER_PATH}")
             else:
                 logger.info("WebDriver path not specified, assuming it's in system PATH.")
             logger.info("Ensure the correct WebDriver executable is installed and accessible.")
        else:
             logger.warning("Selenium fallback is enabled in config, BUT 'selenium' library is NOT installed.")
             logger.warning("Install it ('pip install selenium') and the appropriate WebDriver.")
        logger.info("---")


    logger.info(f"\n=== Запуск улучшенного поиска и скрапинга для {len(search_tasks)} задач ===")
    logger.info(f"Целевое количество URL на задачу: {results_per_query}")
    start_time = time.time()

    scraped_items = [] # Includes both success and failure items for URLs
    final_failed_searches = [] # Tasks where *no* URLs were found

    def item_scraped_handler(item, response, spider):
        if item and isinstance(item, dict):
            scraped_items.append(dict(item))
            status = item.get('status', 'unknown')
            spider.logger.info(f"Item collected (Status: {status}): {item.get('url')} (Query: '{item.get('query')}')")

    def spider_closed_handler(spider, reason):
        nonlocal final_failed_searches
        logger.info(f"Spider closed. Reason: {reason}")
        final_failed_searches = getattr(spider, 'failed_searches', [])
        visited = getattr(spider, 'visited_urls', set())
        processed = getattr(spider, 'processed_urls', set())
        logger.info(f"Spider stats: Processed {len(processed)} URL requests, Visited {len(visited)} URLs.")

    # --- Настройки Scrapy (Keep previous settings or adjust as needed) ---
    settings = get_project_settings()
    settings.set('LOG_LEVEL', 'INFO')
    settings.set('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    settings.set('LOG_DATEFORMAT', '%Y-%m-%d %H:%M:%S')
    settings.set('ROBOTSTXT_OBEY', False)
    settings.set('AUTOTHROTTLE_ENABLED', True)
    settings.set('DOWNLOAD_DELAY', 1.0)
    settings.set('AUTOTHROTTLE_MAX_DELAY', 15.0)
    settings.set('AUTOTHROTTLE_TARGET_CONCURRENCY', 1.0) # More conservative with Selenium potentially running
    settings.set('AUTOTHROTTLE_DEBUG', False)
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 1) # Keep low
    settings.set('CONCURRENT_REQUESTS', 4) # Reduce total concurrency if using Selenium frequently
    settings.set('USER_AGENT', random.choice(USER_AGENTS))
    settings.set('DOWNLOAD_TIMEOUT', 35)
    settings.set('DNS_TIMEOUT', 25)
    settings.set('REDIRECT_ENABLED', True)
    settings.set('RETRY_ENABLED', True)
    settings.set('RETRY_TIMES', 1) # Reduce Scrapy retries if Selenium handles failures
    settings.set('RETRY_HTTP_CODES', [500, 502, 503, 504, 522, 524, 408, 429])
    settings.set('COOKIES_ENABLED', False)

    # --- Запуск процесса ---
    process = CrawlerProcess(settings)
    # Crawler is created inside EnhancedArticleSpider.from_crawler now
    # crawler = process.create_crawler(EnhancedArticleSpider) # No need to create here

    # Connect signals here or rely on from_crawler
    # process.signals.connect(item_scraped_handler, signal=signals.item_scraped) # Handled in crawler now
    # process.signals.connect(spider_closed_handler, signal=signals.spider_closed) # Handled in crawler now

    logger.info("--- Starting CrawlerProcess ---")
    # Pass args to the spider constructor via process.crawl
    process.crawl(EnhancedArticleSpider, search_tasks=search_tasks, results_per_query=results_per_query)

    try:
        process.start()
        logger.info("--- CrawlerProcess finished successfully ---")
    except Exception as e:
        logger.error(f"--- CrawlerProcess encountered an error: {e} ---", exc_info=True)

    end_time = time.time()
    logger.info(f"\n=== Scrape Run Complete ===")
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds.")

    # Separate successful and failed items based on the 'status' field
    successful_items = [item for item in scraped_items if item.get('status') == 'success']
    failed_url_items = [item for item in scraped_items if item.get('status') == 'failure']

    logger.info(f"Collected {len(successful_items)} successfully scraped items.")
    if failed_url_items:
        logger.warning(f"Encountered {len(failed_url_items)} failures during URL processing (check logs and results file).")
    if final_failed_searches:
         logger.warning(f"Found {len(final_failed_searches)} tasks where no URLs could be found initially.")

    # Return all scraped items (success + failure) and the list of tasks where no URLs were found
    return scraped_items, final_failed_searches


# --- Пример Использования (Adjusted output) ---
if __name__ == '__main__':
    test_tasks = [
        {
            'query': "применение трансформеров в NLP",
            'plan_item': "Обзор трансформеров", 'plan_item_id': "plan_0", 'query_id': "q_0_0"
        },
        {
            'query': "React component lifecycle hooks", # JS heavy site example
            'plan_item': "React Lifecycle", 'plan_item_id': "plan_1", 'query_id': "q_1_0"
        },
         {
            'query': "python dynamic content loading example", # Another potential JS site
            'plan_item': "Dynamic Content", 'plan_item_id': "plan_4", 'query_id': "q_4_0"
        },
        {
            'query': "методы кластеризации данных",
            'plan_item': "Кластеризация", 'plan_item_id': "plan_2", 'query_id': "q_2_0"
        },
        {
            'query': "несуществующая чепуха абракадабра xyzzy", # Запрос без результатов
            'plan_item': "Тест ошибки", 'plan_item_id': "plan_3", 'query_id': "q_3_0"
        }
    ]
    num_sites_to_parse_per_query = 2

    print("\n--- Starting Test Scrape with Selenium Fallback ---")
    all_scraped_items, failed_search_tasks = run_enhanced_scrape(
        search_tasks=test_tasks,
        results_per_query=num_sites_to_parse_per_query
    )

    print(f"\n--- Scraping Finished ---")

    # Separate results
    successful_items = [item for item in all_scraped_items if item.get('status') == 'success']
    failed_items = [item for item in all_scraped_items if item.get('status') == 'failure']

    if successful_items:
        print(f"\n--- Successfully Scraped Content Summary ({len(successful_items)} items) ---")
        # (Optional: Grouping and detailed printing like before)
        for i, item in enumerate(successful_items[:5]): # Print first 5 successful
            print(f"\n Success Item {i+1}:")
            print(f"  Query: {item.get('query')}")
            print(f"  URL: {item.get('url')}")
            print(f"  Method: {item.get('extraction_method')}")
            print(f"  Title: {item.get('title', 'N/A')}")
            print(f"  Length: {item.get('content_length')}")
        if len(successful_items) > 5: print("  ...")

        try:
            with open("scraped_content_successful.json", "w", encoding="utf-8") as f:
                import json
                json.dump(successful_items, f, ensure_ascii=False, indent=2)
            print("\nFull successful results saved to scraped_content_successful.json")
        except Exception as e: print(f"\nFailed to save successful results to JSON: {e}")
    else:
        print("\n--- No content was successfully scraped ---")

    if failed_items:
        print(f"\n--- Failed URL Processing Summary ({len(failed_items)} items) ---")
        for i, item in enumerate(failed_items[:5]): # Print first 5 failures
             print(f"\n Failure Item {i+1}:")
             print(f"  Query: {item.get('query')}")
             print(f"  URL: {item.get('url')}")
             print(f"  Reason: {item.get('failure_reason')}")
        if len(failed_items) > 5: print("  ...")
        try:
            with open("scraped_content_failed_urls.json", "w", encoding="utf-8") as f:
                import json
                json.dump(failed_items, f, ensure_ascii=False, indent=2)
            print("\nFull failed URL results saved to scraped_content_failed_urls.json")
        except Exception as e: print(f"\nFailed to save failed URL results to JSON: {e}")

    if failed_search_tasks:
        print(f"\n--- Tasks With No URLs Found ({len(failed_search_tasks)}) ---")
        # (Printing logic remains the same)
        for i, task in enumerate(failed_search_tasks): print(f"  Task {i+1}: Query='{task.get('query')}', Plan='{task.get('plan_item')}'")
        try:
            with open("failed_search_tasks.json", "w", encoding="utf-8") as f:
                import json
                json.dump(failed_search_tasks, f, ensure_ascii=False, indent=2)
            print("\nList of tasks with no URLs found saved to failed_search_tasks.json")
        except Exception as e: print(f"\nFailed to save failed tasks list to JSON: {e}")
    else:
        print("\n--- All tasks had at least one URL found during the search phase ---")

    print("\n--- End of Script ---")