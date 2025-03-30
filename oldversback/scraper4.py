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
from scrapy.exceptions import CloseSpider

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
# Уменьшаем шум от библиотек
logging.getLogger('scrapy').propagate = False
# logging.getLogger('scrapy').setLevel(logging.INFO) # Scrapy будет управляться через настройки ниже
logging.getLogger('duckduckgo_search').setLevel(logging.INFO)
logging.getLogger('urllib3').propagate = False
logging.getLogger('trafilatura').setLevel(logging.WARNING)
logging.getLogger('newspaper').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING) # Уменьшаем шум от requests

# Создаем свой логгер для отслеживания процесса
logger = logging.getLogger('search_spider')
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s', '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Конфигурация скрапинга ---
SEARCH_RESULTS_PER_QUERY = 8  # Увеличим количество запрашиваемых результатов
SEARCH_DELAY = 4.0  # Немного уменьшаем задержку между запросами поисковиков
MIN_CONTENT_LENGTH = 150  # Минимальная длина текста
MAX_RETRIES = 2  # Максимальное количество попыток для поиска (DDG)
DIVERSE_QUERY_COUNT = 2  # Количество альтернативных формулировок запроса

# Список альтернативных user agents для запросов
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36', # More recent
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1' # Mobile
]

# --- Вспомогательные функции ---

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

    # Выбираем несколько шаблонов случайным образом, стараясь не повторяться сильно
    num_to_select = min(DIVERSE_QUERY_COUNT, len(query_templates))
    selected_templates = random.sample(query_templates, num_to_select)
    alternative_queries = [template.format(original_query) for template in selected_templates]

    # Добавляем исходный запрос в начало списка, если его там еще нет
    if original_query not in alternative_queries:
        alternative_queries.insert(0, original_query)

    return alternative_queries[:DIVERSE_QUERY_COUNT + 1] # Ограничиваем общее число

def is_valid_url(url: Optional[str]) -> bool:
    """Проверяет, является ли URL подходящим для парсинга."""
    if not url or not isinstance(url, str):
        return False

    # Базовые проверки
    if not (url.startswith('http://') or url.startswith('https://')):
        return False

    # Проверка на расширения файлов
    excluded_extensions = ('.pdf', '.docx', '.xlsx', '.pptx', '.zip', '.rar', '.jpg', '.png', '.gif', '.mp3', '.mp4',
                          '.avi', '.exe', '.dmg', '.iso', '.xml', '.json', '.css', '.js', '.svg', '.webp', '.ico')
    try:
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        if path and path.endswith(excluded_extensions):
            return False
    except Exception:
        # Если URL не парсится, считаем его невалидным
        return False

    # Исключаем нежелательные домены (соцсети, агрегаторы вопросов-ответов с низким качеством контента и т.д.)
    excluded_domains = ('facebook.com', 'twitter.com', 'instagram.com', 'youtube.com', 'tiktok.com', 'pinterest.com',
                       'linkedin.com', 't.me', 'telegram.org', 'vk.com', 'ok.ru', 'quora.com', 'reddit.com',
                       'amazon.', 'ebay.', 'aliexpress.', 'google.com/search', 'yandex.ru/search', 'bing.com/search',
                       'slideshare.net', 'scribd.com', 'academia.edu', 'researchgate.net', # Часто требуют логин
                       'codepen.io', 'jsfiddle.net' # Песочницы кода, не статьи
                       )
    try:
        domain = parsed_url.netloc.lower()
        # Убираем 'www.' для сравнения
        if domain.startswith('www.'):
            domain = domain[4:]
        if domain and any(bad_domain in domain for bad_domain in excluded_domains):
            #logger.debug(f"Excluding URL due to domain: {url}")
            return False
    except Exception:
        # Если не удалось получить домен, пропускаем проверку
        pass

    # Исключаем URL, которые выглядят как поиск внутри сайта
    if 'search' in url.lower() or 'find' in url.lower() or '?' in url and ('q=' in url or 'query=' in url):
        # logger.debug(f"Excluding URL due to potential search path: {url}")
        return False

    return True

def fallback_search_yandex(query: str, num_results: int = 10) -> List[Dict[str, str]]:
    """Резервный поиск через Yandex (без использования API)."""
    results = []
    encoded_query = quote_plus(query)
    # Используем lr=213 для Москвы, чтобы получить более релевантные результаты на русском
    search_url = f"https://yandex.ru/search/?text={encoded_query}&lr=213"

    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Referer': 'https://yandex.ru/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }

    try:
        #logger.info(f"Fallback search: Requesting Yandex for '{query}'")
        response = requests.get(search_url, headers=headers, timeout=15) # Увеличим таймаут
        response.raise_for_status() # Проверка на HTTP ошибки

        soup = BeautifulSoup(response.text, 'html.parser')
        # Ищем ссылки в результатах поиска. Селекторы могут меняться!
        # Используем более общий селектор для ссылки внутри заголовка результата
        links = soup.select('li.serp-item h2 a[href]')

        found_count = 0
        for link in links:
            url = link.get('href')
            # Яндекс может добавлять мусор, чистим URL
            if url and url.startswith('http') and 'yandex.ru/clck/' not in url:
                 # Проверяем валидность *после* базовой очистки
                if is_valid_url(url):
                    title = link.get_text(strip=True)
                    # logger.debug(f"  [Yandex Found]: {title} - {url}")
                    results.append({'href': url, 'title': title})
                    found_count += 1
                    if found_count >= num_results:
                        break
            # else: logger.debug(f"  [Yandex Skipped Invalid URL]: {url}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при поиске через Yandex ({type(e).__name__}): {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при поиске через Yandex: {e}")

    logger.info(f"Yandex fallback for '{query}' returned {len(results)} valid results.")
    return results

def fallback_search_bing(query: str, num_results: int = 10) -> List[Dict[str, str]]:
    """Резервный поиск через Bing (без использования API)."""
    results = []
    encoded_query = quote_plus(query)
    search_url = f"https://www.bing.com/search?q={encoded_query}"

    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8', # Добавим русский язык
        'Referer': 'https://www.bing.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    try:
        #logger.info(f"Fallback search: Requesting Bing for '{query}'")
        response = requests.get(search_url, headers=headers, timeout=15) # Увеличим таймаут
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        # Селектор для ссылок в результатах Bing (может измениться)
        links = soup.select("li.b_algo h2 a")

        found_count = 0
        for link in links:
            url = link.get('href')
            if url and is_valid_url(url): # Bing обычно дает чистые URL
                title = link.get_text(strip=True)
                # logger.debug(f"  [Bing Found]: {title} - {url}")
                results.append({'href': url, 'title': title})
                found_count += 1
                if found_count >= num_results:
                    break
            # else: logger.debug(f"  [Bing Skipped Invalid URL]: {url}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при поиске через Bing ({type(e).__name__}): {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при поиске через Bing: {e}")

    logger.info(f"Bing fallback for '{query}' returned {len(results)} valid results.")
    return results

# --- Улучшенный Spider ---

class EnhancedArticleSpider(Spider):
    name = 'enhanced_article_spider'

    # Переменные для отслеживания прогресса и результатов
    urls_found_for_task: Dict[Tuple[str, str], Set[str]] # {(plan_item_id, query_id): {set_of_urls}}
    urls_to_scrape: Dict[str, Dict[str, Any]]           # {url: original_task_info}
    failed_searches: List[Dict[str, Any]]               # Отслеживаем неудачные *задачи* (если ни один URL не найден)
    processed_urls: Set[str]                            # URL, для которых уже был yield Request
    visited_urls: Set[str]                              # URL, которые были успешно или неуспешно обработаны parse_article/handle_error

    def __init__(self, search_tasks: List[Dict[str, Any]] = None, results_per_query: int = 3, *args, **kwargs):
        super(EnhancedArticleSpider, self).__init__(*args, **kwargs)
        if search_tasks is None or not isinstance(search_tasks, list):
            raise ValueError("Spider needs 'search_tasks' argument (list of dicts)")
        if not search_tasks:
            raise ValueError("'search_tasks' list cannot be empty.")

        self.search_tasks = search_tasks
        self.results_per_query = results_per_query

        # Инициализация структур данных
        self.urls_found_for_task = {}
        self.urls_to_scrape = {}
        self.failed_searches = []
        self.processed_urls = set()
        self.visited_urls = set()

        self.logger.info(f"Spider initialized for {len(search_tasks)} search tasks (target: {results_per_query} results per query).")

    def start_requests(self):
        # Шаг 1: Последовательный поиск URL для всех запросов
        self.logger.info("--- Starting Search Phase ---")
        search_requests_made = 0
        total_urls_collected = 0

        for task_index, task_info in enumerate(self.search_tasks):
            base_query = task_info.get('query')
            plan_item_id = task_info.get('plan_item_id', f'task_{task_index}') # ID для группировки
            query_id = task_info.get('query_id', 'q_0')                   # ID для группировки
            task_key = (plan_item_id, query_id)

            if not base_query:
                self.logger.warning(f"Skipping task with empty query: {task_info}")
                continue

            self.logger.info(f"\n--- Processing Task {task_index+1}/{len(self.search_tasks)} (ID: {task_key}): Base Query = '{base_query}' ---")
            self.urls_found_for_task[task_key] = set() # Инициализируем набор URL для этой задачи

            # Генерируем альтернативные запросы
            alternative_queries = generate_alternative_queries(base_query)
            self.logger.info(f"Generated {len(alternative_queries)} query variations: {alternative_queries}")

            task_urls_found_count = 0
            attempted_queries = 0

            # Пробуем разные формулировки запроса
            for query_index, query in enumerate(alternative_queries):
                if task_urls_found_count >= self.results_per_query:
                    self.logger.info(f"Target of {self.results_per_query} URLs reached for task {task_key}, stopping search variations.")
                    break

                self.logger.info(f"Trying query variation {query_index+1}/{len(alternative_queries)}: '{query}'")
                attempted_queries += 1
                new_urls_from_ddg = []

                # Попытка поиска через DuckDuckGo
                try:
                    new_urls_from_ddg = self._search_with_ddg(query, task_info, task_key)
                    task_urls_found_count += len(new_urls_from_ddg)
                    search_requests_made += 1
                    self.logger.info(f"DDG added {len(new_urls_from_ddg)} new URLs. Total for task {task_key}: {task_urls_found_count}")
                except Exception as e:
                    self.logger.error(f"Unexpected error during DDG search for '{query}': {e}")


                # Задержка между запросами к поисковикам (даже если DDG не сработал)
                if query_index < len(alternative_queries) - 1 or task_urls_found_count < self.results_per_query:
                     # Добавляем задержку перед fallback или следующим DDG запросом
                     delay = SEARCH_DELAY * (0.8 + 0.4 * random.random()) # Случайная задержка ±20%
                     self.logger.info(f"Pausing for {delay:.2f}s before next search action")
                     time.sleep(delay)

                # Если DuckDuckGo не нашел достаточно *для этой конкретной вариации*
                # И если это первая (основная) формулировка запроса, пробуем резервные ПС
                if not new_urls_from_ddg and query_index == 0 and task_urls_found_count < self.results_per_query:
                    self.logger.info(f"DDG found no new URLs for the primary query variation, trying fallback search...")

                    # Пробуем Yandex
                    if task_urls_found_count < self.results_per_query:
                        try:
                             new_urls_from_yandex = self._search_with_fallback(query, task_info, task_key, 'yandex')
                             task_urls_found_count += len(new_urls_from_yandex)
                             self.logger.info(f"Yandex added {len(new_urls_from_yandex)} new URLs. Total for task {task_key}: {task_urls_found_count}")
                             if new_urls_from_yandex: time.sleep(SEARCH_DELAY * 0.5) # Краткая пауза после успешного fallback
                        except Exception as e:
                             self.logger.error(f"Unexpected error during Yandex fallback for '{query}': {e}")


                    # Если все еще недостаточно, пробуем Bing
                    if task_urls_found_count < self.results_per_query:
                        try:
                            new_urls_from_bing = self._search_with_fallback(query, task_info, task_key, 'bing')
                            task_urls_found_count += len(new_urls_from_bing)
                            self.logger.info(f"Bing added {len(new_urls_from_bing)} new URLs. Total for task {task_key}: {task_urls_found_count}")
                            if new_urls_from_bing: time.sleep(SEARCH_DELAY * 0.5) # Краткая пауза
                        except Exception as e:
                            self.logger.error(f"Unexpected error during Bing fallback for '{query}': {e}")

            # Итоги по задаче
            self.logger.info(f"--- Task {task_key} Search Summary ---")
            self.logger.info(f"Attempted {attempted_queries} query variations.")
            self.logger.info(f"Collected {task_urls_found_count} unique valid URLs for this task.")
            total_urls_collected += task_urls_found_count

            # Если после всех попыток не нашли URLs, добавляем в список неудачных *задач*
            if task_urls_found_count == 0:
                self.failed_searches.append(task_info)
                self.logger.warning(f"❌ FAILED TASK: No URLs found for task {task_key} (query: '{base_query}') after all attempts.")

            # Задержка между основными задачами, если есть еще задачи
            if task_index < len(self.search_tasks) - 1:
                delay = SEARCH_DELAY * 1.2 * (0.9 + 0.2 * random.random()) # Немного дольше, ±10%
                self.logger.info(f"--- Pausing for {delay:.2f}s before next task ---")
                time.sleep(delay)

        # Установка allowed_domains на основе найденных URL (не обязательно, но может помочь Scrapy)
        allowed_domains_set = set()
        for url in self.urls_to_scrape.keys():
            try:
                domain = urlparse(url).netloc
                if domain:
                    # Убираем www. для более общего разрешения
                    if domain.startswith('www.'):
                         domain = domain[4:]
                    allowed_domains_set.add(domain)
            except Exception as e:
                self.logger.warning(f"Could not parse domain from URL '{url}': {e}")
        self.allowed_domains = list(allowed_domains_set)
        self.logger.info(f"Configured allowed_domains: {len(self.allowed_domains)} domains")


        self.logger.info(f"\n--- Search Phase Complete ---")
        self.logger.info(f"Total unique URLs collected across all tasks: {len(self.urls_to_scrape)}")
        self.logger.info(f"Total search engine requests made (approx): {search_requests_made} (excluding fallbacks)")
        if self.failed_searches:
             self.logger.warning(f"Found {len(self.failed_searches)} tasks with zero results.")

        # Шаг 2: Создаем запросы Scrapy для найденных URL
        if not self.urls_to_scrape:
            self.logger.warning("No valid URLs found to scrape after all searches. Stopping spider.")
            # Не используем CloseSpider здесь, т.к. процесс должен завершиться штатно
            return # Просто не генерируем запросы

        self.logger.info(f"\n--- Starting Scrapy Download Phase for {len(self.urls_to_scrape)} URLs ---")
        request_count = 0
        for url, task_info in self.urls_to_scrape.items():
            if url not in self.processed_urls:
                request_count += 1
                self.logger.debug(f"Yielding request {request_count}/{len(self.urls_to_scrape)}: {url}")
                self.processed_urls.add(url)
                yield scrapy.Request(
                    url,
                    callback=self.parse_article,
                    errback=self.handle_error,
                    meta={
                        'task_info': task_info,
                        # Разрешаем Scrapy обрабатывать эти коды без ошибок по умолчанию
                        'handle_httpstatus_list': [403, 404, 500, 503, 429, 502, 504],
                        'download_timeout': 30,  # Таймаут для скачивания
                        # Добавим изначальную попытку (Scrapy добавит свои при RETRY_ENABLED)
                        'retry_times': 0
                    },
                    # Добавим случайный User-Agent для каждого запроса
                    headers={'User-Agent': random.choice(USER_AGENTS)}
                )
            else:
                 self.logger.debug(f"Skipping already processed URL: {url}")


    def _add_url_if_valid(self, url: str, task_info: Dict[str, Any], task_key: Tuple[str, str], source: str) -> bool:
        """Проверяет URL, добавляет его в общие и задачные списки, если он валиден и нов."""
        task_urls = self.urls_found_for_task.setdefault(task_key, set())

        # Проверяем, не достигли ли лимита для *этой конкретной задачи*
        if len(task_urls) >= self.results_per_query:
            # self.logger.debug(f"  Limit reached for task {task_key}, skipping URL: {url}")
            return False

        if is_valid_url(url):
            # Проверяем, не был ли этот URL уже добавлен для *любой* задачи
            if url not in self.urls_to_scrape:
                self.urls_to_scrape[url] = task_info # Добавляем в общий список для скрапинга
                task_urls.add(url)              # Добавляем в список URL для *этой* задачи
                self.logger.debug(f"  [+] Added URL from {source}: {url} (Task: {task_key})")
                return True
            else:
                # URL уже есть, но может быть от другой задачи. Проверим, добавлен ли он к *этой* задаче.
                if url not in task_urls:
                     task_urls.add(url)
                     self.logger.debug(f"  [=] Added existing URL to task {task_key}: {url} (From: {source})")
                     # Не считаем это "новым" добавлением, но связали с задачей
                     return False # Не считаем за новое добавление к общему счетчику
                else:
                     # self.logger.debug(f"  [=] URL already collected for task {task_key}: {url} (From: {source})")
                     return False # Уже есть и в общем, и в задачном списке
        else:
            # self.logger.debug(f"  [-] Invalid URL skipped: {url} (From: {source})")
            return False

    def _search_with_ddg(self, query: str, task_info: Dict[str, Any], task_key: Tuple[str, str]) -> List[str]:
        """Выполняет поиск через DuckDuckGo и возвращает список *новых* URL, добавленных для этой задачи."""
        newly_added_urls = []
        retry_count = 0
        task_urls = self.urls_found_for_task.setdefault(task_key, set())
        results_needed_for_task = self.results_per_query - len(task_urls)

        if results_needed_for_task <= 0:
             # self.logger.debug(f"DDG search skipped for '{query}', task {task_key} already has enough URLs.")
             return newly_added_urls # Уже достаточно URL для этой задачи

        while retry_count < MAX_RETRIES:
            try:
                # Запрашиваем немного больше результатов, т.к. будем фильтровать
                # Учитываем, сколько уже найдено для этой задачи
                max_results_to_fetch = results_needed_for_task + 8
                self.logger.info(f"DDG search for '{query}' (Task: {task_key}, Attempt: {retry_count+1}/{MAX_RETRIES}, Need: {results_needed_for_task}, Fetching: {max_results_to_fetch})")

                # Используем контекстный менеджер DDGS
                with DDGS(headers={'User-Agent': random.choice(USER_AGENTS)}, timeout=20) as ddgs:
                    results_iterator = ddgs.text(query, max_results=max_results_to_fetch)

                    results_processed = 0
                    if results_iterator:
                        for r in results_iterator:
                             # Проверяем, не набрали ли уже достаточно для задачи *во время* итерации
                             if len(self.urls_found_for_task[task_key]) >= self.results_per_query:
                                 break

                             if r and isinstance(r, dict) and 'href' in r:
                                 url = r.get('href')
                                 results_processed += 1
                                 if self._add_url_if_valid(url, task_info, task_key, "DDG"):
                                     newly_added_urls.append(url)
                             else:
                                 self.logger.debug(f"  [DDG Invalid Result Format]: {r}")


                        self.logger.debug(f"DDG processed {results_processed} results for '{query}'.")
                        # Если обработали результаты, выходим из цикла попыток
                        break
                    else:
                        self.logger.warning(f"DDG returned no results iterator for query '{query}'")
                        # Не выходим, попробуем еще раз

            except Exception as e:
                self.logger.error(f"Error during DDG search for '{query}' (Attempt {retry_count+1}): {type(e).__name__} - {e}")
                # Можно добавить обработку специфических ошибок, например, таймаутов

            # Увеличиваем счетчик попыток и делаем задержку перед следующей попыткой
            retry_count += 1
            if retry_count < MAX_RETRIES:
                retry_delay = SEARCH_DELAY * (0.5 + retry_count * 0.3) # Увеличиваем задержку
                self.logger.info(f"Retrying DDG search in {retry_delay:.2f}s...")
                time.sleep(retry_delay)
            else:
                 self.logger.warning(f"Max retries reached for DDG search on '{query}'.")

        return newly_added_urls

    def _search_with_fallback(self, query: str, task_info: Dict[str, Any], task_key: Tuple[str, str], search_engine: str) -> List[str]:
        """Выполняет поиск через запасной поисковик и возвращает список *новых* URL, добавленных для этой задачи."""
        newly_added_urls = []
        task_urls = self.urls_found_for_task.setdefault(task_key, set())
        results_needed_for_task = self.results_per_query - len(task_urls)

        if results_needed_for_task <= 0:
            # self.logger.debug(f"{search_engine.capitalize()} fallback skipped for '{query}', task {task_key} already has enough URLs.")
            return newly_added_urls

        self.logger.info(f"Trying fallback search via {search_engine.capitalize()} for '{query}' (Task: {task_key}, Need: {results_needed_for_task})")

        try:
            results = []
            # Выбираем функцию поиска
            if search_engine == 'yandex':
                results = fallback_search_yandex(query, num_results=results_needed_for_task + 5)
            elif search_engine == 'bing':
                results = fallback_search_bing(query, num_results=results_needed_for_task + 5)
            else:
                self.logger.error(f"Unknown fallback search engine: {search_engine}")
                return newly_added_urls

            # Обрабатываем результаты
            if results:
                results_processed = 0
                for r in results:
                    # Проверяем лимит задачи внутри цикла
                    if len(self.urls_found_for_task[task_key]) >= self.results_per_query:
                        break

                    url = r.get('href')
                    results_processed += 1
                    if self._add_url_if_valid(url, task_info, task_key, search_engine.capitalize()):
                        newly_added_urls.append(url)

                self.logger.debug(f"{search_engine.capitalize()} processed {results_processed} results for '{query}'.")
            else:
                self.logger.warning(f"No results from {search_engine.capitalize()} fallback for query '{query}'")

        except Exception as e:
            self.logger.error(f"Error during {search_engine.capitalize()} fallback search for '{query}': {type(e).__name__} - {e}")

        return newly_added_urls

    def parse_article(self, response):
        # Шаг 3: Парсинг контента страницы
        url = response.url
        task_info = response.meta.get('task_info', {})
        status = response.status
        self.visited_urls.add(url) # Отмечаем URL как посещенный

        self.logger.info(f"Processing response from: {url} (Status: {status})")

        # Проверяем статус ответа
        if status >= 400:
             self.logger.warning(f"Received non-2xx status {status} for {url}. Skipping content parsing.")
             # Не возвращаем item
             return

        # Проверяем Content-Type (на всякий случай, если не HTML)
        content_type = response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore').lower()
        if 'html' not in content_type and 'text' not in content_type:
            self.logger.warning(f"Skipping non-HTML content: {url} (Type: {content_type})")
            return

        extracted_text = None
        extraction_method = None
        title = ""

        # 0. Попытка извлечь заголовок (лучше сделать до извлечения текста)
        try:
            title = response.css('title::text').get() or ""
            title = title.strip()
            # Попробуем найти h1, если title пустой или слишком общий
            if not title or title.lower() in ["home", "index", "blog", "article"]:
                 h1_text = response.css('h1::text').get()
                 if h1_text:
                      title = h1_text.strip()
            # self.logger.debug(f"  Title extracted: '{title}'")
        except Exception as e:
            self.logger.debug(f"  Could not extract title for {url}: {e}")
            title = ""


        # 1. Попытка с Trafilatura (обычно лучший)
        try:
            # Настройки Trafilatura для лучшего извлечения основного контента
            extracted_text = trafilatura.extract(
                response.body,
                include_comments=False,    # Не включать комментарии
                include_tables=True,       # Включать таблицы (могут быть полезны)
                include_formatting=True,   # Сохранять базовое форматирование (абзацы)
                include_links=False,       # Не включать сами ссылки
                output_format='text',      # Получить чистый текст
                url=url                    # Передаем URL для контекста
            )
            if extracted_text:
                 extracted_text = extracted_text.strip() # Убираем пробелы по краям
                 if len(extracted_text) >= MIN_CONTENT_LENGTH:
                    extraction_method = "trafilatura"
                    # self.logger.debug(f"  Extracted ~{len(extracted_text)} chars using Trafilatura.")
                 else:
                    # self.logger.debug(f"  Trafilatura extracted short text ({len(extracted_text)} chars). Discarding.")
                    extracted_text = None
            else:
                 # self.logger.debug(f"  Trafilatura extracted no text.")
                 extracted_text = None
        except Exception as e:
            extracted_text = None
            self.logger.warning(f"  Trafilatura failed for {url}: {e}")

        # 2. Попытка с Newspaper3k (если Trafilatura не сработал)
        if not extracted_text:
            # self.logger.debug(f"  Trying Newspaper3k fallback for {url}...")
            try:
                article = Article(url=url, language='ru' if '.ru/' in url or '.рф/' in url else 'en') # Поможем с языком
                # Передаем уже загруженный HTML
                article.download(input_html=response.body.decode(response.encoding, errors='ignore'))
                article.parse()
                if article.text:
                     article_text = article.text.strip()
                     if len(article_text) >= MIN_CONTENT_LENGTH:
                        extracted_text = article_text
                        extraction_method = "newspaper3k"
                        # self.logger.debug(f"  Extracted ~{len(extracted_text)} chars using Newspaper3k (fallback).")
                        # Попробуем использовать заголовок из newspaper, если наш пуст
                        if not title and article.title:
                             title = article.title.strip()
                     else:
                         # self.logger.debug(f"  Newspaper3k extracted short text ({len(article_text)} chars). Discarding.")
                         extracted_text = None
                else:
                     # self.logger.debug(f"  Newspaper3k extracted no text.")
                     extracted_text = None
            except ArticleException as e:
                extracted_text = None
                self.logger.debug(f"  Newspaper3k ArticleException for {url}: {e}")
            except Exception as e:
                extracted_text = None
                self.logger.warning(f"  Newspaper3k failed unexpectedly for {url}: {e}")

        # 3. Последняя попытка: простой парсинг HTML с BeautifulSoup (если все остальное не удалось)
        if not extracted_text:
            # self.logger.debug(f"  Trying simple HTML parsing (BeautifulSoup) for {url}...")
            try:
                soup = BeautifulSoup(response.body, 'lxml') # Используем lxml для скорости

                # Ищем основные теги контента
                main_content = soup.find('main') or soup.find('article') or soup.find('div', role='main') or soup.find('div', class_=re.compile(r'(content|main|body|post|entry)', re.I))

                if not main_content:
                    # Если не нашли основной блок, берем body целиком
                    main_content = soup.body

                if main_content:
                    # Удаляем ненужные элементы внутри основного блока
                    for element in main_content.select('script, style, nav, footer, header, aside, form, iframe, noscript, .sidebar, #sidebar, .comments, #comments, .related-posts, .social-links, .ad, [aria-hidden="true"]'):
                        element.extract()

                    # Получаем текст, сохраняя абзацы
                    paragraphs = main_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'pre', 'code', 'td', 'th'])
                    text_parts = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)] # Берем непустые
                    raw_text = '\n\n'.join(text_parts) # Соединяем через двойной перенос строки

                    # Базовая чистка
                    clean_text = re.sub(r'\s{2,}', ' ', raw_text).strip() # Убираем лишние пробелы внутри
                    clean_text = re.sub(r'\n{3,}', '\n\n', clean_text)    # Убираем лишние переносы строк

                    if len(clean_text) >= MIN_CONTENT_LENGTH:
                        extracted_text = clean_text
                        extraction_method = "simple_html"
                        # self.logger.debug(f"  Extracted ~{len(extracted_text)} chars using simple HTML parsing (last resort).")
                    else:
                        # self.logger.debug(f"  Simple HTML parsing extracted short/no text ({len(clean_text)} chars).")
                        pass # Не сбрасываем extracted_text в None здесь
                else:
                     # self.logger.debug(f"  Could not find <body> or main content block for simple parsing.")
                     pass

            except Exception as e:
                self.logger.warning(f"  Simple HTML parsing failed for {url}: {e}")

        # Генерируем результат, если удалось извлечь текст
        if extracted_text:
            self.logger.info(f"✅ Successfully extracted text from: {url} (Method: {extraction_method}, Length: {len(extracted_text)})")
            # Финальная очистка текста
            cleaned_text = re.sub(r'\s{2,}', ' ', extracted_text.strip())
            cleaned_text = re.sub(r'(\r\n|\r|\n){2,}', '\n\n', cleaned_text) # Нормализуем переносы строк

            yield {
                # Метаданные из исходной задачи
                'query': task_info.get('query'),
                'plan_item': task_info.get('plan_item'),
                'plan_item_id': task_info.get('plan_item_id'),
                'query_id': task_info.get('query_id'),
                # Результаты парсинга
                'url': url,
                'title': title or "No Title Found", # Предоставляем значение по умолчанию
                'text': cleaned_text,
                'extraction_method': extraction_method,
                'content_length': len(cleaned_text)
            }
        else:
            self.logger.warning(f"❌ Failed to extract significant text content from: {url} after all attempts.")
            # Можно вернуть item с пустым текстом или не возвращать ничего
            # yield {
            #     'query': task_info.get('query'),
            #     'plan_item': task_info.get('plan_item'),
            #     'plan_item_id': task_info.get('plan_item_id'),
            #     'query_id': task_info.get('query_id'),
            #     'url': url,
            #     'title': title or "No Title Found",
            #     'text': "",
            #     'extraction_method': "failed",
            #     'content_length': 0
            # }

    def handle_error(self, failure):
        request = failure.request
        url = request.url
        self.visited_urls.add(url) # Отмечаем URL как посещенный, даже если ошибка

        # Получаем информацию об ошибке
        error_type = failure.type.__name__ if failure.type else 'Unknown Error'
        error_message = str(failure.value) # Сообщение об ошибке

        self.logger.error(f"🕷️ Request failed for URL: {url}")
        self.logger.error(f"  Error Type: {error_type}")
        self.logger.error(f"  Error Message: {error_message}")

        # Логируем метаданные запроса, если они есть
        if request.meta:
            task_info = request.meta.get('task_info', {})
            retry_times = request.meta.get('retry_times', 0)
            self.logger.error(f"  Associated Query: '{task_info.get('query', 'N/A')}'")
            self.logger.error(f"  Retry attempt: {retry_times}")
            # Можно добавить логику для отслеживания постоянно падающих URL или доменов


# --- Функция для запуска Scrapy из скрипта ---

def run_enhanced_scrape(search_tasks: List[Dict[str, Any]], results_per_query: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Запускает улучшенный процесс поиска и скрапинга для всех задач.

    Args:
        search_tasks: Список словарей с поисковыми задачами.
                      Каждый словарь должен содержать как минимум 'query'.
                      Желательно также 'plan_item', 'plan_item_id', 'query_id' для лучшей организации.
        results_per_query: Желаемое количество *успешно спарсенных* сайтов на каждый запрос (цель, не гарантия).

    Returns:
        Кортеж из двух списков:
        1. Успешно спарсенные источники (list of dicts, как yield паука).
        2. Задачи, для которых не удалось найти ни одного URL (list of dicts, исходные задачи).
    """
    if not search_tasks:
        logger.error("Нет задач для запуска.")
        return [], []
    if not isinstance(search_tasks, list) or not all(isinstance(task, dict) and 'query' in task for task in search_tasks):
        logger.error("Ошибка: 'search_tasks' должен быть списком словарей, каждый с ключом 'query'.")
        return [], []
    if not isinstance(results_per_query, int) or results_per_query <= 0:
        logger.error("Ошибка: 'results_per_query' должен быть положительным целым числом.")
        return [], []

    logger.info(f"\n=== Запуск улучшенного поиска и скрапинга для {len(search_tasks)} задач ===")
    logger.info(f"Целевое количество URL на задачу: {results_per_query}")
    start_time = time.time()

    # Списки для сбора результатов
    scraped_items = []
    final_failed_searches = [] # Задачи, где не нашли URL

    # Обработчик сигнала item_scraped
    def item_scraped_handler(item, response, spider):
        if item and isinstance(item, dict):
            scraped_items.append(dict(item)) # Копируем item
            spider.logger.info(f"Item collected: {item.get('url')} (Query: '{item.get('query')}')")

    # Обработчик сигнала spider_closed
    def spider_closed_handler(spider, reason):
        nonlocal final_failed_searches
        logger.info(f"Spider closed. Reason: {reason}")
        # Получаем список задач, для которых не нашли URL, из самого паука
        final_failed_searches = getattr(spider, 'failed_searches', [])
        # Логируем статистику по посещенным URL
        visited = getattr(spider, 'visited_urls', set())
        processed = getattr(spider, 'processed_urls', set())
        logger.info(f"Spider stats: Processed {len(processed)} URL requests, Visited {len(visited)} URLs (includes errors/redirects).")


    # --- Настройки Scrapy (ОБНОВЛЕНО) ---
    settings = get_project_settings()
    # Уровень логирования: INFO - показывает основные шаги Scrapy, WARNING - тише
    settings.set('LOG_LEVEL', 'INFO')
    settings.set('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    settings.set('LOG_DATEFORMAT', '%Y-%m-%d %H:%M:%S')

    settings.set('ROBOTSTXT_OBEY', False) # Будьте осторожны и этичны! Соблюдайте задержки.
    # Включаем AutoThrottle - он будет управлять задержками динамически!
    settings.set('AUTOTHROTTLE_ENABLED', True)
    # Начальная задержка для AutoThrottle (можно оставить небольшой)
    settings.set('DOWNLOAD_DELAY', 1.0) # Начальная задержка чуть больше
    # Максимальная задержка, которую может установить AutoThrottle
    settings.set('AUTOTHROTTLE_MAX_DELAY', 15.0) # Не ждать слишком долго
    # Количество одновременных запросов, к которому стремится AutoThrottle (более консервативно)
    settings.set('AUTOTHROTTLE_TARGET_CONCURRENCY', 1.0) # Стараться делать ~1 запрос одновременно
    # Показывать логи AutoThrottle для отладки
    settings.set('AUTOTHROTTLE_DEBUG', False) # Поставьте True для детальной отладки задержек

    # Количество одновременных запросов к одному домену (важно для вежливости)
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 1) # Не более 1 запроса к одному сайту одновременно
    # Общее количество одновременных запросов (AutoThrottle может переопределить)
    settings.set('CONCURRENT_REQUESTS', 8) # Уменьшим общее число

    # Устанавливаем случайный User-Agent по умолчанию (паук может переопределять)
    settings.set('USER_AGENT', random.choice(USER_AGENTS))
    settings.set('DOWNLOAD_TIMEOUT', 35) # Чуть больше времени на загрузку
    settings.set('DNS_TIMEOUT', 25)      # Чуть больше времени на DNS

    # Обработка редиректов и повторных попыток
    settings.set('REDIRECT_ENABLED', True)
    settings.set('RETRY_ENABLED', True) # Включить повторные попытки для временных ошибок (сеть, 5xx)
    settings.set('RETRY_TIMES', 2)      # Попробовать 2 раза повторно (в дополнение к первой попытке)
    settings.set('RETRY_HTTP_CODES', [500, 502, 503, 504, 522, 524, 408, 429]) # Коды для повтора

    # Отключаем куки для уменьшения отслеживания
    settings.set('COOKIES_ENABLED', False)

    # --- Запуск процесса ---
    process = CrawlerProcess(settings)
    crawler = process.create_crawler(EnhancedArticleSpider)

    # Подключаем сигналы
    crawler.signals.connect(item_scraped_handler, signal=signals.item_scraped)
    crawler.signals.connect(spider_closed_handler, signal=signals.spider_closed)


    logger.info("--- Starting CrawlerProcess ---")
    # Передаем аргументы в __init__ паука
    process.crawl(crawler, search_tasks=search_tasks, results_per_query=results_per_query)

    try:
        process.start() # Блокирующий вызов, запускает реактор Twisted
        logger.info("--- CrawlerProcess finished successfully ---")
    except Exception as e:
        logger.error(f"--- CrawlerProcess encountered an error: {e} ---", exc_info=True)
    # Реактор останавливается либо сам по завершении работы, либо по ошибке

    end_time = time.time()
    logger.info(f"\n=== Scrape Run Complete ===")
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds.")
    logger.info(f"Collected {len(scraped_items)} items (successfully parsed sources).")
    if final_failed_searches:
         logger.warning(f"Found {len(final_failed_searches)} tasks where no URLs could be found initially.")
         # logger.debug(f"Failed tasks details: {final_failed_searches}")


    return scraped_items, final_failed_searches


# --- Пример Использования ---
if __name__ == '__main__':
    # Формируем список задач как список словарей
    test_tasks = [
        {
            'query': "применение трансформеров в NLP",
            'plan_item': "Обзор трансформеров",
            'plan_item_id': "plan_0",
            'query_id': "q_0_0"
        },
        {
            'query': "BERT model architecture explained", # Пример английского запроса
            'plan_item': "Архитектура BERT",
            'plan_item_id': "plan_1",
            'query_id': "q_1_0"
        },
        {
            'query': "методы кластеризации данных",
            'plan_item': "Кластеризация",
            'plan_item_id': "plan_2",
            'query_id': "q_2_0"
        },
        {
            'query': "несуществующая чепуха абракадабра", # Запрос без результатов
            'plan_item': "Тест ошибки",
            'plan_item_id': "plan_3",
            'query_id': "q_3_0"
        }
        # Можно добавить больше задач
    ]
    num_sites_to_parse_per_query = 2 # Запрашиваем 2 сайта на каждый запрос

    # # Перед первым запуском newspaper убедитесь, что nltk.download('punkt') выполнен
    # try:
    #     import nltk
    #     try: nltk.data.find('tokenizers/punkt')
    #     except nltk.downloader.DownloadError:
    #         print("NLTK 'punkt' not found. Downloading...")
    #         nltk.download('punkt')
    #         print("'punkt' downloaded.")
    #     except LookupError: # Другой тип ошибки, если ресурс не найден
    #         print("NLTK 'punkt' not found (LookupError). Downloading...")
    #         nltk.download('punkt')
    #         print("'punkt' downloaded.")
    # except ImportError:
    #     print("nltk library not found. Newspaper3k might need it. Please install: pip install nltk")
    # except Exception as e:
    #     print(f"Could not check/download nltk punkt: {e}")

    print("\n--- Starting Test Scrape ---")
    # Запускаем скрапинг и получаем оба результата
    scraped_results, failed_search_tasks = run_enhanced_scrape(
        search_tasks=test_tasks,
        results_per_query=num_sites_to_parse_per_query
    )

    # --- Вывод результатов ---
    print(f"\n--- Scraping Finished ---")

    if scraped_results:
        print(f"\n--- Scraped Content Summary ({len(scraped_results)} items) ---")
        # Группируем результаты по исходной задаче для наглядности
        results_by_task = {}
        for item in scraped_results:
            task_key = (item.get('plan_item_id'), item.get('query_id'))
            if task_key not in results_by_task:
                 results_by_task[task_key] = {'query': item.get('query'), 'plan_item': item.get('plan_item'), 'items': []}
            results_by_task[task_key]['items'].append(item)

        task_counter = 0
        for task_key, task_data in results_by_task.items():
             task_counter += 1
             print(f"\n--- Task {task_counter} (ID: {task_key}) ---")
             print(f"  Query: {task_data['query']}")
             print(f"  Plan Item: {task_data['plan_item']}")
             print(f"  Scraped Items ({len(task_data['items'])}):")
             for i, item in enumerate(task_data['items']):
                  print(f"\n  Item {i+1}:")
                  print(f"    URL: {item.get('url')}")
                  print(f"    Title: {item.get('title', 'N/A')}")
                  print(f"    Method: {item.get('extraction_method')}")
                  text_preview = item.get('text', '')[:250].replace('\n', ' ') # Показываем начало текста
                  print(f"    Text Preview ({len(item.get('text', ''))} chars): {text_preview}...")

        # Сохраняем успешные результаты в JSON файл
        try:
            with open("scraped_content_enhanced.json", "w", encoding="utf-8") as f:
                import json
                json.dump(scraped_results, f, ensure_ascii=False, indent=2)
            print("\nFull successful results saved to scraped_content_enhanced.json")
        except Exception as e:
            print(f"\nFailed to save successful results to JSON: {e}")
    else:
        print("\n--- No content was successfully scraped ---")

    # Выводим информацию о задачах, для которых не нашли URL
    if failed_search_tasks:
        print(f"\n--- Tasks With No URLs Found ({len(failed_search_tasks)}) ---")
        for i, task in enumerate(failed_search_tasks):
             print(f"  Task {i+1}:")
             print(f"    Query: {task.get('query')}")
             print(f"    Plan Item: {task.get('plan_item', 'N/A')}")
             print(f"    Plan Item ID: {task.get('plan_item_id', 'N/A')}")
             print(f"    Query ID: {task.get('query_id', 'N/A')}")

        # Можно сохранить и этот список
        try:
            with open("failed_search_tasks.json", "w", encoding="utf-8") as f:
                import json
                json.dump(failed_search_tasks, f, ensure_ascii=False, indent=2)
            print("\nList of tasks with no URLs found saved to failed_search_tasks.json")
        except Exception as e:
            print(f"\nFailed to save failed tasks list to JSON: {e}")
    else:
        print("\n--- All tasks had at least one URL found during the search phase ---")

    print("\n--- End of Script ---")