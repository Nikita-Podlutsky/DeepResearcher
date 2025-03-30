import time
import os
import logging
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional # Добавим type hints

# --- Scrapy Imports ---
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.spiders import Spider # Используем базовый Spider, т.к. правила не нужны
from scrapy.linkextractors import LinkExtractor
from scrapy import signals
from scrapy.exceptions import CloseSpider # Для остановки, если нет URL

# --- Other Libraries ---
from dotenv import load_dotenv
from duckduckgo_search import DDGS # Импортируем исключение
import trafilatura # Предпочтительный экстрактор контента
from newspaper import Article, ArticleException # Запасной вариант

# --- Начальная настройка ---
load_dotenv()

# Настройка логирования (оставляем как есть, выглядит хорошо)
logging.getLogger('scrapy').propagate = False
logging.getLogger('scrapy').setLevel(logging.INFO) # Можно INFO для отладки Scrapy
logging.getLogger('duckduckgo_search').setLevel(logging.INFO)
logging.getLogger('urllib3').propagate = False
logging.getLogger('trafilatura').setLevel(logging.WARNING) # Уменьшим шум от trafilatura
logging.getLogger('newspaper').setLevel(logging.WARNING) # Уменьшим шум от newspaper

# --- Конфигурация Скрапинга (Обновлено) ---
SEARCH_RESULTS_PER_QUERY = 5 # Берем топ-5 из поиска
# Задержки (ОБНОВЛЕНО: AUTOTHROTTLE предпочтительнее)
SEARCH_DELAY =  7.0 # Пауза МЕЖДУ поисковыми запросами к DDG (оставляем для безопасности DDG)
# DOWNLOAD_DELAY = 0.5 # Уменьшаем, т.к. будет работать AutoThrottle
MIN_CONTENT_LENGTH = 150 # Минимальная длина извлеченного текста

# --- Scrapy Spider ---

class ArticleSpider(Spider): # Используем базовый Spider
    name = 'article_spider'
    # Не указываем allowed_domains заранее, он будет определен динамически

    def __init__(self, search_tasks: List[Dict[str, Any]] = None, results_per_query: int = 3, *args, **kwargs):
        super(ArticleSpider, self).__init__(*args, **kwargs)
        if search_tasks is None or not isinstance(search_tasks, list):
            raise ValueError("Spider needs 'search_tasks' argument (list of dicts like {'query': q, 'plan_item': p, ...})")
        if not search_tasks:
             raise ValueError("'search_tasks' list cannot be empty.")

        self.search_tasks = search_tasks
        self.results_per_query = results_per_query
        # allowed_domains будет собран в start_requests
        self.urls_to_scrape: Dict[str, Dict[str, Any]] = {} # Словарь {url: original_task_info}
        self.logger.info(f"Spider initialized for {len(search_tasks)} search tasks (max {results_per_query} results per query).")

    def start_requests(self):
        # Шаг 1: ПОСЛЕДОВАТЕЛЬНЫЙ Поиск URL для ВСЕХ запросов
        # Примечание: Делаем последовательно с задержкой, чтобы не перегружать DDG.
        # Для большого количества запросов это может быть медленно.
        self.logger.info("Starting DDG searches sequentially...")
        urls_found_count = 0
        ddg_requests_made = 0

        for task_info in self.search_tasks:
            query = task_info.get('query')
            if not query:
                self.logger.warning(f"Skipping task with empty query: {task_info}")
                continue

            self.logger.info(f"--- Performing DDG search for task query: '{query}' ---")
            current_query_urls = 0
            try:
                # Используем менеджер контекста, он безопаснее
                with DDGS(timeout=10) as ddgs: # Увеличим таймаут для DDG
                    # Запрашиваем чуть больше, т.к. будем фильтровать
                    results_iterator = ddgs.text(query, max_results=self.results_per_query + 5) # Запрашиваем чуть больше
                    ddg_requests_made += 1

                    if results_iterator:
                        for r in results_iterator:
                            if current_query_urls >= self.results_per_query:
                                break # Достаточно URL для этого запроса

                            if r and isinstance(r, dict) and 'href' in r:
                                url = r['href']
                                # Улучшенная проверка URL
                                if self._is_valid_url(url):
                                    if url not in self.urls_to_scrape:
                                        self.urls_to_scrape[url] = task_info # Сохраняем всю инфу о задаче
                                        self.logger.debug(f"  [+] Added URL: {url} (Query: '{query}')")
                                        current_query_urls += 1
                                    else:
                                         self.logger.debug(f"  [=] URL already collected: {url}")
                            # else: self.logger.debug(f"  [-] Invalid/incomplete DDG result skipped: {r}") # Раскомментировать для отладки DDG

                    self.logger.info(f"  Found {current_query_urls} new, valid URLs for query '{query}'.")
                    urls_found_count += current_query_urls

            except Exception as e: # Ловим общее исключение
                # Сообщение лога все еще указывает на проблему с DDG
                self.logger.error(f"  Error during DDG search for query '{query}': {e}", exc_info=True) # exc_info=True покажет traceback

            # Задержка МЕЖДУ поисковыми запросами DDG
            if ddg_requests_made < len(self.search_tasks):
                self.logger.info(f"--- Pausing for {SEARCH_DELAY}s before next DDG search ---")
                time.sleep(SEARCH_DELAY)

        # Динамически устанавливаем allowed_domains
        allowed_domains_set = set()
        for url in self.urls_to_scrape.keys():
            try:
                domain = urlparse(url).netloc
                if domain:
                    allowed_domains_set.add(domain)
            except Exception as e:
                self.logger.warning(f"Could not parse domain from URL '{url}': {e}")
        self.allowed_domains = list(allowed_domains_set) # Устанавливаем атрибут паука
        self.logger.info(f"Collected {len(self.urls_to_scrape)} unique URLs across {len(self.allowed_domains)} domains.")

        # Шаг 2: Создаем запросы Scrapy для ВСЕХ найденных URL
        if not self.urls_to_scrape:
            self.logger.warning("No valid URLs found to scrape after all DDG searches. Stopping spider.")
            # Можно просто вернуть пустой итератор или вызвать исключение CloseSpider
            # return iter([])
            raise CloseSpider("No URLs to scrape.")

        self.logger.info(f"Yielding Scrapy requests for {len(self.urls_to_scrape)} URLs...")
        request_count = 0
        for url, task_info in self.urls_to_scrape.items():
            request_count += 1
            self.logger.debug(f"  Request {request_count}/{len(self.urls_to_scrape)}: {url}")
            yield scrapy.Request(
                url,
                callback=self.parse_article,
                errback=self.handle_error,
                meta={
                    'task_info': task_info, # Передаем информацию о задаче
                    'handle_httpstatus_list': [403, 404, 500, 503], # Обрабатываем ошибки в errback
                     # 'dont_filter': True # Раскомментировать, если есть проблемы с дубликатами запросов (хотя не должно)
                }
            )

    def _is_valid_url(self, url: Optional[str]) -> bool:
        """Проверяет, является ли URL подходящим для парсинга."""
        if not url or not isinstance(url, str):
            return False
        # Базовые проверки
        if not (url.startswith('http://') or url.startswith('https://')):
            return False
        # Проверка на расширения файлов (можно расширить список)
        excluded_extensions = ('.pdf', '.docx', '.xlsx', '.pptx', '.zip', '.rar', '.jpg', '.png', '.gif', '.mp3', '.mp4', '.avi', '.exe', '.dmg', '.iso', '.xml', '.json', '.css', '.js')
        if url.lower().endswith(excluded_extensions):
            return False
        # Можно добавить проверку на известные "плохие" домены (форумы, соцсети), если нужно
        # excluded_domains = ('facebook.com', 'twitter.com', 'reddit.com', ...)
        # try:
        #     domain = urlparse(url).netloc
        #     if domain and any(url.endswith(d) for d in excluded_domains): return False
        # except: pass # Игнорируем ошибки парсинга URL здесь

        return True

    def parse_article(self, response):
        # Шаг 3: Парсинг контента страницы
        url = response.url
        task_info = response.meta.get('task_info', {})
        self.logger.info(f"Parsing response from: {url} (Status: {response.status})")

        # Проверяем Content-Type еще раз на всякий случай
        content_type = response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore').lower()
        if 'html' not in content_type:
             self.logger.warning(f"Skipping non-HTML content: {url} (Type: {content_type})")
             # Можно вернуть пустой словарь или None, чтобы сигнал сработал, но без данных
             # return None
             # Или просто ничего не возвращать
             return

        extracted_text = None
        extraction_method = None

        # 1. Попытка с Trafilatura
        try:
            # include_links=False может немного улучшить качество текста
            extracted_text = trafilatura.extract(response.body, include_comments=False, include_tables=False, include_links=False, output_format='text', url=url)
            if extracted_text and len(extracted_text.strip()) >= MIN_CONTENT_LENGTH:
                extraction_method = "trafilatura"
                self.logger.debug(f"  Extracted ~{len(extracted_text)} chars using Trafilatura.")
            else:
                self.logger.debug(f"  Trafilatura extracted short/no text ({len(extracted_text.strip()) if extracted_text else 0} chars).")
                extracted_text = None # Сбрасываем, если текст короткий
        except Exception as e:
            extracted_text = None
            self.logger.warning(f"  Trafilatura failed for {url}: {e}", exc_info=False) # Не спамим traceback

        # 2. Попытка с Newspaper3k (если Trafilatura не сработал или дал короткий текст)
        if not extracted_text:
             self.logger.debug(f"  Trying Newspaper3k fallback for {url}...")
             try:
                 # НЕ указываем язык, пусть библиотека определит сама!
                 article = Article(url=url)
                 # Передаем уже загруженный HTML
                 article.download(input_html=response.body)
                 article.parse()
                 if article.text and len(article.text.strip()) >= MIN_CONTENT_LENGTH:
                     extracted_text = article.text
                     extraction_method = "newspaper3k"
                     self.logger.info(f"  Extracted ~{len(extracted_text)} chars using Newspaper3k (fallback).")
                 else:
                      self.logger.debug(f"  Newspaper3k extracted short/no text ({len(article.text.strip()) if article.text else 0} chars).")
                      extracted_text = None
             except ArticleException as e:
                  extracted_text = None
                  # Эти ошибки часто некритичны (например, не найдены авторы)
                  self.logger.debug(f"  Newspaper3k ArticleException for {url}: {e}")
             except Exception as e:
                  extracted_text = None
                  self.logger.warning(f"  Newspaper3k failed unexpectedly for {url}: {e}", exc_info=False)

        # Если текст успешно извлечен, ВЫДАЕМ СЛОВАРЬ (Item)
        if extracted_text:
            self.logger.info(f"Successfully extracted text from: {url} (Method: {extraction_method})")
            # Очистка текста (базовая) - убираем лишние пробелы/переносы строк
            cleaned_text = re.sub(r'\s{2,}', ' ', extracted_text.strip()).replace('\n', ' ')

            yield {
                # Метаданные из исходной задачи
                'query': task_info.get('query'),
                'plan_item': task_info.get('plan_item'),
                'plan_item_id': task_info.get('plan_item_id'),
                'query_id': task_info.get('query_id'),
                # Результаты парсинга
                'url': url,
                'text': cleaned_text,
                'extraction_method': extraction_method
            }
        else:
            self.logger.warning(f"Failed to extract significant text content (>{MIN_CONTENT_LENGTH} chars) from: {url}")
            # Можно yield пустой элемент или None, чтобы его поймал обработчик сигнала, если нужно отслеживать неудачные попытки
            # yield {'url': url, 'text': None, ...}


    def handle_error(self, failure):
        # failure — это объект twisted.python.failure.Failure
        request = failure.request
        url = request.url
        # Получаем тип ошибки (если доступно)
        error_type = failure.type.__name__ if failure.type else 'Unknown Error'
        error_message = str(failure.value)

        self.logger.error(f"Request failed for URL: {url}")
        self.logger.error(f"  Error Type: {error_type}")
        self.logger.error(f"  Error Message: {error_message}")
        # Логируем метаданные запроса для отладки
        if request.meta:
            task_info = request.meta.get('task_info', {})
            self.logger.error(f"  Associated Query: '{task_info.get('query', 'N/A')}'")
            # self.logger.error(f"  Request Meta: {request.meta}") # Раскомментировать для полной меты

# --- Функция для Запуска Scrapy из Скрипта ---
import re # Добавим импорт re для очистки текста

def run_complete_scrape(search_tasks: List[Dict[str, Any]], results_per_query: int) -> List[Dict[str, Any]]:
    """
    Запускает ОДИН процесс Scrapy для обработки ВСЕХ поисковых задач.

    Args:
        search_tasks: Список словарей, каждый описывает поисковую задачу.
                      Пример: [{'query': '...', 'plan_item': '...', ...}]
        results_per_query: Желаемое количество сайтов для парсинга на каждый запрос.

    Returns:
        Список словарей, где каждый словарь представляет успешно спарсенный
        источник (содержит 'query', 'url', 'text' и др. метаданные).
    """
    if not search_tasks:
        print("[Scrapy Runner] Нет задач для запуска.")
        return []
    if not isinstance(search_tasks, list) or not all(isinstance(task, dict) for task in search_tasks):
        print("[Scrapy Runner] Ошибка: 'search_tasks' должен быть списком словарей.")
        return []
    if results_per_query <= 0:
        print("[Scrapy Runner] Ошибка: 'results_per_query' должен быть положительным числом.")
        return []


    print(f"\n[Scrapy Runner] Запуск ЕДИНОГО процесса для {len(search_tasks)} поисковых задач...")
    print(f"[Scrapy Runner] Максимум URL на запрос: {results_per_query}")
    start_time = time.time()

    scraped_items: List[Dict[str, Any]] = [] # Используем type hint

    def item_scraped_handler(item, response, spider):
        # Проверяем, что item не None и содержит текст (если нужно)
        if item and isinstance(item, dict): # and item.get('text'):
            scraped_items.append(dict(item))
            spider.logger.info(f"Item collected: {item.get('url')}")
        # else: spider.logger.debug(f"Empty/Invalid item received for {response.url}, not adding to results.")

    # --- Настройки Scrapy (ОБНОВЛЕНО) ---
    settings = get_project_settings()
    # Уровень логирования: INFO - показывает основные шаги Scrapy, WARNING - тише
    settings.set('LOG_LEVEL', 'INFO')
    settings.set('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    settings.set('LOG_DATEFORMAT', '%Y-%m-%d %H:%M:%S')

    settings.set('ROBOTSTXT_OBEY', False) # Будьте осторожны и этичны!
    # Включаем AutoThrottle - он будет управлять задержками динамически!
    settings.set('AUTOTHROTTLE_ENABLED', True)
    # Начальная задержка для AutoThrottle (можно оставить небольшой)
    settings.set('DOWNLOAD_DELAY', 0.5) # УМЕНЬШИЛИ, т.к. AutoThrottle рулит
    # Максимальная задержка, которую может установить AutoThrottle
    settings.set('AUTOTHROTTLE_MAX_DELAY', 15.0) # Не ждать слишком долго
    # Количество одновременных запросов, к которому стремится AutoThrottle
    settings.set('AUTOTHROTTLE_TARGET_CONCURRENCY', 1.5) # Немного больше 1 запроса в среднем
    # Показывать логи AutoThrottle для отладки
    settings.set('AUTOTHROTTLE_DEBUG', False) # Поставьте True для детальной отладки задержек

    # Количество одновременных запросов к одному домену
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 2) # Можно 1 для большей осторожности
    # Общее количество одновременных запросов
    settings.set('CONCURRENT_REQUESTS', 16) # Стандартное значение Scrapy

    settings.set('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36') # Обновим User-Agent
    settings.set('DOWNLOAD_TIMEOUT', 30) # 30 секунд на загрузку
    settings.set('DNS_TIMEOUT', 20)
    # Добавим обработку редиректов
    settings.set('REDIRECT_ENABLED', True)
    settings.set('RETRY_ENABLED', True) # Включить повторные попытки для временных ошибок
    settings.set('RETRY_TIMES', 2) # Попробовать 2 раза повторно (в дополнение к первой попытке)

    # --- Запуск процесса ---
    process = CrawlerProcess(settings)
    crawler = process.create_crawler(ArticleSpider)
    crawler.signals.connect(item_scraped_handler, signal=signals.item_scraped)

    print("[Scrapy Runner] Starting CrawlerProcess...")
    process.crawl(crawler, search_tasks=search_tasks, results_per_query=results_per_query)

    try:
        process.start() # Блокирующий вызов
        print("[Scrapy Runner] CrawlerProcess finished.")
    except Exception as e:
        print(f"[Scrapy Runner] Error during CrawlerProcess execution: {e}")

    end_time = time.time()
    print(f"[Scrapy Runner] Total scraping time: {end_time - start_time:.2f} sec.")
    print(f"[Scrapy Runner] Collected {len(scraped_items)} items (successfully parsed sources).")

    return scraped_items


# --- Пример Использования (ИСПРАВЛЕНО) ---
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
        }
        # Можно добавить больше задач
    ]
    num_sites_to_parse_per_query = 2 # Запрашиваем 2 сайта на каждый запрос

    # Перед первым запуском newspaper убедитесь, что nltk.download('punkt') выполнен
    # import nltk
    # try: nltk.data.find('tokenizers/punkt')
    # except nltk.downloader.DownloadError: nltk.download('punkt')
    # except Exception as e: print(f"Could not check/download nltk punkt: {e}")

    print("\n--- Starting Test Scrape ---")
    scraped_results = run_complete_scrape(search_tasks=test_tasks, results_per_query=num_sites_to_parse_per_query)

    if scraped_results:
        print(f"\n--- Scraped Content Summary ({len(scraped_results)} items) ---")
        for i, item in enumerate(scraped_results):
            print(f"\nItem {i+1}:")
            print(f"  Query: {item.get('query')}")
            print(f"  Plan Item: {item.get('plan_item')}")
            print(f"  URL: {item.get('url')}")
            print(f"  Method: {item.get('extraction_method')}")
            text_preview = item.get('text', '')[:200].replace('\n', ' ') # Показываем начало текста
            print(f"  Text Preview ({len(item.get('text', ''))} chars): {text_preview}...")
        # Можно сохранить в JSON файл для удобства
        try:
            with open("scraped_content.json", "w", encoding="utf-8") as f:
                import json
                json.dump(scraped_results, f, ensure_ascii=False, indent=2)
            print("\nFull results saved to scraped_content.json")
        except Exception as e:
            print(f"\nFailed to save results to JSON: {e}")
    else:
        print("\n--- No content was scraped ---")