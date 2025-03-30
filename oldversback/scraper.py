import time
import os
import logging
from urllib.parse import urlparse

# --- Scrapy Imports ---
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import signals

# --- Other Libraries ---
from dotenv import load_dotenv
from duckduckgo_search import DDGS
import trafilatura # Предпочтительный экстрактор контента
from newspaper import Article, ArticleException # Запасной вариант

# --- Начальная настройка ---
load_dotenv()

# Настройка логирования Scrapy (уменьшаем шум)
logging.getLogger('scrapy').propagate = False
logging.getLogger('scrapy').setLevel(logging.WARNING) # Показываем только предупреждения и ошибки Scrapy
logging.getLogger('duckduckgo_search').setLevel(logging.INFO)
logging.getLogger('urllib3').propagate = False # Убираем лишние логи от requests/urllib3

# --- Конфигурация Скрапинга ---
# Сколько ссылок брать из поиска по каждому запросу
SEARCH_RESULTS_PER_QUERY = 5 # Увеличим немного, т.к. не все спарсятся
# Задержки (важно!)
SEARCH_DELAY = 3.0 # Пауза между поисковыми запросами к DDG (секунды)
DOWNLOAD_DELAY = 2.0 # Пауза между загрузками страниц с сайтов (секунды)

# --- Scrapy Spider ---

class ArticleSpider(scrapy.Spider):
    name = 'article_spider'

    # Теперь принимает список запросов и количество результатов на запрос
    def __init__(self, search_tasks=None, results_per_query=3, *args, **kwargs):
        super(ArticleSpider, self).__init__(*args, **kwargs)
        if search_tasks is None or not isinstance(search_tasks, list):
            raise ValueError("Spider needs 'search_tasks' argument (list of dicts like {'query': q, 'plan_item': p, ...})")

        self.search_tasks = search_tasks # Список словарей с заданиями
        self.results_per_query = results_per_query
        self.allowed_domains = set() # Используем set для уникальности
        self.urls_to_scrape = {} # Словарь {url: original_query_info}
        self.logger.info(f"Spider initialized for {len(search_tasks)} search tasks.")

    def start_requests(self):
        # Шаг 1: Поиск URL для ВСЕХ запросов
        self.logger.info("Starting DDG searches for all tasks...")
        ddg_requests_made = 0
        for task_info in self.search_tasks:
            query = task_info.get('query')
            if not query:
                continue

            self.logger.info(f"  Performing DDG search for: '{query}'")
            try:
                with DDGS() as ddgs:
                    # Безопасность: ограничиваем количество результатов
                    safe_n_results = min(self.results_per_query * 2, 10)
                    results = ddgs.text(query, max_results=safe_n_results)
                    ddg_requests_made += 1
                    count = 0
                    if results:
                        for r in results:
                            if r and 'href' in r and count < self.results_per_query:
                                url = r['href']
                                if url.startswith('http') and not url.lower().endswith(('.pdf', '.docx', '.xlsx', '.pptx', '.zip', '.rar', '.jpg', '.png', '.gif')):
                                    # Сохраняем URL и связанную с ним информацию о задаче
                                    if url not in self.urls_to_scrape:
                                        self.urls_to_scrape[url] = task_info # Сохраняем всю инфу о задаче

                                    try:
                                        domain = urlparse(url).netloc
                                        if domain: self.allowed_domains.add(domain)
                                    except Exception: pass
                                    count += 1
                            if count >= self.results_per_query: break
                    self.logger.info(f"  Found {count} valid URLs for query '{query}'.")

            except Exception as e:
                self.logger.error(f"  Unexpected error during DDG search for '{query}': {e}")

            # Задержка МЕЖДУ поисковыми запросами DDG
            if ddg_requests_made < len(self.search_tasks):
                self.logger.info(f"  Pausing for {SEARCH_DELAY}s before next DDG search...")
                time.sleep(SEARCH_DELAY)

        self.logger.info(f"Total unique URLs to scrape: {len(self.urls_to_scrape)}")
        # Обновляем allowed_domains для паука
        self.allowed_domains = list(self.allowed_domains)

        # Шаг 2: Создаем запросы Scrapy для ВСЕХ найденных URL
        if not self.urls_to_scrape:
            self.logger.warning("No URLs found to scrape after all DDG searches.")
            return # Нечего скрапить

        self.logger.info(f"Starting requests for {len(self.urls_to_scrape)} URLs...")
        for url, task_info in self.urls_to_scrape.items():
            yield scrapy.Request(
                url,
                callback=self.parse_article,
                errback=self.handle_error,
                meta={'task_info': task_info} # Передаем информацию о задаче дальше
            )

    def parse_article(self, response):
        # Шаг 3: Парсинг контента страницы
        url = response.url
        task_info = response.meta.get('task_info', {}) # Получаем инфу о задаче
        self.logger.info(f"Parsing: {url} (Status: {response.status}) for query: '{task_info.get('query')}'")

        content_type = response.headers.get('Content-Type', b'').decode().lower()
        if 'html' not in content_type:
             self.logger.warning(f"Skipping non-HTML content: {url} (Type: {content_type})")
             return

        extracted_text = None
        # 1. Попытка с Trafilatura
        try:
            extracted_text = trafilatura.extract(response.body, include_comments=False, include_tables=False, output_format='text', url=url)
            if not (extracted_text and len(extracted_text) > 150): extracted_text = None
            else: self.logger.info(f"  Extracted ~{len(extracted_text)} chars using Trafilatura.")
        except Exception as e: extracted_text = None; self.logger.warning(f"  Trafilatura failed: {e}")

        # 2. Попытка с Newspaper3k (если Trafilatura не сработал)
        # ... (логика с Newspaper3k как раньше, только без повторной загрузки) ...
        if not extracted_text:
             try:
                 article = Article(url=url); article.download(input_html=response.body); article.parse()
                 if article.text and len(article.text) > 150:
                     extracted_text = article.text; self.logger.info(f"  Extracted ~{len(extracted_text)} chars using Newspaper3k.")
                 else: extracted_text = None
             except Exception as e: extracted_text = None; self.logger.warning(f"  Newspaper3k failed: {e}")


        # Если текст успешно извлечен, ВЫДАЕМ СЛОВАРЬ (Item)
        if extracted_text:
            self.logger.info(f"Successfully processed: {url}")
            yield {
                'query': task_info.get('query'),
                'plan_item': task_info.get('plan_item'),
                'plan_item_id': task_info.get('plan_item_id'),
                'query_id': task_info.get('query_id'),
                'url': url,
                'text': extracted_text
            }
        else:
            self.logger.warning(f"Failed to extract significant text content from: {url}")

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.request.url} - Error: {failure.value}")
# --- Функция для Запуска Scrapy из Скрипта ---

def run_complete_scrape(search_tasks: list, results_per_query: int) -> list[dict]:
    """
    Запускает ОДИН процесс Scrapy для обработки ВСЕХ поисковых задач.

    Args:
        search_tasks: Список словарей, каждый описывает поисковую задачу
                      (например, {'query': '...', 'plan_item': '...', ...}).
        results_per_query: Желаемое количество сайтов для парсинга на каждый запрос.

    Returns:
        Список словарей, где каждый словарь представляет успешно спарсенный
        источник (содержит 'query', 'url', 'text' и др. метаданные).
    """
    if not search_tasks:
        print("[Scrapy Runner] Нет задач для запуска.")
        return []

    print(f"\n[Scrapy Runner] Запуск ЕДИНОГО процесса для {len(search_tasks)} поисковых задач...")
    start_time = time.time()

    # Список для сбора результатов через сигнал
    scraped_items = []

    # Обработчик сигнала item_scraped
    def item_scraped_handler(item, response, spider):
        scraped_items.append(dict(item)) # Добавляем копию словаря

    # Настройки Scrapy (как раньше)
    settings = get_project_settings()
    settings.set('LOG_LEVEL', 'INFO') # Можно поставить INFO для большей детализации
    settings.set('ROBOTSTXT_OBEY', False)
    settings.set('DOWNLOAD_DELAY', DOWNLOAD_DELAY)
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 2)
    settings.set('AUTOTHROTTLE_ENABLED', True)
    settings.set('AUTOTHROTTLE_TARGET_CONCURRENCY', 1.0)
    settings.set('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36')
    # Увеличим таймаут DNS и загрузки, т.к. могут быть медленные сайты
    settings.set('DOWNLOAD_TIMEOUT', 30) # 30 секунд на загрузку
    settings.set('DNS_TIMEOUT', 20)

    # Запускаем процесс Scrapy ОДИН РАЗ
    process = CrawlerProcess(settings)

    # Подключаем обработчик сигнала
    crawler = process.create_crawler(ArticleSpider) # Создаем crawler заранее
    crawler.signals.connect(item_scraped_handler, signal=signals.item_scraped)

    print("[Scrapy Runner] Запуск CrawlerProcess...")
    # Передаем список задач пауку
    process.crawl(crawler, search_tasks=search_tasks, results_per_query=results_per_query)

    try:
        process.start() # Запускаем реактор (блокирующий вызов)
        print("[Scrapy Runner] CrawlerProcess завершен.")
    except Exception as e:
         # ReactorNotRestartable здесь не должно быть, но другие ошибки возможны
        print(f"[Scrapy Runner] Ошибка во время выполнения CrawlerProcess: {e}")

    end_time = time.time()
    print(f"[Scrapy Runner] Весь скрапинг завершен за {end_time - start_time:.2f} сек.")
    print(f"[Scrapy Runner] Собрано {len(scraped_items)} элементов (текстов с источников).")

    return scraped_items



# --- Пример Использования ---
if __name__ == '__main__':
    test_query = "применение трансформеров в NLP"
    num_sites_to_parse = 3 # Запрашиваем 3 сайта

    # Перед первым запуском убедитесь, что nltk.download('punkt') выполнен
    import nltk
    try: nltk.data.find('tokenizers/punkt')
    except: nltk.download('punkt')

    print("\n--- Запуск Тестового Парсинга ---")
    aggregated_content = run_complete_scrape([test_query], num_sites_to_parse)

    if aggregated_content:
        print("\n--- Агрегированный Контент ---")
        # Выводим начало для краткости
        print(aggregated_content[:] + "\n...")
        # Можно сохранить в файл
        # with open("scraped_content.txt", "w", encoding="utf-8") as f:
        #     f.write(aggregated_content)
        # print("Контент сохранен в scraped_content.txt")
    else:
        print("\n--- Контент не был собран ---")