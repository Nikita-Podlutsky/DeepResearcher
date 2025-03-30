import concurrent.futures
import logging
import time
import random
from typing import List, Dict, Tuple, Optional

# --- Newspaper & Requests ---
from newspaper import Article, ArticleException
import requests

# --- Selenium ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import (
    WebDriverException, TimeoutException, NoSuchElementException,
    ElementNotInteractableException, StaleElementReferenceException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

# --- Конфигурация ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)

# --- Selenium Настройки ---
SELENIUM_PAGE_LOAD_TIMEOUT = 45  # Увеличим таймаут загрузки страницы
SELENIUM_IMPLICIT_WAIT = 5       # Неявное ожидание Selenium
SELENIUM_EXPLICIT_WAIT = 20      # Явное ожидание элементов (WebDriverWait)
SEARCH_ENGINE_URL = "https://duckduckgo.com/"
# Важно: Селекторы могут измениться! Проверяйте их в браузере.
# Селекторы для DuckDuckGo (могут потребовать обновления)
SEARCH_INPUT_SELECTOR = "#search_form_input_homepage" # На главной
SEARCH_BUTTON_SELECTOR = "#search_button_homepage" # На главной
SEARCH_INPUT_SERP_SELECTOR = "#search_form_input"  # На странице результатов (SERP)
# Селектор для контейнеров результатов (более надежный способ найти ссылки)
RESULTS_CONTAINER_SELECTOR = "#links" # Основной контейнер с результатами
RESULT_LINK_SELECTOR = "a.result__a" # Ссылка внутри каждого результата
# Альтернативный/запасной селектор ссылки, если основной не сработает
RESULT_LINK_SELECTOR_FALLBACK = "a[data-testid='result-title-a']"


# Паузы между запросами к поисковику (ВАЖНО для снижения риска бана)
MIN_DELAY_BETWEEN_SEARCHES = 5  # Секунд
MAX_DELAY_BETWEEN_SEARCHES = 15 # Секунд

# --- Requests Настройки ---
REQUESTS_TIMEOUT = 15  # Таймаут для скрапинга URL
MAX_SCRAPING_WORKERS = 10 # Параллельные потоки для Фазы 2

# --- Вспомогательные функции Selenium ---

def setup_selenium_driver() -> Optional[webdriver.Chrome]:
    """Настраивает и возвращает ОДИН экземпляр Selenium WebDriver."""
    logging.info("Инициализация Selenium WebDriver...")
    try:
        chrome_options = ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        # Добавляем больше опций для маскировки под обычного пользователя
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument(f'user-agent={random.choice(USER_AGENTS)}') # USER_AGENTS определены в предыдущем примере

        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # Устанавливаем таймауты
        driver.set_page_load_timeout(SELENIUM_PAGE_LOAD_TIMEOUT)
        # Неявное ожидание - Selenium будет ждать до N секунд при поиске элемента
        driver.implicitly_wait(SELENIUM_IMPLICIT_WAIT)

        logging.info("Selenium WebDriver успешно инициализирован.")
        return driver
    except Exception as e:
        logging.error(f"Критическая ошибка инициализации Selenium WebDriver: {e}", exc_info=True)
        return None

def safe_find_element(driver, by, value, timeout=SELENIUM_EXPLICIT_WAIT):
    """Безопасный поиск элемента с явным ожиданием."""
    try:
        wait = WebDriverWait(driver, timeout)
        return wait.until(EC.presence_of_element_located((by, value)))
    except TimeoutException:
        logging.warning(f"Элемент не найден ({by}={value}) за {timeout} сек.")
        return None
    except Exception as e:
        logging.error(f"Ошибка при поиске элемента ({by}={value}): {e}")
        return None

def safe_find_elements(driver, by, value, timeout=SELENIUM_EXPLICIT_WAIT):
    """Безопасный поиск списка элементов с явным ожиданием."""
    try:
        # Ждем, пока появится хотя бы один элемент, соответствующий селектору
        wait = WebDriverWait(driver, timeout)
        wait.until(EC.presence_of_element_located((by, value)))
        # Затем возвращаем все найденные элементы
        return driver.find_elements(by, value)
    except TimeoutException:
        logging.warning(f"Элементы не найдены ({by}={value}) за {timeout} сек.")
        return []
    except Exception as e:
        logging.error(f"Ошибка при поиске элементов ({by}={value}): {e}")
        return []

# --- Фаза 1: Сбор URL с помощью Selenium ---

def collect_search_urls_selenium(tasks: List[Dict], num_results_per_query: int) -> List[Tuple[str, Dict]]:
    """
    Использует ОДИН экземпляр Selenium для выполнения поиска по всем задачам
    и сбора URL из результатов. Включает задержки.

    Args:
        tasks: Список словарей задач.
        num_results_per_query: Сколько URL собирать для каждого запроса.

    Returns:
        Список кортежей (url, task_info).
    """
    urls_to_scrape: List[Tuple[str, Dict]] = []
    driver = None
    logging.info(f"--- Фаза 1: Сбор URL с помощью Selenium для {len(tasks)} задач ---")

    try:
        driver = setup_selenium_driver()
        if not driver:
            logging.error("Не удалось запустить Selenium. Фаза 1 прервана.")
            return []

        is_first_search = True
        for i, task in enumerate(tasks):
            query = task.get('query')
            query_id = task.get('query_id', 'N/A')
            if not query:
                logging.warning(f"[Фаза 1 / Selenium] Пропуск задачи {query_id}: отсутствует 'query'.")
                continue

            logging.info(f"[Фаза 1 / Selenium] Обработка задачи {i+1}/{len(tasks)} (query_id={query_id}): '{query}'")

            try:
                # --- Навигация и Поиск ---
                if is_first_search:
                    logging.debug(f"Переход на {SEARCH_ENGINE_URL}")
                    driver.get(SEARCH_ENGINE_URL)
                    time.sleep(random.uniform(1, 3)) # Пауза после загрузки
                    search_box = safe_find_element(driver, By.CSS_SELECTOR, SEARCH_INPUT_SELECTOR)
                    search_button = safe_find_element(driver, By.CSS_SELECTOR, SEARCH_BUTTON_SELECTOR) # Кнопка может быть не нужна, Enter сработает
                else:
                    # Для последующих поисков используем поле на странице результатов
                    search_box = safe_find_element(driver, By.CSS_SELECTOR, SEARCH_INPUT_SERP_SELECTOR)

                if not search_box:
                    logging.error(f"Не найдено поле поиска для query_id={query_id}. Пропуск задачи.")
                    # Попытка перезагрузить страницу или перейти на главную?
                    driver.get(SEARCH_ENGINE_URL) # Попробуем вернуться на главную
                    is_first_search = True # Следующий поиск будет как первый
                    continue

                logging.debug("Очистка поля ввода и ввод запроса...")
                search_box.clear()
                search_box.send_keys(query)
                time.sleep(random.uniform(0.5, 1.5)) # Имитация набора текста
                search_box.send_keys(Keys.RETURN)
                logging.debug("Запрос отправлен.")
                is_first_search = False # Следующий поиск будет на странице результатов

                # --- Ожидание и сбор результатов ---
                logging.debug("Ожидание загрузки результатов...")
                # Ждем появления контейнера с результатами
                wait = WebDriverWait(driver, SELENIUM_EXPLICIT_WAIT)
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, RESULTS_CONTAINER_SELECTOR)))
                    logging.debug("Контейнер результатов найден.")
                except TimeoutException:
                    logging.warning(f"Контейнер результатов ({RESULTS_CONTAINER_SELECTOR}) не найден для query_id={query_id} за {SELENIUM_EXPLICIT_WAIT} сек.")
                    # Проверить, не показана ли CAPTCHA или нет результатов
                    if "captcha" in driver.page_source.lower():
                         logging.error(f"Обнаружена CAPTCHA для query_id={query_id}! Фаза 1 для этой задачи не может быть продолжена.")
                    elif "no results found" in driver.page_source.lower():
                         logging.warning(f"Нет результатов поиска для query_id={query_id}.")
                    else:
                         logging.warning(f"Не удалось загрузить результаты поиска для query_id={query_id}.")
                    continue # Переходим к следующей задаче

                # Даем немного времени на прогрузку всех элементов (опционально)
                time.sleep(random.uniform(1, 3))

                # Ищем ссылки внутри контейнера
                logging.debug(f"Поиск ссылок по селектору: {RESULT_LINK_SELECTOR}")
                result_links_elements = driver.find_elements(By.CSS_SELECTOR, RESULT_LINK_SELECTOR)

                # Запасной вариант, если основной селектор не сработал
                if not result_links_elements:
                    logging.warning(f"Основной селектор ({RESULT_LINK_SELECTOR}) не нашел ссылок. Пробую запасной: {RESULT_LINK_SELECTOR_FALLBACK}")
                    result_links_elements = driver.find_elements(By.CSS_SELECTOR, RESULT_LINK_SELECTOR_FALLBACK)

                if not result_links_elements:
                    logging.warning(f"Не найдено ссылок результатов для query_id={query_id} на странице.")
                    continue

                logging.info(f"Найдено {len(result_links_elements)} потенциальных ссылок для query_id={query_id}.")

                count = 0
                for link_element in result_links_elements:
                    if count >= num_results_per_query:
                        break
                    try:
                        # Используем get_attribute('href') - он надежнее .text
                        url = link_element.get_attribute('href')
                        if url and url.startswith('http'): # Проверяем, что это валидный URL
                            urls_to_scrape.append((url, task))
                            count += 1
                            logging.debug(f"  -> Добавлен URL: {url}")
                        # Иногда DDG подсовывает свои внутренние ссылки или рекламу
                        elif url:
                            logging.debug(f"  -> Пропущен не-http URL: {url}")

                    except StaleElementReferenceException:
                        logging.warning("StaleElementReferenceException при получении href, элемент устарел. Пропуск.")
                        continue # Элемент исчез со страницы, пропускаем
                    except Exception as link_e:
                         logging.error(f"Ошибка при извлечении URL из элемента: {link_e}", exc_info=False)

                logging.info(f"Собрано {count} URL для query_id={query_id}.")

            except (WebDriverException, TimeoutException, NoSuchElementException, ElementNotInteractableException) as selenium_error:
                 logging.error(f"Ошибка Selenium при обработке query_id={query_id}: {selenium_error.__class__.__name__} - {str(selenium_error)[:200]}", exc_info=False)
                 # Можно добавить попытку перезагрузить страницу или перейти к следующему
                 try:
                     driver.get(SEARCH_ENGINE_URL) # Попробовать восстановиться
                     is_first_search = True
                 except Exception as recovery_e:
                      logging.error(f"Не удалось восстановиться после ошибки Selenium: {recovery_e}")
                      # Если восстановление не удалось, возможно, стоит прервать Фазу 1
                      # raise selenium_error # Передать ошибку выше, если нужно остановить всё
            except Exception as general_error:
                 logging.error(f"Неожиданная ошибка при обработке query_id={query_id}: {general_error}", exc_info=True)
                 # Попытка восстановления
                 try:
                     driver.get(SEARCH_ENGINE_URL)
                     is_first_search = True
                 except: pass # Игнорируем ошибку восстановления здесь

            finally:
                # --- Задержка перед следующим запросом ---
                # ВАЖНО: Делаем паузу, чтобы не забанили
                delay = random.uniform(MIN_DELAY_BETWEEN_SEARCHES, MAX_DELAY_BETWEEN_SEARCHES)
                logging.info(f"Пауза {delay:.2f} сек перед следующим запросом...")
                time.sleep(delay)

    except Exception as outer_e:
        logging.error(f"Критическая ошибка в цикле Фазы 1: {outer_e}", exc_info=True)
    finally:
        # --- Обязательно закрываем браузер ---
        if driver:
            logging.info("Закрытие Selenium WebDriver...")
            try:
                driver.quit()
                logging.info("WebDriver закрыт.")
            except Exception as e:
                logging.error(f"Ошибка при закрытии WebDriver: {e}")

    logging.info(f"--- Фаза 1 / Selenium Завершена: Собрано {len(urls_to_scrape)} URL для дальнейшего скрапинга ---")
    # Добавляем предупреждение о возможных проблемах
    if len(urls_to_scrape) < len(tasks) * num_results_per_query * 0.5: # Если собрали меньше половины ожидаемого
         logging.warning("Собрано значительно меньше URL, чем ожидалось. Возможны проблемы с поиском (CAPTCHA, бан, смена селекторов).")

    return urls_to_scrape

# --- Фаза 2: Скрапинг собранных URL (без изменений) ---
# Используем функции scrape_single_url_requests и scrape_collected_urls_requests
# из предыдущего примера (где Фаза 2 использует newspaper3k/requests)

# Функция scrape_single_url_requests из ПРЕДЫДУЩЕГО ответа:
def scrape_single_url_requests(url: str, task_info: dict) -> dict:
    """
    Загружает и парсит ОДИН URL с использованием newspaper3k (на базе requests).
    Возвращает словарь с результатом или информацией об ошибке.
    """
    logging.info(f"[Фаза 2] Попытка скрапинга: {url} (для query_id: {task_info.get('query_id', 'N/A')})")
    result = {
        **task_info,
        "url": url,
        "title": "N/A",
        "text": "",
        "extraction_method": "newspaper3k", # Указываем метод
        "content_length": 0,
        "status": "error",
        "error_message": None
    }
    # --- Headers для имитации браузера ---
    headers = {
        'User-Agent': random.choice(USER_AGENTS), # USER_AGENTS определены ранее
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8', # Можно добавить языки
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'DNT': '1', # Do Not Track
    }
    try:
        # Используем Article с конфигурацией и передаем headers
        config = requests.Session()
        config.headers.update(headers)
        article = Article(url, config=config) # Передаем сессию requests в newspaper
        article.config.REQUEST_TIMEOUT = REQUESTS_TIMEOUT
        article.config.FETCH_IMAGE = False # Не скачивать изображения
        article.config.KEEP_ARTICLE_HTML = False # Не хранить HTML в памяти

        article.download() # newspaper3k использует настроенную сессию requests
        article.parse()

        if not article.text or not article.title:
            # Попытка просто получить title из HTML, если newspaper не справился
            try:
                # Используем requests напрямую для получения <title>
                response = requests.get(url, timeout=REQUESTS_TIMEOUT, headers=headers)
                response.raise_for_status() # Проверка на HTTP ошибки
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'lxml')
                html_title = soup.find('title')
                if html_title and html_title.string:
                     result["title"] = html_title.string.strip()
                     result["error_message"] = "Newspaper3k не извлек текст, но заголовок получен из HTML."
                     logging.warning(f"[Фаза 2] Ошибка извлечения текста для {url}, но заголовок '{result['title'][:60]}...' найден.")
                else:
                     result["error_message"] = "Newspaper3k не смог извлечь контент, <title> не найден."
                     logging.warning(f"[Фаза 2] Ошибка извлечения для {url}: {result['error_message']}")

            except Exception as title_exc:
                 result["error_message"] = f"Newspaper3k не извлек контент. Ошибка при попытке получить title: {title_exc}"
                 logging.warning(f"[Фаза 2] Ошибка извлечения для {url}: {result['error_message']}")
            return result # Возвращаем с ошибкой, но возможно с title

        # Успех
        result["title"] = article.title
        result["text"] = article.text
        result["content_length"] = len(article.text)
        result["status"] = "success"
        result.pop("error_message", None)
        logging.info(f"[Фаза 2] Успешно: {url} (Title: {result['title'][:60]}...)")

    except ArticleException as e:
        result["error_message"] = f"Newspaper3k ArticleException: {e}"
        logging.error(f"[Фаза 2] Newspaper3k ошибка для {url}: {e}")
    except requests.exceptions.RequestException as e:
         result["error_message"] = f"Requests Exception: {e.__class__.__name__}"
         logging.error(f"[Фаза 2] Ошибка Requests для {url}: {e}")
         # Можно добавить код состояния HTTP, если доступен
         if hasattr(e, 'response') and e.response is not None:
              result["error_message"] += f" (Status Code: {e.response.status_code})"
    except Exception as e:
        result["error_message"] = f"Общая ошибка скрапинга: {e.__class__.__name__} - {e}"
        logging.error(f"[Фаза 2] Общая ошибка для {url}: {e}", exc_info=False)

    return result


# Функция scrape_collected_urls_requests из ПРЕДЫДУЩЕГО ответа:
def scrape_collected_urls_requests(urls_with_tasks: list[tuple[str, dict]], max_workers: int) -> list[dict]:
    """
    Параллельно скрапит список URL с использованием newspaper3k/requests.
    """
    if not urls_with_tasks:
        logging.warning("[Фаза 2] Нет URL для скрапинга.")
        return []

    all_results = []
    total_urls = len(urls_with_tasks)
    logging.info(f"--- Фаза 2: Скрапинг {total_urls} URL с использованием {max_workers} воркеров (Requests/Newspaper3k) ---")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='ScraperWorker') as executor:
        future_to_url_task = {
            executor.submit(scrape_single_url_requests, url, task_info): (url, task_info)
            for url, task_info in urls_with_tasks
        }

        processed_count = 0
        for future in concurrent.futures.as_completed(future_to_url_task):
            url, task_info = future_to_url_task[future]
            processed_count += 1
            try:
                result_dict = future.result()
                if result_dict:
                    all_results.append(result_dict)
                if processed_count % 20 == 0 or processed_count == total_urls:
                     logging.info(f"[Фаза 2] Обработано {processed_count}/{total_urls} URL.")

            except Exception as exc:
                logging.error(f"[Фаза 2] Критическая ошибка при получении результата для URL {url}: {exc}", exc_info=True)
                error_result = {
                    **task_info, "url": url, "title": "N/A", "text": "",
                    "extraction_method": "newspaper3k", "content_length": 0,
                    "status": "error", "error_message": f"Concurrency execution error: {exc}"
                }
                all_results.append(error_result)

    logging.info(f"--- Фаза 2 Завершена: Собрано {len(all_results)} результатов скрапинга ---")
    return all_results


# --- Основная функция оркестрации ---

def process_tasks_selenium_search_then_requests(
    tasks: List[Dict],
    num_results_per_query: int = 3,
    max_scraping_workers: int = 10
) -> List[Dict]:
    """
    Оркестрирует двухфазный процесс:
    1. Сбор URL с помощью Selenium (с задержками).
    2. Параллельный скрапинг URL с помощью Requests/Newspaper3k.

    Args:
        tasks: Исходный список задач.
        num_results_per_query: Сколько URL собирать для каждого запроса.
        max_scraping_workers: Сколько потоков использовать для скрапинга во второй фазе.

    Returns:
        Список словарей с результатами скрапинга.
    """
    # --- Фаза 1: Сбор URL (Selenium) ---
    # ВАЖНО: Эта фаза будет медленной из-за задержек!
    urls_to_process = collect_search_urls_selenium(tasks, num_results_per_query)

    if not urls_to_process:
        logging.warning("Не было собрано ни одного URL на Фазе 1 (Selenium). Завершение.")
        # Можно вернуть пустой список или список задач с ошибками поиска
        results_with_search_errors = []
        for task in tasks:
             results_with_search_errors.append({
                **task, "url": None, "title": "N/A", "text": "",
                "extraction_method": "N/A", "content_length": 0,
                "status": "search_failed_selenium",
                "error_message": "No URLs collected during Selenium search phase."
             })
        return results_with_search_errors # Возвращаем информацию об ошибках поиска

    # --- Фаза 2: Скрапинг URL (Requests/Newspaper3k) ---
    scraped_data = scrape_collected_urls_requests(urls_to_process, max_scraping_workers)

    # Дополнить результат информацией о тех задачах, для которых URL не были найдены в фазе 1
    processed_query_ids = {r['query_id'] for r in scraped_data if 'query_id' in r}
    all_task_query_ids = {t['query_id'] for t in tasks if 'query_id' in t}
    missing_query_ids = all_task_query_ids - processed_query_ids

    if missing_query_ids:
        logging.warning(f"Для следующих query_id не было собрано URL на Фазе 1: {missing_query_ids}")
        for qid in missing_query_ids:
             # Найдем исходную задачу для этого qid
             original_task = next((t for t in tasks if t.get('query_id') == qid), None)
             if original_task:
                 scraped_data.append({
                     **original_task, "url": None, "title": "N/A", "text": "",
                     "extraction_method": "N/A", "content_length": 0,
                     "status": "search_failed_selenium",
                     "error_message": "No URLs collected during Selenium search phase for this query."
                 })

    return scraped_data

# --- Глобальный список User-Agents ---
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/115.0'
]


# --- Пример использования ---
if __name__ == "__main__":
    test_tasks = [
        {'query': "применение трансформеров в NLP", 'plan_item': "Обзор трансформеров", 'plan_item_id': "plan_0", 'query_id': "q_0_0"},
        {'query': "React component lifecycle hooks", 'plan_item': "React Lifecycle", 'plan_item_id': "plan_1", 'query_id': "q_1_0"},
        {'query': "python dynamic content loading example", 'plan_item': "Dynamic Content", 'plan_item_id': "plan_4", 'query_id': "q_4_0"},
        {'query': "методы кластеризации данных", 'plan_item': "Кластеризация", 'plan_item_id': "plan_2", 'query_id': "q_2_0"},
        {'query': "fastapi background tasks tutorial", 'plan_item': "FastAPI Tasks", 'plan_item_id': "plan_5", 'query_id': "q_5_0"},
        # Добавим еще несколько для объема
        {'query': "benefits of using rust programming language", 'plan_item': "Rust Intro", 'plan_item_id': "plan_rust", 'query_id': "q_rust_0"},
        {'query': "how does quantum computing work simple explanation", 'plan_item': "Quantum Simple", 'plan_item_id': "plan_quantum", 'query_id': "q_quantum_0"},
        {'query': "лучшие практики ansible", 'plan_item': "Ansible Best Practices", 'plan_item_id': "plan_ansible", 'query_id': "q_ansible_0"},
        {'query': "несуществующая чепуха абракадабра xyzzy фываолдж", 'plan_item': "Тест ошибки поиска", 'plan_item_id': "plan_3", 'query_id': "q_3_0"}
    ]

    NUM_RESULTS = 2 # Сколько ссылок брать из поиска
    MAX_WORKERS_SCRAPE_PHASE2 = 8 # Потоков для скрапинга ссылок (Фаза 2)

    logging.info("--- Запуск двухфазной обработки (Фаза 1: Selenium, Фаза 2: Requests) ---")
    start_run_time = time.time()

    # ВАЖНО: Фаза 1 (Selenium) будет выполняться ПОСЛЕДОВАТЕЛЬНО с задержками!
    final_results = process_tasks_selenium_search_then_requests(
        test_tasks,
        num_results_per_query=NUM_RESULTS,
        max_scraping_workers=MAX_WORKERS_SCRAPE_PHASE2
    )

    end_run_time = time.time()
    total_duration = end_run_time - start_run_time
    logging.info(f"--- Обработка завершена за {total_duration:.2f} секунд ---")


    print("\n--- Результаты скрапинга (Selenium Поиск + Requests Скрапинг) ---")
    import json
    # Отфильтруем текст для краткости вывода
    results_summary = []
    for r in final_results:
        summary = r.copy()
        if 'text' in summary and summary['text']:
            summary['text'] = summary['text'][:100] + '...' # Показываем только начало текста
        results_summary.append(summary)

    print(json.dumps(results_summary, indent=2, ensure_ascii=False))

    print(f"\nВсего собрано записей (включая ошибки поиска): {len(final_results)}")

    # Анализ результатов
    success_scrape_count = sum(1 for r in final_results if r.get('status') == 'success')
    search_failed_count = sum(1 for r in final_results if r.get('status') == 'search_failed_selenium')
    scrape_error_count = sum(1 for r in final_results if r.get('status') == 'error')

    print(f"Успешных скрапингов (Фаза 2): {success_scrape_count}")
    print(f"Ошибок/пропусков поиска (Фаза 1): {search_failed_count}")
    print(f"Ошибок скрапинга (Фаза 2): {scrape_error_count}")