import time
import random
import json
import requests
import io # Для работы с байтами PDF в памяти
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import quote_plus, urlparse
from typing import List, Dict, Tuple, Any, Optional

# --- Установите эти библиотеки ---
# pip install beautifulsoup4 selenium webdriver-manager requests lxml # lxml опционально, но рекомендуется
# pip install pymupdf # Для обработки PDF
try:
    import fitz  # PyMuPDF
    PDF_PARSER_AVAILABLE = True
except ImportError:
    print("ПРЕДУПРЕЖДЕНИЕ: Библиотека PyMuPDF не найдена.")
    print("Функциональность извлечения текста из PDF будет недоступна.")
    print("Пожалуйста, установите ее: pip install pymupdf")
    PDF_PARSER_AVAILABLE = False

# --- Общие Настройки ---
NUM_RESULTS_PER_QUERY_DDG = 3 # Кол-во ссылок на запрос для DuckDuckGo
MIN_DELAY_DDG = 0.5  # Мин. задержка между запросами DDG (Selenium)
MAX_DELAY_DDG = 1.5  # Макс. задержка между запросами DDG (Selenium)
MIN_DELAY_FETCH = 0.2 # Мин. задержка между скачиванием HTML/PDF
MAX_DELAY_FETCH = 0.6 # Макс. задержка между скачиванием HTML/PDF

# Настройки для requests (HTML и PDF скачивание)
REQUESTS_TIMEOUT_HTML = 10 # Таймаут для HTML
REQUESTS_TIMEOUT_PDF = 25 # Таймаут для PDF (может быть больше)
REQUESTS_HEADERS_GENERAL = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

# --- Настройки Semantic Scholar API ---
SEMANTIC_SCHOLAR_API_URL = "https://api.semanticscholar.org/graph/v1"
# ВАЖНО: Замените на ваш ключ API Semantic Scholar для надежности и лимитов!
SEMANTIC_SCHOLAR_API_KEY: Optional[str] = None # <--- ВАШ КЛЮЧ ЗДЕСЬ или None
S2_API_REQUEST_TIMEOUT = 12 # Таймаут для запросов к API S2
S2_HEADERS = {"Accept": "application/json"}
if SEMANTIC_SCHOLAR_API_KEY:
    S2_HEADERS["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY
# Задержка ПОСЛЕ каждого запроса к API S2 (1.1 сек рекомендуется с ключом)
S2_API_DELAY = 1.1 if SEMANTIC_SCHOLAR_API_KEY else 0.6
# Сколько статей из API S2 пытаться обработать (скачать/извлечь PDF)
S2_MAX_PAPERS_TO_PROCESS_PER_QUERY = 3


# --- Функция парсинга ссылок и заголовков DuckDuckGo (без изменений) ---
def scrape_duckduckgo_links_titles(queries: List[str], num_results: int) -> List[List[Dict[str, str]]]:
    """
    Ищет запросы в DuckDuckGo через Selenium и возвращает список списков ссылок/заголовков.
    (КОД ОСТАЕТСЯ ПРЕЖНИМ, как в вашем исходном примере)
    """
    all_results: List[List[Dict[str, str]]] = []
    driver = None
    if not queries: return [] # Проверка на пустой список запросов

    try:
        print("Инициализация WebDriver для Chrome (DuckDuckGo)...")
        chrome_options = Options()
        # chrome_options.add_argument("--headless") # Раскомментируйте для фонового режима
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={REQUESTS_HEADERS_GENERAL['User-Agent']}")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Используем webdriver-manager для автоматической установки/обновления драйвера
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_window_size(1920, 1080) # Установка размера окна
        except Exception as e_wdm:
            print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось инициализировать WebDriver через webdriver-manager: {e_wdm}")
            print("Убедитесь, что Chrome установлен и доступен, или попробуйте указать путь к chromedriver вручную.")
            return [[] for _ in queries] # Возвращаем пустые списки для всех запросов

        print(f"Запуск обработки {len(queries)} запросов в DuckDuckGo...")

        for i, query in enumerate(queries):
            print("-" * 20)
            print(f"Обработка DDG запроса {i+1}/{len(queries)}: '{query}'")
            current_query_results: List[Dict[str, str]] = []

            if not query:
                print("  Пропущен пустой запрос.")
                all_results.append([])
                continue

            try:
                search_url = f"https://duckduckgo.com/?q={quote_plus(query)}&ia=web"
                print(f"  Переход на: {search_url}")
                driver.get(search_url)

                # Обновленный и более надежный селектор для органических результатов DDG
                results_selector = (By.CSS_SELECTOR, 'article[data-testid="result"] a[data-testid="result-title-a"]')

                print("  Ожидание загрузки результатов DDG...")
                try:
                    WebDriverWait(driver, 15).until(
                        EC.visibility_of_element_located(results_selector)
                    )
                    print("  Результаты DDG загружены.")
                except Exception as e_wait:
                     print(f"  Ошибка: Результаты поиска DuckDuckGo не найдены или не загрузились для запроса '{query}'. {e_wait}")
                     all_results.append([])
                     # Задержка перед следующим запросом важна, даже если текущий неудачен
                     if i < len(queries) - 1:
                         delay = random.uniform(MIN_DELAY_DDG, MAX_DELAY_DDG)
                         print(f"  Пауза {delay:.2f} сек перед следующим запросом DDG...")
                         time.sleep(delay)
                     continue # Переходим к следующему запросу

                print(f"  Извлечение до {num_results} ссылок и заголовков из DDG...")
                result_elements = driver.find_elements(*results_selector) # Используем новый селектор

                count = 0
                processed_urls_for_query = set() # Отслеживаем уникальные URL для текущего запроса
                for element in result_elements:
                    if count >= num_results:
                        break
                    try:
                        # Получаем href из родительского элемента 'a'
                        href = element.get_attribute('href')
                        # Получаем текст из дочернего span внутри ссылки
                        title_span = element.find_element(By.TAG_NAME, 'span')
                        title = title_span.text.strip() if title_span else element.text.strip() # Запасной вариант

                        # Дополнительные проверки валидности
                        if href and title and href.startswith('http') and \
                           'duckduckgo.com' not in urlparse(href).netloc and \
                           href not in processed_urls_for_query:
                             # Проверка, что ссылка не ведет обратно на DDG
                             parsed_href = urlparse(href)
                             if 'ad_provider' not in parsed_href.query and 'ad_domain' not in parsed_href.query:
                                 current_query_results.append({'url': href, 'title': title})
                                 processed_urls_for_query.add(href)
                                 count += 1
                                 print(f"    Найдено DDG: {title[:60]}... ({href[:60]}...)")


                    except Exception as e_extract:
                        # Логируем, но не прерываем цикл из-за одного элемента
                        print(f"    Предупреждение: Не удалось извлечь данные из элемента DDG: {e_extract}")
                        continue

                if not current_query_results:
                     print("  Не найдено релевантных ссылок/заголовков в DDG по указанным селекторам.")

            except Exception as e_query:
                print(f"  Произошла ошибка Selenium при обработке DDG запроса '{query}': {e_query}")
                current_query_results = [] # Обнуляем результаты для этого запроса в случае ошибки

            all_results.append(current_query_results)
            print(f"  Найдено {len(current_query_results)} ссылок в DDG для '{query}'.")

            # Задержка перед СЛЕДУЮЩИМ запросом к DDG
            if i < len(queries) - 1:
                delay = random.uniform(MIN_DELAY_DDG, MAX_DELAY_DDG)
                print(f"  Пауза {delay:.2f} сек перед следующим запросом DDG...")
                time.sleep(delay)

    except Exception as e_main:
        print(f"Произошла критическая ошибка WebDriver при работе с DuckDuckGo: {e_main}")
        # В случае критической ошибки вернем пустые списки для всех оставшихся запросов
        num_remaining = len(queries) - len(all_results)
        all_results.extend([[] for _ in range(num_remaining)])

    finally:
        if driver:
            print("Закрытие браузера Selenium...")
            try:
                driver.quit()
            except Exception as e_quit:
                print(f"  Предупреждение: Ошибка при закрытии драйвера: {e_quit}")

    return all_results


# --- Функция извлечения текста из HTML (requests + bs4) ---
def fetch_and_extract_html_text(url: str) -> Dict[str, Any]:
    """
    Загружает HTML контент по URL и извлекает текст с помощью BeautifulSoup.
    """
    print(f"    [HTML] Попытка извлечь текст с: {url[:80]}...")
    extracted_text = None
    content_length = 0
    status = 'pending'
    error_message = None
    final_url = url # Сохраняем URL после редиректов

    try:
        response = requests.get(url, headers=REQUESTS_HEADERS_GENERAL, timeout=REQUESTS_TIMEOUT_HTML, allow_redirects=True)
        final_url = response.url # Обновляем URL на случай редиректа
        response.raise_for_status() # Проверяем на HTTP ошибки (4xx, 5xx)

        content_type = response.headers.get('content-type', '').lower()
        # Проверяем, что это ТОЧНО HTML, а не PDF или что-то еще
        if 'html' not in content_type:
            status = 'not_html'
            error_message = f"Контент не HTML ({content_type})"
            print(f"    ! [HTML] Ошибка: {error_message} для {final_url}")
            return {'text': None, 'content_length': 0, 'status': status, 'error_message': error_message, 'final_url': final_url}

        # Используем 'lxml' если установлен, иначе 'html.parser'
        try:
             soup = BeautifulSoup(response.content, 'lxml')
        except ImportError:
             print("    [HTML] Предупреждение: lxml не установлен, используется html.parser (может быть медленнее).")
             soup = BeautifulSoup(response.content, 'html.parser')
        except Exception as e_bs_init:
            raise RuntimeError(f"Ошибка инициализации BeautifulSoup: {e_bs_init}") # Перевыбрасываем серьезную ошибку

        # --- Улучшенная эвристика извлечения текста для HTML ---
        # 1. Удаляем ненужные теги
        for element in soup(["script", "style", "nav", "footer", "header", "aside", "form", "button", "figure", "figcaption", "iframe", "select", "textarea"]):
            element.decompose()

        # 2. Пытаемся найти основной контент (более широкий набор селекторов)
        main_content = (soup.find('article')
                        or soup.find('main')
                        or soup.find('div', role='main')
                        or soup.find('div', id='main-content') # Общие ID
                        or soup.find('div', class_='main-content')
                        or soup.find('div', id='content')
                        or soup.find('div', class_='content')
                        or soup.find('div', class_='post-content') # Для блогов
                        or soup.find('div', class_='entry-content')
                        or soup.find('div', class_='article-body') # Для новостей/статей
                        or soup.find('section', id='content')
                        )

        text_parts = []
        if main_content:
            # Ищем заголовки и параграфы внутри основного контента
            tags_to_extract = main_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'li']) # Добавили заголовки и списки
            if tags_to_extract:
                for tag in tags_to_extract:
                     # Проверяем, что внутри нет таблиц или слишком короткий текст
                     if not tag.find('table') and len(tag.get_text(strip=True)) > 15:
                          text_parts.append(tag.get_text(separator=' ', strip=True))
            else: # Если специфичных тегов нет, берем весь текст из блока
                 extracted_text = main_content.get_text(separator=' ', strip=True)
        else:
            # 4. Если основной блок не найден, берем параграфы со всей страницы
            paragraphs = soup.find_all('p')
            if paragraphs:
                 text_parts = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20 and not p.find('table')]
            else:
                 # 5. Как крайняя мера, берем весь видимый текст (может быть шумно)
                 extracted_text = soup.get_text(separator=' ', strip=True)

        if text_parts:
             extracted_text = ' '.join(text_parts)

        # Финальная очистка
        if extracted_text:
            extracted_text = ' '.join(extracted_text.split()) # Убираем лишние пробелы
            if len(extracted_text) < 50: # Слишком короткий текст - вероятно, шум
                 status = 'extraction_failed'
                 error_message = "Извлеченный текст слишком короткий (< 50 симв)."
                 extracted_text = None
                 print(f"    ! [HTML] Ошибка: {error_message} для {final_url}")
            else:
                 content_length = len(extracted_text)
                 status = 'success'
                 print(f"    + [HTML] Успешно извлечено {content_length} символов.")
        else:
            status = 'extraction_failed'
            error_message = "Не удалось извлечь текст (контент не найден или пуст)."
            print(f"    ! [HTML] Ошибка: {error_message} для {final_url}")

    except requests.exceptions.Timeout:
        status = 'fetch_failed'
        error_message = f"Таймаут ({REQUESTS_TIMEOUT_HTML} сек) при получении {url}"
        print(f"    ! [HTML] Ошибка: {error_message}")
    except requests.exceptions.HTTPError as e:
         status = 'fetch_failed'
         error_message = f"HTTP ошибка: {e.response.status_code} {e.response.reason} для {final_url}"
         print(f"    ! [HTML] Ошибка: {error_message}")
    except requests.exceptions.RequestException as e:
        status = 'fetch_failed'
        error_message = f"Ошибка сети/запроса при получении {url}: {e}"
        print(f"    ! [HTML] Ошибка: {error_message}")
    except Exception as e:
        status = 'extraction_failed' # Ошибка скорее всего при парсинге BS4
        error_message = f"Ошибка при парсинге HTML {final_url}: {e}"
        print(f"    ! [HTML] Ошибка: {error_message}")
        import traceback
        traceback.print_exc(limit=1) # Печать краткого стека

    return {'text': extracted_text, 'content_length': content_length, 'status': status, 'error_message': error_message, 'final_url': final_url}

# --- Функции для работы с PDF ---
def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> Optional[str]:
    """Извлекает текст из PDF с помощью PyMuPDF."""
    if not PDF_PARSER_AVAILABLE: return None
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        full_text = ""
        for page in doc:
            full_text += page.get_text("text")
        doc.close()
        cleaned_text = ' '.join(full_text.split())
        return cleaned_text if len(cleaned_text) > 50 else None # Возвращаем None, если текст слишком короткий
    except Exception as e:
        print(f"    ! [PDF] Ошибка PyMuPDF при парсинге: {e}")
        return None

def fetch_pdf_content(pdf_url: str) -> Optional[bytes]:
    """Скачивает содержимое PDF по URL."""
    print(f"    [PDF] Попытка скачать с: {pdf_url[:100]}...")
    try:
        response = requests.get(pdf_url, timeout=REQUESTS_TIMEOUT_PDF, headers=REQUESTS_HEADERS_GENERAL, stream=True)
        response.raise_for_status()
        content_type = response.headers.get('content-type', '').lower()
        if 'pdf' not in content_type:
            print(f"    ! [PDF] Предупреждение: Content-Type не PDF ({content_type}) для {pdf_url}")
            # Можно вернуть None здесь, если нужна строгая проверка
        pdf_bytes = response.content
        print(f"    + [PDF] Успешно скачан ({len(pdf_bytes) / 1024:.1f} KB).")
        return pdf_bytes
    except requests.exceptions.Timeout:
        print(f"    ! [PDF] Таймаут при скачивании с {pdf_url}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    ! [PDF] Ошибка сети/HTTP при скачивании с {pdf_url}: {e}")
        return None
    except Exception as e:
        print(f"    ! [PDF] Непредвиденная ошибка при скачивании {pdf_url}: {e}")
        return None

# --- Функция для работы с Semantic Scholar API ---
def search_semantic_scholar(
    topic: str,
    limit: int,
    fields: List[str] = ["paperId", "title", "abstract", "year", "authors", "isOpenAccess", "openAccessPdf", "url", "externalIds"]
) -> Optional[Dict[str, Any]]:
    """Выполняет поисковый запрос к Semantic Scholar API."""
    print(f"  [S2 API] Поиск по теме: '{topic}' (лимит: {limit})")
    search_url = f"{SEMANTIC_SCHOLAR_API_URL}/paper/search"
    query_params = {
        'query': topic,
        'limit': limit,
        'fields': ",".join(fields)
    }
    try:
        response = requests.get(search_url, params=query_params, headers=S2_HEADERS, timeout=S2_API_REQUEST_TIMEOUT)
        # --- Задержка ПОСЛЕ запроса к API ---
        print(f"    [S2 API] Пауза {S2_API_DELAY:.1f} сек...")
        time.sleep(S2_API_DELAY)
        # -------------------------------------
        response.raise_for_status()
        print("    [S2 API] Запрос успешен.")
        return response.json()
    except requests.exceptions.Timeout:
        print(f"    ! [S2 API] Таймаут при запросе.")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"    ! [S2 API] HTTP ошибка: {e.response.status_code} {e.response.reason}")
        if e.response.status_code == 429: print("      -> Превышен лимит запросов!")
        try: print(f"      Тело ответа: {e.response.json()}")
        except: print(f"      Тело ответа: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    ! [S2 API] Ошибка сети: {e}")
        return None
    except Exception as e:
        print(f"    ! [S2 API] Непредвиденная ошибка: {e}")
        return None


# --- НОВАЯ Основная функция обработки задач ---
def process_tasks_combined(tasks: List[Dict[str, Any]],
                           num_ddg_results: int = NUM_RESULTS_PER_QUERY_DDG,
                           num_s2_papers_to_process: int = S2_MAX_PAPERS_TO_PROCESS_PER_QUERY
                           ) -> List[Dict[str, Any]]:
    """
    Обрабатывает задачи: ищет в DuckDuckGo (HTML) и Semantic Scholar (PDF),
    извлекает текст и возвращает комбинированный список результатов.
    """
    print(f"Начало КОМБИНИРОВАННОЙ обработки {len(tasks)} задач...")
    final_output: List[Dict[str, Any]] = []

    # 1. Подготовка запросов для DuckDuckGo
    ddg_queries = [task.get('query', '') for task in tasks if task.get('query')]
    valid_ddg_queries = list(dict.fromkeys(ddg_queries)) # Уникальные непустые запросы

    # 2. Поиск ссылок в DuckDuckGo (один раз для всех уникальных запросов)
    print("\n=== Шаг 1: Поиск ссылок в DuckDuckGo ===")
    ddg_results_map: Dict[str, List[Dict[str, str]]] = {}
    if valid_ddg_queries:
        scraped_ddg_results = scrape_duckduckgo_links_titles(valid_ddg_queries, num_ddg_results)
        # Сопоставляем результаты с исходными запросами
        for query, results in zip(valid_ddg_queries, scraped_ddg_results):
            ddg_results_map[query] = results
    else:
        print("Нет валидных запросов для поиска в DuckDuckGo.")

    # 3. Обработка каждой задачи
    print("\n=== Шаг 2: Обработка каждой задачи (DDG HTML + S2 PDF) ===")
    processed_urls = set() # Отслеживаем УЖЕ обработанные URL (из любого источника)

    for task in tasks:
        task_query = task.get('query', '')
        plan_item = task.get('plan_item', '')
        plan_item_id = task.get('plan_item_id', '')
        query_id = task.get('query_id', '')
        print(f"\n--- Обработка задачи: '{task_query}' (ID: {query_id}) ---")

        if not task_query:
            print("  Пропуск задачи с пустым запросом.")
            # Можно добавить запись об ошибке, если нужно
            final_output.append({
                "query": task_query, "plan_item": plan_item, "plan_item_id": plan_item_id, "query_id": query_id,
                "source": "task_error", "status": "invalid_query", "error_message": "Пустой запрос в задаче",
                "url": None, "title": None, "text": None, "content_length": 0
            })
            continue

        # --- 3.1 Обработка результатов DuckDuckGo (HTML) ---
        print(f"  -> Поиск HTML результатов из DuckDuckGo для '{task_query}'...")
        ddg_links_for_task = ddg_results_map.get(task_query, [])
        if ddg_links_for_task:
            print(f"    Найдено {len(ddg_links_for_task)} ссылок в DDG.")
            for link_data in ddg_links_for_task:
                url = link_data.get('url')
                title = link_data.get('title')
                if not url or url in processed_urls: # Пропускаем пустые или уже обработанные URL
                     if url: print(f"      Пропуск уже обработанного URL: {url[:80]}...")
                     continue

                # Извлекаем HTML текст
                html_content_result = fetch_and_extract_html_text(url)
                processed_urls.add(html_content_result.get('final_url', url)) # Добавляем конечный URL

                # Формируем результат
                result_item = {
                    "query": task_query, "plan_item": plan_item, "plan_item_id": plan_item_id, "query_id": query_id,
                    "source": "duckduckgo",
                    "url": html_content_result.get('final_url', url), # Используем конечный URL
                    "title": title,
                    "text": html_content_result.get('text'),
                    "extraction_method": "requests+bs4" if html_content_result.get('status') == 'success' else None,
                    "content_length": html_content_result.get('content_length', 0),
                    "status": html_content_result.get('status'),
                    "error_message": html_content_result.get('error_message')
                }
                final_output.append(result_item)
                # Пауза между запросами к сайтам
                time.sleep(random.uniform(MIN_DELAY_FETCH, MAX_DELAY_FETCH))
        else:
            print("    Ссылки из DuckDuckGo для этого запроса не найдены или не были получены.")

        # --- 3.2 Поиск и обработка результатов Semantic Scholar (PDF) ---
        print(f"\n  -> Поиск научных статей (PDF) через Semantic Scholar API для '{task_query}'...")
        if not PDF_PARSER_AVAILABLE:
             print("     Пропуск поиска S2: библиотека PyMuPDF недоступна.")
             continue # Переходим к следующей задаче, если парсер PDF не работает

        s2_results_data = search_semantic_scholar(task_query, num_s2_papers_to_process)

        if s2_results_data and 'data' in s2_results_data:
            s2_papers = s2_results_data['data']
            print(f"    API S2 вернуло {len(s2_papers)} статей.")

            for s2_paper in s2_papers:
                 paper_id = s2_paper.get('paperId')
                 paper_title = s2_paper.get('title')
                 paper_url = s2_paper.get('url') # Ссылка на страницу статьи

                 # Проверяем, не обработали ли мы уже страницу этой статьи через DDG
                 if paper_url and paper_url in processed_urls:
                     print(f"      Пропуск статьи S2, т.к. ее URL уже обработан: {paper_url[:80]}...")
                     continue

                 # Готовим базовую информацию о статье
                 s2_result_item = {
                    "query": task_query, "plan_item": plan_item, "plan_item_id": plan_item_id, "query_id": query_id,
                    "source": "semantic_scholar",
                    "paperId": paper_id,
                    "url": paper_url, # Ссылка на страницу статьи
                    "title": paper_title,
                    "year": s2_paper.get('year'),
                    "authors": [a['name'] for a in s2_paper.get('authors', []) if 'name' in a],
                    "abstract": s2_paper.get('abstract'),
                    "isOpenAccess": s2_paper.get('isOpenAccess'),
                    "externalIds": s2_paper.get('externalIds'),
                    "pdf_url": None, # Инициализируем поля PDF
                    "text": None, # Текст будет извлечен ниже, если возможно
                    "extraction_method": None,
                    "content_length": 0,
                    "status": "pending_pdf_check", # Начальный статус для S2
                    "error_message": None
                 }

                 is_oa = s2_paper.get('isOpenAccess')
                 oa_pdf_info = s2_paper.get('openAccessPdf')
                 pdf_dl_url: Optional[str] = None

                 if oa_pdf_info and isinstance(oa_pdf_info, dict):
                      pdf_dl_url = oa_pdf_info.get('url')

                 if is_oa and pdf_dl_url:
                      s2_result_item["pdf_url"] = pdf_dl_url
                      print(f"      Найдена ссылка на Open Access PDF для '{paper_title[:50]}...': {pdf_dl_url[:80]}...")

                      # Проверяем, не обработан ли уже сам PDF URL (менее вероятно, но возможно)
                      if pdf_dl_url in processed_urls:
                           print(f"      Пропуск PDF, т.к. этот URL уже обработан: {pdf_dl_url[:80]}...")
                           s2_result_item["status"] = "skipped_duplicate_url"
                           final_output.append(s2_result_item)
                           continue

                      # Пытаемся скачать и извлечь текст PDF
                      pdf_content = fetch_pdf_content(pdf_dl_url)
                      processed_urls.add(pdf_dl_url) # Добавляем URL PDF в обработанные
                      time.sleep(random.uniform(MIN_DELAY_FETCH, MAX_DELAY_FETCH)) # Пауза после скачивания

                      if pdf_content:
                           pdf_text = extract_text_from_pdf_bytes(pdf_content)
                           if pdf_text:
                                s2_result_item["text"] = pdf_text
                                s2_result_item["content_length"] = len(pdf_text)
                                s2_result_item["status"] = "success"
                                s2_result_item["extraction_method"] = "semantic_scholar_api+pymupdf"
                                print(f"        + [PDF] Успешно извлечено {len(pdf_text)} симв.")
                           else:
                                s2_result_item["status"] = "pdf_extraction_failed"
                                s2_result_item["error_message"] = "Не удалось извлечь текст из скачанного PDF."
                                print(f"        ! [PDF] Ошибка извлечения текста.")
                      else:
                           s2_result_item["status"] = "pdf_fetch_failed"
                           s2_result_item["error_message"] = "Не удалось скачать PDF контент."
                           print(f"        ! [PDF] Ошибка скачивания.")

                 elif is_oa:
                      s2_result_item["status"] = "oa_pdf_link_missing"
                      s2_result_item["error_message"] = "Статья Open Access, но ссылка на PDF не найдена API."
                      print(f"      Статья OA, но ссылка на PDF отсутствует для '{paper_title[:50]}...'.")
                 else:
                      s2_result_item["status"] = "not_open_access"
                      s2_result_item["error_message"] = "Статья не в открытом доступе (по данным API)."
                      print(f"      Статья не OA '{paper_title[:50]}...'. PDF не обрабатывается.")

                 # Добавляем информацию о статье S2 (даже если PDF не извлечен)
                 # Также добавляем основной URL страницы статьи в обработанные, если он есть
                 if paper_url:
                      processed_urls.add(paper_url)
                 final_output.append(s2_result_item)

        elif s2_results_data is None:
             print("    Ошибка при запросе к Semantic Scholar API.")
        else: # s2_results_data пуст или не содержит 'data'
             print("    Semantic Scholar API не вернул статьи по этому запросу.")


    print("\n=== КОМБИНИРОВАННАЯ обработка всех задач завершена ===")
    return final_output


# --- Входные данные (можно добавить более научные запросы) ---
test_tasks = [
    {'query': "применение трансформеров в NLP", 'plan_item': "Обзор трансформеров", 'plan_item_id': "plan_0", 'query_id': "q_0_0"},
    {'query': "BERT model fine-tuning for text classification", 'plan_item': "Fine-tuning BERT", 'plan_item_id': "plan_bert", 'query_id': "q_bert_0"}, # Научный запрос
    {'query': "React component lifecycle hooks", 'plan_item': "React Lifecycle", 'plan_item_id': "plan_1", 'query_id': "q_1_0"},
    {'query': "diffusion models for image generation survey", 'plan_item': "Diffusion Models Review", 'plan_item_id': "plan_diff", 'query_id': "q_diff_0"}, # Научный запрос
    {'query': "методы кластеризации данных", 'plan_item': "Кластеризация", 'plan_item_id': "plan_2", 'query_id': "q_2_0"},
    {'query': "quantum computing algorithms review", 'plan_item': "Quantum Review", 'plan_item_id': "plan_quantum_review", 'query_id': "q_quantum_1"}, # Научный запрос
    {'query': "несуществующая чепуха абракадабра xyzzy фываолдж", 'plan_item': "Тест ошибки поиска", 'plan_item_id': "plan_3", 'query_id': "q_3_0"},
    {'query': "", 'plan_item': "Пустой запрос", 'plan_item_id': "plan_empty", 'query_id': "q_empty_0"}
]

# --- Запуск обработки и вывод результата ---
if __name__ == "__main__":
    start_time = time.time()

    final_results_combined = process_tasks_combined(
        test_tasks,
        num_ddg_results=NUM_RESULTS_PER_QUERY_DDG,
        num_s2_papers_to_process=S2_MAX_PAPERS_TO_PROCESS_PER_QUERY
    )

    end_time = time.time()
    print(f"\n\nОбщее время выполнения: {end_time - start_time:.2f} секунд")

    print("\n" + "=" * 30)
    print("--- Итоговые КОМБИНИРОВАННЫЕ результаты ---")
    print("=" * 30)
    # Печать для отладки (может быть очень много текста)
    # print(json.dumps(final_results_combined, indent=2, ensure_ascii=False))

    # Сводка по результатам
    print(f"\nВсего получено записей: {len(final_results_combined)}")
    ddg_success = sum(1 for r in final_results_combined if r.get('source') == 'duckduckgo' and r.get('status') == 'success')
    s2_pdf_success = sum(1 for r in final_results_combined if r.get('source') == 'semantic_scholar' and r.get('status') == 'success' and r.get('extraction_method') == 'semantic_scholar_api+pymupdf')
    s2_total = sum(1 for r in final_results_combined if r.get('source') == 'semantic_scholar')
    errors = sum(1 for r in final_results_combined if 'fail' in r.get('status', '') or 'error' in r.get('status', ''))

    print(f"  Успешно извлечено HTML (DuckDuckGo): {ddg_success}")
    print(f"  Всего найдено статей (Semantic Scholar): {s2_total}")
    print(f"  Успешно извлечено из PDF (Semantic Scholar): {s2_pdf_success}")
    print(f"  Записей с ошибками/неудачами: {errors}")


    # Сохранение в файл
    output_filename = "processed_combined_search_results.json"
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            # Сохраняем только основные поля для читаемости, если нужно
            # filtered_results = [{k: v for k, v in r.items() if k != 'text'} for r in final_results_combined]
            # json.dump(filtered_results, f, ensure_ascii=False, indent=2)
            # Или сохраняем все
            json.dump(final_results_combined, f, ensure_ascii=False, indent=2)

        print(f"\nРезультаты также сохранены в файл: {output_filename}")
    except Exception as e_save:
        print(f"\nНе удалось сохранить результаты в JSON: {e_save}")