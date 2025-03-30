import time
import random
import json
import requests # Добавлено
from bs4 import BeautifulSoup # Добавлено
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import quote_plus, urlparse # Добавлено urlparse
from typing import List, Dict, Tuple, Any, Optional # Обновлены аннотации

# --- Настройки ---
NUM_RESULTS_PER_QUERY = 3 # Кол-во ссылок на запрос
MIN_DELAY = 0.5
MAX_DELAY = 1.5
# Настройки для requests
REQUESTS_TIMEOUT = 10 # Секунд на ожидание ответа от сайта
REQUESTS_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8', # Пример заголовка языка
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}


# --- Функция парсинга ссылок и заголовков (без изменений) ---
def scrape_duckduckgo_links_titles(queries: List[str], num_results: int = 5) -> List[List[Dict[str, str]]]:
    """
    Открывает браузер ОДИН РАЗ, выполняет поиск для КАЖДОГО запроса из списка
    в DuckDuckGo и извлекает первые N ссылок и их заголовков для каждого запроса.
    (Код этой функции остается таким же, как в предыдущем ответе)
    """
    all_results: List[List[Dict[str, str]]] = []
    driver = None

    try:
        print("Инициализация WebDriver для Chrome...")
        chrome_options = Options()
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={REQUESTS_HEADERS['User-Agent']}") # Используем тот же User-Agent
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_window_size(1920, 1080)

        print(f"Запуск обработки {len(queries)} запросов...")

        for i, query in enumerate(queries):
            print("-" * 20)
            print(f"Обработка запроса {i+1}/{len(queries)}: '{query}'")
            current_query_results: List[Dict[str, str]] = []

            if not query:
                print("  Пропущен пустой запрос.")
                all_results.append([])
                continue

            try:
                search_url = f"https://duckduckgo.com/?q={quote_plus(query)}&ia=web"
                print(f"  Переход на: {search_url}")
                driver.get(search_url)

                results_selector = (By.CSS_SELECTOR, 'li[data-layout="organic"] h2 a[href]')

                print("  Ожидание загрузки результатов...")
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located(results_selector)
                    )
                    print("  Результаты загружены.")
                    selector_to_use = results_selector
                except Exception as e_wait:
                    try:
                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "links")))
                        print("  Контейнер результатов найден, но ссылки по селектору не появились сразу.")
                        selector_to_use = results_selector
                    except Exception:
                         print(f"  Ошибка: Результаты поиска DuckDuckGo не найдены или не загрузились для запроса '{query}'. {e_wait}")
                         all_results.append([])
                         delay = random.uniform(MIN_DELAY, MAX_DELAY)
                         if i < len(queries) - 1:
                              print(f"  Пауза {delay:.2f} сек перед следующим запросом...")
                              time.sleep(delay)
                         continue

                print(f"  Извлечение до {num_results} ссылок и заголовков...")
                result_elements = driver.find_elements(*selector_to_use)

                count = 0
                processed_urls_for_query = set()
                for element in result_elements:
                    try:
                        href = element.get_attribute('href')
                        title = element.text.strip()

                        if href and title and href.startswith('http') and \
                           'duckduckgo.com' not in href and href not in processed_urls_for_query:
                             current_query_results.append({'url': href, 'title': title})
                             processed_urls_for_query.add(href)
                             count += 1
                             if count >= num_results:
                                 break
                    except Exception as e_extract:
                        print(f"    Предупреждение: Не удалось извлечь данные из элемента: {e_extract}")
                        continue

                if not current_query_results:
                     print("  Не найдено релевантных ссылок/заголовков по указанным селекторам.")

            except Exception as e_query:
                print(f"  Произошла ошибка при обработке запроса '{query}': {e_query}")
                current_query_results = []

            all_results.append(current_query_results)
            print(f"  Найдено {len(current_query_results)} ссылок для '{query}'.")

            if i < len(queries) - 1:
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                print(f"  Пауза {delay:.2f} сек перед следующим запросом...")
                time.sleep(delay)

    except Exception as e_main:
        print(f"Произошла критическая ошибка WebDriver: {e_main}")

    finally:
        if driver:
            print("Закрытие браузера...")
            driver.quit()

    return all_results


# --- НОВАЯ Функция для извлечения текста ---
def fetch_and_extract_text(url: str) -> Dict[str, Any]:
    """
    Загружает контент по URL с помощью requests и извлекает текст с помощью BeautifulSoup.

    Args:
        url (str): URL-адрес страницы.

    Returns:
        Dict[str, Any]: Словарь с результатами:
            'text': Извлеченный текст (str) или None в случае ошибки.
            'content_length': Длина текста (int) или 0.
            'status': Статус извлечения ('success', 'fetch_failed', 'extraction_failed', 'not_html').
            'error_message': Сообщение об ошибке (str) или None.
    """
    print(f"    Попытка извлечь текст с: {url[:80]}...") # Обрезаем длинные URL для лога
    extracted_text = None
    content_length = 0
    status = 'pending'
    error_message = None

    try:
        response = requests.get(url, headers=REQUESTS_HEADERS, timeout=REQUESTS_TIMEOUT, allow_redirects=True)
        response.raise_for_status() # Проверяем на HTTP ошибки (4xx, 5xx)

        content_type = response.headers.get('content-type', '').lower()
        if 'html' not in content_type:
            status = 'not_html'
            error_message = f"Контент не является HTML ({content_type})"
            print(f"    ! Ошибка: {error_message}")
            return {'text': None, 'content_length': 0, 'status': status, 'error_message': error_message}

        # Используем 'html.parser' или 'lxml' если установлен (pip install lxml)
        # 'lxml' обычно быстрее и надежнее
        try:
             soup = BeautifulSoup(response.content, 'lxml')
        except: # Если lxml не установлен, используем встроенный
             soup = BeautifulSoup(response.content, 'html.parser')


        # --- Эвристика извлечения текста (можно улучшать) ---
        # 1. Удаляем ненужные теги
        for element in soup(["script", "style", "nav", "footer", "header", "aside", "form", "button"]):
            element.decompose()

        # 2. Пытаемся найти основной контент (эти селекторы могут не работать на всех сайтах)
        main_content = soup.find('main') or soup.find('article') or soup.find('div', role='main') or soup.find('div', id='content') or soup.find('div', class_='content') # Добавлены ID/class

        # 3. Если нашли основной блок, ищем параграфы (<p>) внутри него
        if main_content:
            paragraphs = main_content.find_all('p')
            # Если параграфов мало, пробуем взять весь текст из основного блока
            if len(paragraphs) < 3:
                 extracted_text = main_content.get_text(separator=' ', strip=True)
            else:
                 extracted_text = ' '.join(p.get_text(strip=True) for p in paragraphs)
        else:
            # 4. Если основной блок не найден, просто берем все параграфы на странице
            paragraphs = soup.find_all('p')
            if paragraphs:
                 extracted_text = ' '.join(p.get_text(strip=True) for p in paragraphs)
            else:
                 # 5. Как крайняя мера, берем весь видимый текст (может быть шумно)
                 extracted_text = soup.get_text(separator=' ', strip=True)


        if extracted_text:
            # Простая очистка от лишних пробелов
            extracted_text = ' '.join(extracted_text.split())
            content_length = len(extracted_text)
            status = 'success'
            print(f"    + Успешно извлечено {content_length} символов.")
        else:
            status = 'extraction_failed'
            error_message = "Не удалось извлечь текст после парсинга HTML."
            print(f"    ! Ошибка: {error_message}")

    except requests.exceptions.Timeout:
        status = 'fetch_failed'
        error_message = f"Превышен таймаут ({REQUESTS_TIMEOUT} сек)."
        print(f"    ! Ошибка: {error_message}")
    except requests.exceptions.RequestException as e:
        status = 'fetch_failed'
        error_message = f"Ошибка сети/HTTP: {e}"
        print(f"    ! Ошибка: {error_message}")
    except Exception as e:
        status = 'extraction_failed' # Ошибка скорее всего при парсинге BS4
        error_message = f"Ошибка при парсинге HTML: {e}"
        print(f"    ! Ошибка: {error_message}")


    return {'text': extracted_text, 'content_length': content_length, 'status': status, 'error_message': error_message}


# --- Основная функция обработки задач (модифицирована) ---
def process_search_tasks_with_content_extraction(tasks: List[Dict[str, Any]], num_results_per_query: int = 5) -> List[Dict[str, Any]]:
    """
    Обрабатывает список поисковых задач, извлекает ссылки через Selenium,
    а затем извлекает контент каждой ссылки с помощью requests и BeautifulSoup.

    Args:
        tasks (list): Список словарей с задачами.
        num_results_per_query (int): Кол-во ссылок на запрос для скрейпера.

    Returns:
        list: Список словарей в требуемом выходном формате с извлеченным текстом.
    """
    print(f"Начало обработки {len(tasks)} задач (включая извлечение контента)...")
    final_output = []

    # 1. Извлекаем запросы
    queries_to_scrape = [task.get('query', '') for task in tasks]
    valid_queries = [q for q in queries_to_scrape if q]
    if not valid_queries:
        print("Нет валидных запросов для обработки.")
        return []

    # 2. Получаем ссылки и заголовки через Selenium
    scraped_results_nested = scrape_duckduckgo_links_titles(valid_queries, num_results_per_query)

    # 3. Обрабатываем результаты: сопоставляем и извлекаем контент
    print("\n--- Извлечение контента для найденных URL ---")
    query_index = 0
    for original_task in tasks:
        task_query = original_task.get('query', '')

        if task_query and task_query in valid_queries:
            if query_index < len(scraped_results_nested):
                links_and_titles = scraped_results_nested[query_index]

                if links_and_titles:
                    print(f"\n  Обработка URL для запроса: '{task_query}' ({original_task.get('query_id')})")
                    for link_data in links_and_titles:
                        url = link_data.get('url')
                        title = link_data.get('title')

                        if not url: continue # Пропускаем, если URL пустой

                        # --- Вызываем функцию извлечения текста ---
                        content_result = fetch_and_extract_text(url)
                        # -------------------------------------------

                        # Формируем итоговый словарь
                        result_item = {
                            "query": task_query,
                            "plan_item": original_task.get('plan_item'),
                            "plan_item_id": original_task.get('plan_item_id'),
                            "query_id": original_task.get('query_id'),
                            "url": url,
                            "title": title,
                            "text": content_result.get('text'),
                            "extraction_method": "requests+bs4" if content_result.get('status') == 'success' else None,
                            "content_length": content_result.get('content_length', 0),
                            "status": content_result.get('status'), # Статус из функции извлечения
                            # Можно добавить поле для ошибки, если нужно
                            # "error_message": content_result.get('error_message')
                        }
                        final_output.append(result_item)

                        # Небольшая пауза между запросами к сайтам
                        time.sleep(random.uniform(0.1, 0.3))

                else:
                     print(f"  Для запроса '{task_query}' ссылки не найдены скрейпером.")
                     # Можно добавить запись, что поиск не дал URL
                     # error_item = { ... "status": "search_failed", "text": None ...}
                     # final_output.append(error_item)

                query_index += 1
            else:
                 print(f"  Предупреждение: Не найдены результаты для запроса '{task_query}' в данных скрейпера.")

        else:
             print(f"\n  Пропуск задачи с невалидным/пустым запросом: {original_task.get('query_id', 'N/A')}")
             # Можно добавить запись об ошибке из-за пустого запроса
             # error_item = { ... "query": task_query, "status": "invalid_query", "text": None ...}
             # final_output.append(error_item)


    print("\nОбработка всех задач завершена.")
    return final_output


# --- Входные данные (те же) ---
test_tasks = [
    {'query': "применение трансформеров в NLP", 'plan_item': "Обзор трансформеров", 'plan_item_id': "plan_0", 'query_id': "q_0_0"},
    {'query': "React component lifecycle hooks", 'plan_item': "React Lifecycle", 'plan_item_id': "plan_1", 'query_id': "q_1_0"},
    {'query': "python dynamic content loading example", 'plan_item': "Dynamic Content", 'plan_item_id': "plan_4", 'query_id': "q_4_0"},
    {'query': "методы кластеризации данных", 'plan_item': "Кластеризация", 'plan_item_id': "plan_2", 'query_id': "q_2_0"},
    {'query': "fastapi background tasks tutorial", 'plan_item': "FastAPI Tasks", 'plan_item_id': "plan_5", "query_id": "q_5_0"},
    {'query': "benefits of using rust programming language", 'plan_item': "Rust Intro", 'plan_item_id': "plan_rust", 'query_id': "q_rust_0"},
    {'query': "how does quantum computing work simple explanation", 'plan_item': "Quantum Simple", 'plan_item_id': "plan_quantum", 'query_id': "q_quantum_0"},
    {'query': "лучшие практики ansible", 'plan_item': "Ansible Best Practices", 'plan_item_id': "plan_ansible", 'query_id': "q_ansible_0"},
    {'query': "несуществующая чепуха абракадабра xyzzy фываолдж", 'plan_item': "Тест ошибки поиска", 'plan_item_id': "plan_3", 'query_id': "q_3_0"},
    {'query': "", 'plan_item': "Пустой запрос", 'plan_item_id': "plan_empty", 'query_id': "q_empty_0"}
]

# --- Запуск обработки и вывод результата ---
if __name__ == "__main__":
    final_results_with_content = process_search_tasks_with_content_extraction(
        test_tasks,
        num_results_per_query=NUM_RESULTS_PER_QUERY
    )

    print("\n" + "=" * 30)
    print("--- Итоговые результаты (с текстом) ---")
    print("=" * 30)
    print(json.dumps(final_results_with_content, indent=2, ensure_ascii=False))

    # Сохранение в файл
    output_filename = "processed_search_results_with_content.json"
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(final_results_with_content, f, ensure_ascii=False, indent=2)
        print(f"\nРезультаты также сохранены в файл: {output_filename}")
    except Exception as e_save:
        print(f"\nНе удалось сохранить результаты в JSON: {e_save}")