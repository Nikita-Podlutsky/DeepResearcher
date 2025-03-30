import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
# from selenium.webdriver.common.keys import Keys # Больше не нужен для ввода
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager # pip install webdriver-manager
from urllib.parse import quote_plus # Для кодирования запроса в URL
from typing import List, Tuple # Для аннотаций типов

# --- Настройки ---
# Укажите количество ссылок, которое нужно получить ДЛЯ КАЖДОГО запроса
NUM_RESULTS_PER_QUERY = 5
# Задержка между запросами к поисковику (в секундах), чтобы не быть заблокированным
DELAY_BETWEEN_QUERIES = random.uniform(0, 0.1) # Случайная задержка

# --- Функция для парсинга ссылок для нескольких запросов ---
def scrape_multiple_queries(queries: List[str], num_results: int = 5) -> List[List[str]]:
    """
    Открывает браузер ОДИН РАЗ, выполняет поиск для КАЖДОГО запроса из списка
    в DuckDuckGo и извлекает первые N ссылок для каждого запроса.

    Args:
        queries (List[str]): Список поисковых запросов.
        num_results (int): Количество ссылок для извлечения для каждого запроса.

    Returns:
        List[List[str]]: Список, где каждый элемент - это список URL-адресов (строк)
                         для соответствующего запроса. Если для запроса ничего не найдено
                         или произошла ошибка, соответствующий элемент будет пустым списком [].
    """
    all_results: List[List[str]] = []
    driver = None # Инициализируем переменную driver

    try:
        print("Инициализация WebDriver для Chrome...")
        # Настройка опций Chrome
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # Раскомментируйте для фонового режима
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # Можно добавить другие опции для маскировки, если нужно
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36") # Обновленный User Agent
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Использование webdriver_manager
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        # Неявное ожидание может замедлять, лучше использовать явные WebDriverWait
        # driver.implicitly_wait(5)

        print(f"Запуск обработки {len(queries)} запросов...")

        # --- Цикл по всем запросам ---
        for i, query in enumerate(queries):
            print("-" * 20)
            print(f"Обработка запроса {i+1}/{len(queries)}: '{query}'")
            current_query_links: List[str] = [] # Список для ссылок текущего запроса

            try:
                # Формируем URL для прямого перехода
                search_url = f"https://duckduckgo.com/?q={quote_plus(query)}&ia=web" # ia=web для веб-результатов
                print(f"Переход на: {search_url}")
                driver.get(search_url)

                # Селектор для ссылок результатов DuckDuckGo (относительно стабилен)
                # Обновленный селектор, часто используемый DDG для органики
                results_selector = (By.CSS_SELECTOR, 'li[data-layout="organic"] h2 a[href]')
                # results_selector_alternative = (By.CSS_SELECTOR, 'li[data-layout="organic"] h2 a[href]') # Предыдущий вариант как запасной

                print("Ожидание загрузки результатов...")
                # Ждем, пока не появится хотя бы один элемент результата по основному селектору
                try:
                    WebDriverWait(driver, 15).until( # Увеличим ожидание
                        EC.presence_of_element_located(results_selector)
                    )
                    print("Результаты загружены (найдены элементы по основному селектору).")
                    selector_to_use = results_selector
                except Exception:
                    # Если основной селектор не сработал, попробуем альтернативный (если он есть)
                    # print(f"Основной селектор не найден, пробую альтернативный...")
                    # try:
                    #      WebDriverWait(driver, 5).until(
                    #          EC.presence_of_element_located(results_selector_alternative)
                    #      )
                    #      print("Результаты загружены (найдены элементы по альтернативному селектору).")
                    #      selector_to_use = results_selector_alternative
                    # except Exception as e_alt:
                         print(f"Ошибка: Результаты поиска DuckDuckGo не найдены или не загрузились для запроса '{query}'. Проверьте селекторы или возможную блокировку.")
                         # Добавляем пустой список для этого запроса и переходим к следующему
                         all_results.append([])
                         # Добавляем задержку перед следующим запросом даже при ошибке
                         if i < len(queries) - 1:
                              print(f"Пауза {DELAY_BETWEEN_QUERIES:.1f} сек перед следующим запросом...")
                              time.sleep(DELAY_BETWEEN_QUERIES)
                         continue # К следующему запросу в цикле

                print(f"Извлечение первых {num_results} ссылок...")
                # Находим все элементы, соответствующие селектору результатов
                result_elements = driver.find_elements(*selector_to_use)

                count = 0
                processed_urls_for_query = set() # Чтобы избежать дублей на одной странице
                for element in result_elements:
                    try:
                        href = element.get_attribute('href')
                        # Дополнительная проверка валидности и исключение ненужных ссылок
                        if href and href.startswith('http') and \
                           'duckduckgo.com' not in href and \
                           href not in processed_urls_for_query:
                             # Здесь можно добавить вызов is_valid_url из предыдущих версий, если нужна более строгая фильтрация
                             # from your_previous_code import is_valid_url
                             # if is_valid_url(href):
                                current_query_links.append(href)
                                processed_urls_for_query.add(href)
                                count += 1
                                if count >= num_results:
                                    break
                    except Exception as e_extract:
                        print(f"  Предупреждение: Не удалось извлечь href из элемента: {e_extract}")
                        continue

                if not current_query_links:
                     print("Не найдено релевантных ссылок по указанным селекторам на этой странице.")

            except Exception as e_query:
                print(f"Произошла ошибка при обработке запроса '{query}': {e_query}")
                # В случае ошибки для конкретного запроса, добавляем пустой список
                current_query_links = []

            # Добавляем список ссылок (возможно, пустой) для текущего запроса в общий результат
            all_results.append(current_query_links)
            print(f"Найдено {len(current_query_links)} ссылок для '{query}'.")

            # --- Задержка между запросами ---
            if i < len(queries) - 1: # Не делаем задержку после последнего запроса
                print(f"Пауза {DELAY_BETWEEN_QUERIES:.1f} сек перед следующим запросом...")
                time.sleep(DELAY_BETWEEN_QUERIES)

    except Exception as e_main:
        print(f"Произошла критическая ошибка: {e_main}")

    finally:
        if driver:
            print("Закрытие браузера...")
            driver.quit()

    return all_results

# --- Основная часть скрипта ---
if __name__ == "__main__":
    # --- Ввод списка запросов ---
    input_queries_str = input("Введите поисковые запросы через точку с запятой (;): ")
    list_of_queries = [q.strip() for q in input_queries_str.split(';') if q.strip()]

    if not list_of_queries:
        print("Список поисковых запросов не может быть пустым.")
    else:
        print(f"\nЗапускаю поиск для {len(list_of_queries)} запросов...")
        # Вызов функции для обработки списка запросов
        list_of_link_lists = scrape_multiple_queries(list_of_queries, NUM_RESULTS_PER_QUERY)

        print("\n" + "=" * 30)
        print("--- Результаты поиска ---")
        print("=" * 30)

        if list_of_link_lists:
            # Вывод результатов в формате: Запрос -> Список ссылок
            for i, query in enumerate(list_of_queries):
                print(f"\nЗапрос: '{query}'")
                if i < len(list_of_link_lists) and list_of_link_lists[i]:
                    links = list_of_link_lists[i]
                    print(f"  Найдено ссылок: {len(links)}")
                    for j, link in enumerate(links):
                        print(f"  {j + 1}. {link}")
                else:
                    print("  Ссылки не найдены или произошла ошибка.")
        else:
            print("\nНе удалось получить результаты ни для одного запроса.")

        # --- Опционально: Сохранение в JSON ---
        try:
            import json
            output_data = {}
            for i, query in enumerate(list_of_queries):
                 output_data[query] = list_of_link_lists[i] if i < len(list_of_link_lists) else []

            filename = "search_results_ddg.json"
            with open(filename, "w", encoding="utf-8") as f:
                 json.dump(output_data, f, ensure_ascii=False, indent=2)
            print(f"\nРезультаты также сохранены в файл: {filename}")
        except Exception as e_save:
            print(f"\nНе удалось сохранить результаты в JSON: {e_save}")