import requests
import time
import io  # Для работы с байтами PDF в памяти
from typing import List, Optional, Dict, Any
import random

# --- Установите эту библиотеку ---
# pip install pymupdf
try:
    import fitz  # PyMuPDF
    PDF_PARSER_AVAILABLE = True
except ImportError:
    print("Ошибка: Библиотека PyMuPDF не найдена.")
    print("Пожалуйста, установите ее: pip install pymupdf")
    PDF_PARSER_AVAILABLE = False

# --- Настройки ---
SEMANTIC_SCHOLAR_API_URL = "https://api.semanticscholar.org/graph/v1"
# Укажите ваш API ключ Semantic Scholar здесь для повышения лимитов и надежности
# Получить ключ можно по ссылке в документации: https://www.semanticscholar.org/product/api
# ВАЖНО: Не храните ключ прямо в коде в продакшене! Используйте переменные окружения или другие безопасные методы.
SEMANTIC_SCHOLAR_API_KEY: Optional[str] = None # <--- ЗАМЕНИТЕ НА ВАШ КЛЮЧ или оставьте None

REQUESTS_TIMEOUT = 20  # Таймаут для скачивания PDF (может быть большим)
API_REQUEST_TIMEOUT = 10 # Таймаут для запросов к API
HEADERS = {"Accept": "application/json"}
if SEMANTIC_SCHOLAR_API_KEY:
    HEADERS["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

# Задержка между запросами к API Semantic Scholar (секунды)
# 1.1 секунды рекомендуется при использовании API ключа (лимит 1 RPS)
API_DELAY = 1.1 if SEMANTIC_SCHOLAR_API_KEY else 0.5 # Без ключа можно чуть быстрее, но осторожно


def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> Optional[str]:
    """
    Извлекает текст из содержимого PDF файла (в виде байтов) с помощью PyMuPDF.

    Args:
        pdf_bytes: Содержимое PDF файла в виде байтов.

    Returns:
        Извлеченный текст как одна строка или None в случае ошибки.
    """
    if not PDF_PARSER_AVAILABLE:
        return None
    try:
        # Открываем PDF из байтового потока
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        full_text = ""
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            full_text += page.get_text("text") # Извлекаем только текст
            full_text += "\n" # Добавляем перенос строки между страницами (опционально)

        doc.close()
        # Очистка лишних пробелов и переносов строк, оставленных парсером
        cleaned_text = ' '.join(full_text.split())
        return cleaned_text
    except Exception as e:
        print(f"    [Ошибка PyMuPDF] Не удалось извлечь текст из PDF: {e}")
        return None

def fetch_pdf_content(pdf_url: str) -> Optional[bytes]:
    """
    Скачивает содержимое PDF по URL.

    Args:
        pdf_url: URL для скачивания PDF.

    Returns:
        Содержимое PDF в виде байтов или None в случае ошибки.
    """
    print(f"    Попытка скачать PDF с: {pdf_url[:100]}...") # Обрезаем для лога
    try:
        # Используем стандартные заголовки, имитирующие браузер, для скачивания
        pdf_headers = {
             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }
        response = requests.get(pdf_url, timeout=REQUESTS_TIMEOUT, headers=pdf_headers, stream=True) # stream=True может быть полезен для больших файлов
        response.raise_for_status() # Проверка на HTTP ошибки

        # Дополнительная проверка Content-Type, если возможно
        content_type = response.headers.get('content-type', '').lower()
        if 'pdf' not in content_type:
             print(f"    [Предупреждение] Content-Type не PDF ({content_type}), но все равно попробуем скачать.")
             # Можно добавить более строгую проверку и вернуть None, если не PDF

        pdf_bytes = response.content
        print(f"    PDF успешно скачан ({len(pdf_bytes) / 1024:.1f} KB).")
        return pdf_bytes
    except requests.exceptions.Timeout:
        print(f"    [Ошибка сети] Таймаут при скачивании PDF с {pdf_url}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    [Ошибка сети] Не удалось скачать PDF с {pdf_url}: {e}")
        return None
    except Exception as e:
        print(f"    [Ошибка] Непредвиденная ошибка при скачивании PDF {pdf_url}: {e}")
        return None


def get_pdf_texts_for_topic(
    topic: str,
    max_papers_to_process: int = 5,
    required_fields: List[str] = ["paperId", "title", "abstract", "year", "isOpenAccess", "openAccessPdf", "url"]
) -> List[Dict[str, Any]]:
    """
    Ищет статьи по теме в Semantic Scholar, пытается скачать и извлечь
    текст из PDF файлов, доступных в открытом доступе.

    Args:
        topic: Тема для поиска статей (ключевые слова).
        max_papers_to_process: Максимальное количество найденных статей для попытки обработки PDF.
        required_fields: Поля, запрашиваемые у API Semantic Scholar для каждой статьи.

    Returns:
        Список словарей. Каждый словарь содержит информацию о статье
        и поле 'extracted_pdf_text' с извлеченным текстом (если успешно) или None.
    """
    if not PDF_PARSER_AVAILABLE:
        print("Извлечение текста из PDF невозможно без установленной библиотеки PyMuPDF.")
        return []

    print(f"\n--- Поиск статей по теме: '{topic}' ---")
    search_url = f"{SEMANTIC_SCHOLAR_API_URL}/paper/search"
    query_params = {
        'query': topic,
        'limit': max_papers_to_process, # Запросим столько, сколько хотим обработать
        'fields': ",".join(required_fields)
    }

    processed_results: List[Dict[str, Any]] = []

    try:
        print(f"Отправка запроса к API: {search_url} с параметрами {query_params}")
        response = requests.get(search_url, params=query_params, headers=HEADERS, timeout=API_REQUEST_TIMEOUT)

        # --- Важно: Задержка ПОСЛЕ запроса к API ---
        print(f"Пауза {API_DELAY:.1f} сек после запроса к API...")
        time.sleep(API_DELAY)
        # --------------------------------------------

        response.raise_for_status() # Проверка на ошибки API (4xx, 5xx)

        search_data = response.json()
        papers_found = search_data.get('data', [])
        total_available = search_data.get('total', 0)

        print(f"API Найдено статей (всего): {total_available}")
        print(f"Будет обработано до {len(papers_found)} статей.")

        if not papers_found:
            print("Статьи по данной теме не найдены API Semantic Scholar.")
            return []

        for i, paper_info in enumerate(papers_found):
            print(f"\n--- Обработка статьи {i+1}/{len(papers_found)}: {paper_info.get('title', 'Без названия')} ({paper_info.get('paperId')}) ---")

            # Копируем основную информацию о статье
            result_item = paper_info.copy()
            result_item['extracted_pdf_text'] = None # Инициализируем поле для текста

            is_open_access = paper_info.get('isOpenAccess', False)
            oa_pdf_info = paper_info.get('openAccessPdf')
            pdf_url: Optional[str] = None

            if oa_pdf_info and isinstance(oa_pdf_info, dict):
                 pdf_url = oa_pdf_info.get('url')

            if is_open_access and pdf_url:
                print(f"  Статус: Открытый доступ. Обнаружена ссылка на PDF.")

                # 1. Скачиваем PDF
                pdf_content = fetch_pdf_content(pdf_url)

                # Небольшая пауза между скачиваниями PDF, чтобы не нагружать сервера
                time.sleep(random.uniform(0.3, 0.8)) # Пауза между скачиваниями PDF

                if pdf_content:
                    # 2. Извлекаем текст из скачанного PDF
                    extracted_text = extract_text_from_pdf_bytes(pdf_content)

                    if extracted_text:
                        print(f"  Успешно извлечено {len(extracted_text)} символов текста из PDF.")
                        result_item['extracted_pdf_text'] = extracted_text
                    else:
                        print("  Не удалось извлечь текст из скачанного PDF.")
                        # Текст остается None
                else:
                    print("  Не удалось скачать PDF контент.")
                    # Текст остается None

            elif is_open_access:
                 print("  Статус: Открытый доступ, но ссылка на PDF не найдена в ответе API.")
            else:
                 print("  Статус: Не является открытым доступом (или не определено API). Пропуск извлечения PDF.")

            processed_results.append(result_item)

    except requests.exceptions.Timeout:
         print(f"[Ошибка API] Таймаут при запросе к Semantic Scholar API.")
         return processed_results # Возвращаем то, что успели обработать
    except requests.exceptions.HTTPError as e:
         print(f"[Ошибка API] HTTP ошибка при запросе к Semantic Scholar: {e.response.status_code} {e.response.reason}")
         if e.response.status_code == 401:
              print("  -> Ошибка 401: Проверьте ваш API ключ (SEMANTIC_SCHOLAR_API_KEY).")
         elif e.response.status_code == 403:
              print("  -> Ошибка 403: Доступ запрещен. Возможно, проблема с API ключом или правами.")
         elif e.response.status_code == 429:
              print("  -> Ошибка 429: Превышен лимит запросов к API. Попробуйте увеличить API_DELAY или подождать.")
         # Показать тело ответа, если есть, для диагностики
         try:
              print(f"  Тело ответа API: {e.response.json()}")
         except ValueError: # Если тело не JSON
              print(f"  Тело ответа API: {e.response.text}")
         return processed_results
    except requests.exceptions.RequestException as e:
         print(f"[Ошибка сети] Ошибка при запросе к Semantic Scholar API: {e}")
         return processed_results
    except Exception as e:
         print(f"[Непредвиденная ошибка] Произошла ошибка: {e}")
         import traceback
         traceback.print_exc() # Печать стека вызовов для полной диагностики
         return processed_results

    print("\n--- Обработка завершена ---")
    return processed_results

# --- Пример использования ---
if __name__ == "__main__":
    if not PDF_PARSER_AVAILABLE:
        print("\nВыполнение примера невозможно без PyMuPDF. Установите библиотеку и запустите скрипт снова.")
    else:
        search_topic = "large language models instruction tuning"
        # Сколько статей из результатов поиска мы попытаемся обработать (скачать PDF и извлечь текст)
        papers_to_try = 3 # Уменьшено для быстрого теста

        results = get_pdf_texts_for_topic(search_topic, max_papers_to_process=papers_to_try)

        print("\n\n================ ИТОГОВЫЕ РЕЗУЛЬТАТЫ ================")
        successful_extractions = 0
        for idx, result in enumerate(results):
            print(f"\n--- Результат для статьи {idx+1} ---")
            print(f"  ID: {result.get('paperId')}")
            print(f"  Название: {result.get('title')}")
            print(f"  Год: {result.get('year')}")
            print(f"  Открытый доступ (API): {result.get('isOpenAccess')}")
            pdf_url = result.get('openAccessPdf', {}).get('url') if isinstance(result.get('openAccessPdf'), dict) else None
            print(f"  Ссылка PDF (если была): {pdf_url}")
            print(f"  Ссылка на источник: {result.get('url')}")

            extracted_text = result.get('extracted_pdf_text')
            if extracted_text:
                print(f"  Статус извлечения текста: Успешно ({len(extracted_text)} символов)")
                # Раскомментируйте следующую строку, чтобы увидеть начало текста:
                # print(f"  Начало текста:\n{extracted_text[:500]}...\n")
                successful_extractions += 1
            else:
                print("  Статус извлечения текста: Не удалось или пропущено")

        print("\n================ Сводка ================")
        print(f"Всего обработано статей: {len(results)}")
        print(f"Успешно извлечен текст из PDF: {successful_extractions}")

        # Опционально: сохранить результаты в JSON
        # import json
        # output_filename = f"pdf_texts_{search_topic.replace(' ','_')}.json"
        # try:
        #     with open(output_filename, "w", encoding="utf-8") as f:
        #         json.dump(results, f, ensure_ascii=False, indent=2)
        #     print(f"\nРезультаты сохранены в файл: {output_filename}")
        # except Exception as e:
        #     print(f"\nНе удалось сохранить результаты в JSON: {e}")