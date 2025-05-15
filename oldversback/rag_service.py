import time
import os
import json
import re
from dotenv import load_dotenv
import ollama
import chromadb # Векторная база данных
from langchain.text_splitter import RecursiveCharacterTextSplitter # Для разбиения на чанки
from duckduckgo_search import DDGS
from scraper import run_complete_scrape
from urllib.parse import urlparse

# (Опционально) Библиотеки для реального парсинга
try:
    import requests
    from bs4 import BeautifulSoup
    WEB_SCRAPING_ENABLED = True
except ImportError:
    print("Warning: 'requests' or 'beautifulsoup4' not installed. Web scraping will be simulated.")
    WEB_SCRAPING_ENABLED = False

# Загружаем переменные окружения
load_dotenv()

# --- Конфигурация (Обновлено) ---
OLLAMA_LLM_MODEL = os.getenv("OLLAMA_LLM_MODEL", "gemma3")
# !!! Используем модель эмбеддингов из примера Ollama
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "mxbai-embed-large") # Убедитесь, что скачали: ollama pull mxbai-embed-large
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434") # Используется ollama library, хост может быть не нужен, но оставим

# --- Настройки RAG (Обновлено) ---
# Имя коллекции в ChromaDB
CHROMA_COLLECTION_NAME = "research_docs" # Как в примере, но можно изменить
# Параметры разбиения текста на чанки
CHUNK_SIZE = 2000
CHUNK_OVERLAP = 500
# Количество релевантных чанков для извлечения
TOP_K_RESULTS = 10 # В примере извлекается 1 результат

# --- Инициализация Клиентов (Обновлено) ---
# Проверка доступности Ollama (не через клиент, а через сам модуль)
try:
    # Пробуем получить список моделей для проверки соединения и наличия моделей
    available_models_info = ollama.list()
    # print(available_models_info)
    available_models = [m['model'] for m in available_models_info['models']]
    print(f"Доступные модели Ollama: {available_models}")
    # У Ollama имена моделей включают тег, например 'llama3:latest'
    llm_model_name_tag = f"{OLLAMA_LLM_MODEL}:latest"
    embed_model_name_tag = f"{OLLAMA_EMBED_MODEL}:latest"

    if llm_model_name_tag not in available_models and not any(m.startswith(OLLAMA_LLM_MODEL + ':') for m in available_models):
         print(f"ПРЕДУПРЕЖДЕНИЕ: Модель LLM '{OLLAMA_LLM_MODEL}' не найдена в Ollama!")
    if embed_model_name_tag not in available_models and not any(m.startswith(OLLAMA_EMBED_MODEL + ':') for m in available_models):
         print(f"ПРЕДУПРЕЖДЕНИЕ: Модель эмбеддингов '{OLLAMA_EMBED_MODEL}' не найдена! RAG не будет работать.")

except Exception as e:
    print(f"!!! Ошибка подключения к Ollama или получения списка моделей: {e}")
    print("!!! Убедитесь, что Ollama запущена.")
    exit()

try:
    # Используем In-Memory клиент ChromaDB, как в примере
    # !!! Данные будут теряться при перезапуске скрипта !!!
    # Если нужна персистентность, используйте:
    # chroma_client = chromadb.PersistentClient(path="./chroma_db_inmem_example")
    chroma_client = chromadb.Client()
    print("Используется In-Memory ChromaDB клиент.")

    # Получаем или создаем коллекцию
    # Если коллекция существует (от предыдущего запуска в той же сессии), удаляем ее для чистоты эксперимента
    try:
        chroma_client.delete_collection(name=CHROMA_COLLECTION_NAME)
        print(f"Существующая In-Memory коллекция '{CHROMA_COLLECTION_NAME}' удалена.")
    except:
        pass # Ошибки не будет, если коллекция не существовала
    collection = chroma_client.create_collection(name=CHROMA_COLLECTION_NAME)
    print(f"In-Memory ChromaDB коллекция '{CHROMA_COLLECTION_NAME}' создана.")
except Exception as e:
    print(f"!!! Ошибка инициализации ChromaDB: {e}")
    exit()

# Инициализация сплиттера текста
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    length_function=len,
)

# --- Вспомогательные функции ---

def call_ollama_generate(prompt: str, system_message: str = None) -> str | None:
    """Выполняет вызов генеративной LLM Ollama (используя ollama.chat)."""
    messages = []
    if system_message:
        messages.append({'role': 'system', 'content': system_message})
    messages.append({'role': 'user', 'content': prompt})

    try:
        print(f"\n--- Запрос к LLM Ollama ({OLLAMA_LLM_MODEL}) ---")
        response = ollama.chat(
            model=OLLAMA_LLM_MODEL,
            messages=messages
        )
        content = response['message']['content']
        print(f"--- Ответ от LLM Ollama ---")
        return content.strip()
    except Exception as e:
        print(f"!!! Ошибка при вызове LLM Ollama ({OLLAMA_LLM_MODEL}): {e}")
        return None

# --- Функции для шагов исследования (адаптированные) ---

def generate_research_plan(topic: str) -> list[str]:
    """Шаг 1: Генерирует план исследования."""
    print(f"\n[Шаг 1] Генерация плана для темы: '{topic}'")
    system = "Ты - помощник по исследованиям. Создай структурированный план для исследования на тему. Выведи только нумерованный список разделов."
    prompt = f"Составь план исследования: \"{topic}\"."
    response = call_ollama_generate(prompt, system_message=system)
    # ... (остальная логика парсинга плана без изменений) ...
    if not response: return []
    plan_items = []
    lines = response.strip().split('\n')
    for line in lines:
        match = re.match(r'^\s*\d+\.\s*(.*)', line)
        if match:
            plan_items.append(match.group(1).strip())
    if not plan_items:
         print("!!! Не удалось извлечь пункты плана. Ответ:", response)
         plan_items = [line.strip() for line in lines if line.strip()]
    print(f"Сгенерированный план: {plan_items}")
    return plan_items


def generate_search_queries(plan_item: str, num_queries: int = 1) -> list[str]: # Уменьшил до 1 для скорости
    """Шаг 2: Генерирует поисковые запросы."""
    print(f"\n[Шаг 2] Генерация поисковых запросов для: '{plan_item}'")
    system = f"Ты - помощник. Сгенерируй {num_queries} поисковых запроса для раздела исследования: '{plan_item}'. Выведи только запросы, каждый на новой строке."
    prompt = f"Запросы для: \"{plan_item}\"."
    response = call_ollama_generate(prompt, system_message=system)
    # ... (остальная логика парсинга запросов без изменений) ...
    if not response: return []
    queries = [q.strip() for q in response.strip().split('\n') if q.strip()]
    print(f"Сгенерированные запросы: {queries}")
    return queries




# --- Функции для RAG (переписаны под пример) ---

def add_text_chunks_to_db(text: str, metadata_base: dict, collection, embed_model: str):
    """Разбивает текст, генерирует эмбеддинги по одному и добавляет в ChromaDB."""
    if not text or not text.strip():
        return 0

    chunks = text_splitter.split_text(text)
    if not chunks:
        return 0

    print(f"  Разбито на {len(chunks)} чанков. Индексация...")
    added_count = 0
    # Индексируем по одному чанку, как в примере
    for i, chunk in enumerate(chunks):
        try:
            # Генерируем эмбеддинг для чанка
            response = ollama.embed(model=embed_model, input=chunk) # Используем prompt вместо input для совместимости
            embedding = response["embedding"]

            # Создаем уникальный ID и метаданные
            chunk_id = f"{metadata_base.get('plan_item_id', 'unknown')}_{metadata_base.get('query_id', 'unknown')}_{i}"
            current_metadata = metadata_base.copy()
            current_metadata["chunk_index"] = i

            # Добавляем в коллекцию
            collection.add(
                ids=[chunk_id],
                embeddings=[embedding], # Embedding должен быть списком векторов
                documents=[chunk],
                metadatas=[current_metadata]
            )
            added_count += 1
            # print(f"    Чанк {i} добавлен.") # Раскомментировать для детального лога
            time.sleep(0.05) # Небольшая пауза
        except Exception as e:
            print(f"!!! Ошибка при индексации чанка {i} для '{metadata_base.get('plan_item', '')}': {e}")

    print(f"  Успешно добавлено {added_count} чанков.")
    return added_count

def retrieve_relevant_document(query_text: str, collection, embed_model: str, k: int = TOP_K_RESULTS) -> str | None:
    """Извлекает k наиболее релевантных ДОКУМЕНТОВ (чанков) из ChromaDB."""
    print(f"\n[Шаг 4 - RAG Retrieval] Поиск релевантных документов для: '{query_text[:100]}...'")
    try:
        # Генерируем эмбеддинг для запроса
        response = ollama.embed(model=embed_model, input=query_text) # Используем prompt вместо input
        query_embedding = response["embedding"]

        # Ищем в коллекции
        results = collection.query(
            query_embeddings=[query_embedding], # Должен быть список эмбеддингов
            n_results=k
            # include=['documents', 'distances'] # Можно включить для отладки
        )

        if results and results.get('documents') and results['documents'][0]:
            retrieved_docs = results['documents'][0] # Список найденных документов
            print(f"  Найдено {len(retrieved_docs)} релевантных документов (чанков).")
            # В примере используется только первый результат (k=1)
            # Если k > 1, можно их объединить:
            # return "\n\n---\n\n".join(retrieved_docs)
            return retrieved_docs[0] # Возвращаем только самый релевантный, как в примере
        else:
            print("  Релевантные документы не найдены.")
            return None

    except Exception as e:
        print(f"!!! Ошибка при поиске в ChromaDB или генерации эмбеддинга запроса: {e}")
        return None

def generate_section_text_rag(plan_item: str, collection, embed_model: str) -> str:
    """Шаг 5 (RAG): Генерирует текст раздела, используя ИЗВЛЕЧЕННЫЙ контекст."""
    print(f"\n[Шаг 5 - RAG Generation] Генерация текста для раздела: '{plan_item}'")

    # 4. RAG Retrieval - получаем один самый релевантный документ/чанк
    retrieved_data = retrieve_relevant_document(plan_item, collection, embed_model, k=TOP_K_RESULTS)

    if not retrieved_data:
        print("  Не удалось получить контекст из БД. Генерация без RAG.")
        # Генерируем ответ без специфического контекста
        prompt = f"Напиши раздел исследования на тему: \"{plan_item}\". Базовые знания по теме."
        system = "Ты - писатель-исследователь. Напиши раздел для отчета на заданную тему."
    else:
        print(f"  Используется извлеченный контекст: {retrieved_data[:150]}...")
        # Формируем промпт точно как в примере
        prompt = f"Using this data: {retrieved_data}. Respond to this prompt: {plan_item}"
        # Системное сообщение можно опустить или адаптировать
        system = "Ты - ИИ ассистент. Используй предоставленные данные ('Using this data: ...') чтобы ответить на запрос ('Respond to this prompt: ...'). Будь краток и по существу."

    # 5. RAG Generation
    response = call_ollama_generate(prompt, system_message=system)
    if not response:
        return f"Не удалось сгенерировать текст для раздела '{plan_item}'."

    return response

# --- Основная функция (адаптирована) ---
def run_research(topic: str) -> str:
    start_time_total = time.time()
    print(f"=== Запуск RAG-исследования (Ollama Example Style) по теме: '{topic}' ===")
    print(f"LLM: {OLLAMA_LLM_MODEL}, Embeddings: {OLLAMA_EMBED_MODEL}, DB: Chroma In-Memory '{CHROMA_COLLECTION_NAME}'")

# 1. Генерируем план
    plan = generate_research_plan(topic)
    if not plan: return "Ошибка: Не удалось создать план."

    # --- Подготовка Задач для Scrapy ---
    print("\n=== Подготовка Задач для Scrapy ===")
    all_search_tasks = []
    for i, item in enumerate(plan):
        print(f"  Генерация запросов для пункта {i+1}: '{item}'")
        # Генерируем 1 запрос на пункт для скорости
        queries = generate_search_queries(item, num_queries=1)
        if queries:
            for q_idx, query in enumerate(queries):
                 all_search_tasks.append({
                     'query': query,
                     'plan_item': item,
                     'plan_item_id': f"plan_{i}",
                     'query_id': f"q_{q_idx}"
                 })
        else:
             print(f"  Не удалось сгенерировать запросы для '{item}'.")
        time.sleep(0.5) # Небольшая пауза между запросами к LLM

    if not all_search_tasks:
        return "Ошибка: Не удалось сформировать ни одной поисковой задачи."

    # --- Фаза Сбора Данных (Единый Запуск Scrapy) ---
    print(f"\n=== Запуск Scrapy для {len(all_search_tasks)} задач ===")
    # Указываем, сколько сайтов парсить ДЛЯ КАЖДОГО запроса
    scraped_results = run_complete_scrape(all_search_tasks, results_per_query=1)

    # --- Фаза Индексации ---
    print(f"\n=== Фаза Индексации {len(scraped_results)} Собранных Текстов ===")
    total_chunks_added = 0
    if not scraped_results:
        print("Scrapy не собрал данных для индексации.")
    else:
        for item_data in scraped_results:
            text_to_index = item_data.get('text')
            if text_to_index:
                # Используем метаданные, собранные Scrapy
                metadata = {
                    "plan_item": item_data.get('plan_item'),
                    "plan_item_id": item_data.get('plan_item_id'),
                    "source_query": item_data.get('query'),
                    "query_id": item_data.get('query_id'),
                    "url": item_data.get('url'),
                    "url_id": urlparse(item_data.get('url','_')).path.replace('/','_') # Пример ID из URL
                }
                print(f"  Индексация контента (~{len(text_to_index)} симв.) с {metadata['url']}")
                chunks_added = add_text_chunks_to_db(text_to_index, metadata, collection, OLLAMA_EMBED_MODEL)
                total_chunks_added += chunks_added
            time.sleep(0.1) # Небольшая пауза между индексациями

    print(f"\n=== Индексация Завершена. Всего добавлено чанков: {total_chunks_added} ===")
    print(f"Текущий размер коллекции '{collection.name}': {collection.count()}")

    # --- Фаза Генерации Отчета с RAG (Без изменений) ---
    print("\n=== Фаза Генерации Отчета с RAG ===")
    final_report_parts = []
    final_report_parts.append(f"# Исследование по теме: {topic}\n")
    for i, item in enumerate(plan):
        print(f"\n--- Генерация раздела {i+1}/{len(plan)}: '{item}' ---")
        final_report_parts.append(f"\n## {i+1}. {item}\n")
        section_text = generate_section_text_rag(item, collection, OLLAMA_EMBED_MODEL) # Эта функция не менялась
        final_report_parts.append(section_text + "\n")
        time.sleep(0.5)

    end_time_total = time.time()
    print(f"\n=== Исследование завершено за {end_time_total - start_time_total:.2f} секунд ===")
    return "".join(final_report_parts)

# Пример использования
if __name__ == '__main__':
    # Убедитесь, что Ollama запущена и модели скачаны:
    # ollama pull llama3
    # ollama pull mxbai-embed-large
    test_topic = "Преимущества и недостатки использования микросервисной архитектуры"
    report = run_research(test_topic)
    print("\n\n--- ИТОГОВЫЙ ОТЧЕТ ---")
    print(report)

    try:
        with open("research_report_ollama_example.md", "w", encoding="utf-8") as f:
            f.write(report)
        print("\nОтчет сохранен в файл research_report_ollama_example.md")
    except IOError as e:
        print(f"\nНе удалось сохранить отчет в файл: {e}")