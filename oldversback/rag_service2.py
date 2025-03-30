import time
import os
import json
import re
from dotenv import load_dotenv
import ollama
import chromadb # Векторная база данных
from langchain.text_splitter import RecursiveCharacterTextSplitter # Для разбиения на чанки
# Убрали DDGS, так как scraper.py должен сам искать
# from duckduckgo_search import DDGS
from scraper7 import process_search_tasks # Предполагаем, что scraper.py это делает
from urllib.parse import urlparse

# (Опционально) Библиотеки для реального парсинга - ОСТАВИМ ДЛЯ scraper.py
try:
    import requests
    from bs4 import BeautifulSoup
    WEB_SCRAPING_ENABLED = True
except ImportError:
    print("Warning: 'requests' or 'beautifulsoup4' not installed. Web scraping might fail.")
    WEB_SCRAPING_ENABLED = False # Зависит от scraper.py

# Загружаем переменные окружения
load_dotenv()

# --- Конфигурация (Обновлено) ---
OLLAMA_LLM_MODEL = os.getenv("OLLAMA_LLM_MODEL", "gemma3:latest") # Попробуем другую модель для разнообразия
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "mxbai-embed-large:latest")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")

# --- Настройки RAG (Обновлено) ---
CHROMA_COLLECTION_NAME = "research_docs_v2" # Новое имя, чтобы не конфликтовать
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 150
TOP_K_RESULTS = 3 # Извлекаем чуть больше контекста, но для генерации раздела используем лучший

# --- Инициализация Клиентов (Обновлено) ---
try:
    available_models_info = ollama.list()
    available_models = [m['model'] for m in available_models_info['models']]
    print(f"Доступные модели Ollama: {available_models}")

    # Проверяем наличие моделей (игнорируем тег :latest для большей гибкости)
    if not any(m==OLLAMA_LLM_MODEL for m in available_models):
         print(f"ПРЕДУПРЕЖДЕНИЕ: Модель LLM '{OLLAMA_LLM_MODEL}' может отсутствовать в Ollama!")
    if not any(m == OLLAMA_EMBED_MODEL for m in available_models):
         print(f"ПРЕДУПРЕЖДЕНИЕ: Модель эмбеддингов '{OLLAMA_EMBED_MODEL}' не найдена! RAG не будет работать.")
         exit() # Эмбеддинги критичны

except Exception as e:
    print(f"!!! Ошибка подключения к Ollama или получения списка моделей: {e}")
    print("!!! Убедитесь, что Ollama запущена (`ollama serve` или приложение).")
    exit()

try:
    # Используем In-Memory клиент ChromaDB
    chroma_client = chromadb.Client()
    print("Используется In-Memory ChromaDB клиент.")
    try:
        chroma_client.delete_collection(name=CHROMA_COLLECTION_NAME)
        print(f"Существующая In-Memory коллекция '{CHROMA_COLLECTION_NAME}' удалена.")
    except:
        pass
    collection = chroma_client.create_collection(name=CHROMA_COLLECTION_NAME)
    print(f"In-Memory ChromaDB коллекция '{CHROMA_COLLECTION_NAME}' создана.")
except Exception as e:
    print(f"!!! Ошибка инициализации ChromaDB: {e}")
    exit()

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
        # print(f"System: {system_message}") # Для отладки
        # print(f"User: {prompt}") # Для отладки
        response = ollama.chat(
            model=OLLAMA_LLM_MODEL,
            messages=messages,
            options={'temperature': 0.5} # Чуть меньше случайности для консистентности
        )
        content = response['message']['content']
        print(f"--- Ответ от LLM Ollama (кратко): {content[:100]}... ---")
        return content.strip()
    except Exception as e:
        print(f"!!! Ошибка при вызове LLM Ollama ({OLLAMA_LLM_MODEL}): {e}")
        return None

# --- Функции для шагов исследования (План и запросы - без существенных изменений) ---

def generate_research_plan(topic: str) -> list[str]:
    """Шаг 1: Генерирует план исследования."""
    print(f"\n[Шаг 1] Генерация плана для темы: '{topic}'")
    system = ("Ты - опытный помощник по исследованиям. Создай детальный, логически структурированный план "
              "для всестороннего исследования заданной темы. План должен включать введение, несколько "
              "основных содержательных разделов, заключение и раздел для списка использованной литературы. "
              "Выведи только нумерованный список разделов плана, каждый пункт на новой строке.")
    prompt = f"Составь подробный план исследования на тему: \"{topic}\"."
    response = call_ollama_generate(prompt, system_message=system)
    if not response: return []
    plan_items = []
    # Более устойчивый парсинг нумерованных списков
    for line in response.strip().split('\n'):
        match = re.match(r'^\s*[\*\-\d]+\.?\s*(.*)', line) # Ищем маркеры списка или цифры с точкой
        if match:
            item = match.group(1).strip()
            if item: # Не добавляем пустые строки
                plan_items.append(item)
    if not plan_items:
         print("!!! Не удалось извлечь пункты плана. Ответ LLM:", response)
         # Пытаемся просто разбить по строкам как запасной вариант
         plan_items = [line.strip() for line in response.strip().split('\n') if line.strip()]

    # Добавляем стандартные разделы, если их нет (эвристика)
    if not any(re.search(r'introduction|введение', item, re.IGNORECASE) for item in plan_items):
        plan_items.insert(0, "Введение")
    if not any(re.search(r'conclusion|summary|заключение|выводы', item, re.IGNORECASE) for item in plan_items):
        plan_items.append("Заключение")
    if not any(re.search(r'references|bibliography|literature|sources|список литературы|источники', item, re.IGNORECASE) for item in plan_items):
        plan_items.append("Список использованной литературы")

    print(f"Сгенерированный и дополненный план: {plan_items}")
    return plan_items


def generate_search_queries(topic:str, plan_item: str, full_plan: list[str], num_queries: int = 1) -> list[str]:
    """Шаг 2: Генерирует поисковые запросы С УЧЕТОМ КОНТЕКСТА ПЛАНА."""
    print(f"\n[Шаг 2] Генерация поисковых запросов для: '{plan_item}'")

    # Пропускаем генерацию запросов для не-содержательных разделов
    if any(keyword in plan_item.lower() for keyword in ["введение", "introduction", "заключение", "conclusion", "summary", "references", "bibliography", "literature", "sources", "список"]):
        print("  Пропуск генерации запросов для структурного раздела.")
        return []

    formatted_plan = "\n".join([f"- {item}" for item in full_plan])
    system = (f"Ты - помощник по исследованиям. Тема исследования: '{topic}'. "
              f"Полный план:\n{formatted_plan}\n\n"
              f"Твоя задача - сгенерировать {num_queries} точных и эффективных поисковых запроса (на русском или английском) "
              f"для сбора информации КОНКРЕТНО по следующему разделу плана: '{plan_item}'. "
              f"Учитывай общий контекст плана. Выведи только сами запросы, каждый на новой строке.")
    prompt = f"Сгенерируй поисковые запросы для раздела: \"{plan_item}\"."
    response = call_ollama_generate(prompt, system_message=system)
    if not response: return []
    queries = [q.strip() for q in response.strip().split('\n') if q.strip()]
    # Убираем нумерацию/маркеры, если LLM их добавила
    queries = [re.sub(r'^\s*[\*\-\d]+\.?\s*', '', q) for q in queries]
    print(f"Сгенерированные запросы: {queries}")
    return queries

# --- Функции для RAG (Обновлены) ---

def add_text_chunks_to_db(text: str, metadata_base: dict, collection, embed_model: str):
    """Разбивает текст, генерирует эмбеддинги и добавляет в ChromaDB."""
    # (Логика разбиения и добавления чанков остается прежней)
    if not text or not text.strip():
        print("  Предупреждение: Попытка индексации пустого текста.")
        return 0
    chunks = text_splitter.split_text(text)
    if not chunks:
        print("  Предупреждение: Текст не удалось разбить на чанки.")
        return 0

    print(f"  Разбито на {len(chunks)} чанков. Индексация...")
    added_count = 0
    for i, chunk in enumerate(chunks):
        try:
            response = ollama.embed(model=embed_model, input=chunk) # Используем input
            embedding = response["embedding"]
            # Генерируем уникальный ID для чанка
            # Используем хэш от URL и индекса чанка для большей уникальности
            base_id = f"{metadata_base.get('url', 'no_url')}_{i}"
            chunk_id = f"chunk_{hash(base_id)}"
            current_metadata = metadata_base.copy()
            current_metadata["chunk_index"] = i
            current_metadata["chunk_text_preview"] = chunk[:100] # Добавим превью для отладки

            collection.add(
                ids=[str(chunk_id)], # ID должен быть строкой
                embeddings=[embedding],
                documents=[chunk],
                metadatas=[current_metadata]
            )
            added_count += 1
            time.sleep(0.05) # Небольшая пауза
        except Exception as e:
            print(f"!!! Ошибка при индексации чанка {i} для '{metadata_base.get('url', '')}': {e}")
            # Попробуем пропустить проблемный чанк
            continue

    print(f"  Успешно добавлено {added_count} из {len(chunks)} чанков.")
    return added_count


def retrieve_relevant_document(query_text: str, collection, embed_model: str, k: int = TOP_K_RESULTS) -> tuple[str | None, dict | None]:
    """
    Извлекает k наиболее релевантных чанков из ChromaDB.
    Возвращает текст ЛУЧШЕГО чанка и его метаданные.
    """
    print(f"\n[RAG Retrieval] Поиск релевантных документов для запроса (раздела): '{query_text[:100]}...'")
    try:
        response = ollama.embed(model=embed_model, input=query_text) # Используем input
        query_embedding = response["embedding"]

        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=k,
            include=['documents', 'metadatas', 'distances'] # Включаем метаданные!
        )

        if results and results.get('ids') and results['ids'][0]:
            # Сортируем по расстоянию (если есть, иначе просто берем первый)
            best_index = 0
            if results.get('distances') and results['distances'][0]:
                 # `distances` может быть None, если не поддерживается или не настроено
                 # Chroma возвращает расстояния, меньшее значение = лучшее совпадение
                 sorted_results = sorted(zip(results['ids'][0],
                                             results['documents'][0],
                                             results['metadatas'][0],
                                             results['distances'][0]), key=lambda x: x[3])
                 best_id, best_doc, best_metadata, best_distance = sorted_results[0]
                 print(f"  Найдено {len(results['ids'][0])} релевантных чанков. Лучший ID: {best_id}, Дистанция: {best_distance:.4f}")
                 return best_doc, best_metadata
            else:
                # Если расстояний нет, просто берем первый результат
                best_doc = results['documents'][0][0]
                best_metadata = results['metadatas'][0][0]
                print(f"  Найдено {len(results['ids'][0])} релевантных чанков (без сортировки по дистанции). Используется первый.")
                return best_doc, best_metadata
        else:
            print("  Релевантные документы не найдены.")
            return None, None

    except Exception as e:
        print(f"!!! Ошибка при поиске в ChromaDB или генерации эмбеддинга запроса: {e}")
        return None, None


# --- НОВАЯ, УЛУЧШЕННАЯ ФУНКЦИЯ ГЕНЕРАЦИИ РАЗДЕЛА ---
def generate_section_text_smart(
    topic: str,
    full_plan: list[str],
    current_plan_item: str,
    item_index: int,
    collection,
    embed_model: str,
    used_sources: list[dict] # Список для сбора использованных источников
    ) -> str:
    """
    Генерирует текст для раздела с учетом КОНТЕКСТА ВСЕГО ПЛАНА и типа раздела.
    Обрабатывает Введение, Заключение и Список литературы особым образом.
    Собирает использованные источники.
    """
    print(f"\n[Шаг 5 - Smart Generation] Генерация текста для раздела {item_index+1}: '{current_plan_item}'")

    # Определяем тип раздела по ключевым словам (регистронезависимо)
    item_lower = current_plan_item.lower()
    is_introduction = any(keyword in item_lower for keyword in ["введение", "introduction"]) and item_index == 0 # Доп. проверка на индекс
    is_conclusion = any(keyword in item_lower for keyword in ["заключение", "conclusion", "summary", "выводы"]) and item_index == len(full_plan) - 2 # Проверка на предпоследний индекс (перед References)
    is_references = any(keyword in item_lower for keyword in ["references", "bibliography", "literature", "sources", "список литературы", "источники"]) and item_index == len(full_plan) - 1 # Проверка на последний индекс

    formatted_plan = "\n".join([f"{idx+1}. {item}" for idx, item in enumerate(full_plan)])
    base_system_message = f"Ты - ИИ-ассистент, помогающий писать структурированный отчет по исследованию.\nТема исследования: '{topic}'.\nПолный план отчета:\n{formatted_plan}\n"

    # --- Особая обработка структурных разделов ---

    if is_introduction:
        print("  Тип раздела: Введение. Генерация без RAG.")
        system = base_system_message + "\nТвоя задача: Напиши ВВЕДЕНИЕ для этого отчета. Кратко опиши тему и представь структуру отчета согласно плану."
        prompt = f"Напиши введение для отчета на тему '{topic}'."
        response = call_ollama_generate(prompt, system_message=system)
        return response if response else f"Не удалось сгенерировать введение для '{current_plan_item}'."

    elif is_conclusion:
        print("  Тип раздела: Заключение. Генерация без RAG (на основе плана).")
        system = base_system_message + "\nТвоя задача: Напиши ЗАКЛЮЧЕНИЕ для этого отчета. Подведи итоги исследования, основываясь на РАЗДЕЛАХ ПЛАНА. Сделай краткие выводы по теме."
        prompt = f"Напиши заключение для отчета на тему '{topic}', суммируя ключевые аспекты, затронутые в плане."
        response = call_ollama_generate(prompt, system_message=system)
        return response if response else f"Не удалось сгенерировать заключение для '{current_plan_item}'."

    elif is_references:
        print("  Тип раздела: Список литературы. Форматирование собранных источников.")
        if not used_sources:
            return "В процессе исследования не было зафиксировано использованных внешних источников."

        formatted_sources = []
        # Используем set для хранения уникальных URL
        unique_urls = set()
        for source in used_sources:
             url = source.get('url')
             if url and url not in unique_urls:
                 # Пытаемся получить название из метаданных, иначе используем домен
                 title = source.get('title') # Предполагаем, что scraper мог добавить title
                 if not title:
                     try:
                         title = urlparse(url).netloc # Берем домен как запасной вариант
                     except:
                         title = "Источник" # Совсем запасной вариант
                 # Формируем строку
                 formatted_sources.append(f"- [{title}]({url}) (Источник для раздела: '{source.get('plan_item', 'Неизвестно')}')")
                 unique_urls.add(url)

        if not formatted_sources:
             return "Не найдено уникальных URL среди зафиксированных источников."

        return "Список использованной литературы:\n\n" + "\n".join(formatted_sources)

    # --- Обработка стандартных содержательных разделов с RAG ---
    else:
        print(f"  Тип раздела: Содержательный. Попытка RAG для '{current_plan_item}'.")
        # 1. RAG Retrieval - получаем лучший чанк и его метаданные
        retrieved_text, retrieved_metadata = retrieve_relevant_document(current_plan_item, collection, embed_model, k=TOP_K_RESULTS)

        if not retrieved_text or not retrieved_metadata:
            print("  Не удалось получить релевантный контекст из БД. Генерация без RAG, но с учетом плана.")
            system = base_system_message + f"\nТвоя задача: Напиши текст для раздела плана '{current_plan_item}'. Используй свои общие знания по теме '{topic}' и учитывай место этого раздела в общем плане."
            prompt = f"Напиши содержательный текст для раздела отчета: \"{current_plan_item}\"."
            response = call_ollama_generate(prompt, system_message=system)
            return response if response else f"Не удалось сгенерировать текст для раздела '{current_plan_item}' (без RAG)."
        else:
            print(f"  Используется извлеченный контекст из источника: {retrieved_metadata.get('url', 'URL не найден')}")
            print(f"  Контекст (начало): {retrieved_text[:150]}...")

            # 2. Добавляем источник в список использованных (если есть URL и он уникален)
            source_url = retrieved_metadata.get('url')
            if source_url and not any(s.get('url') == source_url for s in used_sources):
                source_info = {
                    'url': source_url,
                    'title': retrieved_metadata.get('title'), # Scraper должен бы добавить title
                    'plan_item': retrieved_metadata.get('plan_item', current_plan_item), # Связываем с текущим разделом
                    # Можно добавить и другие метаданные при необходимости
                }
                used_sources.append(source_info)
                print(f"  Источник {source_url} добавлен в список использованных.")

            # 3. RAG Generation с контекстом и планом
            system = base_system_message + (f"\nТвоя задача: Напиши текст для раздела плана '{current_plan_item}'. "
                                            f"Используй СЛЕДУЮЩИЙ КОНТЕКСТ, извлеченный из внешнего источника, как ОСНОВУ для ответа. "
                                            f"Адаптируй информацию под структуру отчета и тему '{topic}'. Будь объективен и информативен.\n\n"
                                            f"ИЗВЛЕЧЕННЫЙ КОНТЕКСТ:\n{retrieved_text}")

            prompt = f"Напиши текст для раздела отчета \"{current_plan_item}\", используя предоставленный контекст."
            response = call_ollama_generate(prompt, system_message=system)

            if not response:
                return f"Не удалось сгенерировать текст для раздела '{current_plan_item}' (с RAG), несмотря на наличие контекста."

            # Можно добавить пост-обработку ответа, например, убрать фразы вроде "На основе предоставленного контекста..."
            response = re.sub(r"^(На основе|Согласно|Исходя из) предоставленн(ого|ых) данн(ых|ым|ого контекста)[,:]?\s*", "", response, flags=re.IGNORECASE | re.MULTILINE).strip()

            return response


# --- Основная функция (Адаптирована) ---
def run_research(topic: str) -> str:
    start_time_total = time.time()
    print(f"=== Запуск УЛУЧШЕННОГО RAG-исследования по теме: '{topic}' ===")
    print(f"LLM: {OLLAMA_LLM_MODEL}, Embeddings: {OLLAMA_EMBED_MODEL}, DB: Chroma In-Memory '{CHROMA_COLLECTION_NAME}'")

    # 1. Генерируем план (уже с попыткой добавить структурные разделы)
    plan = generate_research_plan(topic)
    if not plan: return "Ошибка: Не удалось создать план."

    # --- Подготовка Задач для Scrapy ---
    print("\n=== Подготовка Задач для Scrapy ===")
    all_search_tasks = []
    for i, item in enumerate(plan):
        # Генерируем запросы только для содержательных разделов
        # Передаем topic и full_plan для лучшего качества запросов
        queries = generate_search_queries(topic, item, plan, num_queries=1) # 1 запрос для скорости
        if queries:
            for q_idx, query in enumerate(queries):
                 all_search_tasks.append({
                     'query': query,
                     'plan_item': item, # Сохраняем связь с пунктом плана
                     'plan_item_id': f"plan_{i}", # Уникальный ID для пункта плана
                     'query_id': f"q_{i}_{q_idx}" # Уникальный ID для запроса
                 })
        # Пауза не нужна, т.к. запросы к LLM внутри generate_search_queries
        # time.sleep(0.5) # Если LLM работает локально и быстро, можно убрать

    if not all_search_tasks:
        print("Предупреждение: Не удалось сформировать ни одной поисковой задачи (возможно, план состоит только из структурных разделов?).")
        # Продолжаем без скрапинга, чтобы сгенерировать хотя бы структуру отчета

    # --- Фаза Сбора Данных (Единый Запуск Scrapy) ---
    scraped_results = []
    if all_search_tasks:
        print(f"\n=== Запуск Scrapy для {len(all_search_tasks)} задач ===")
        # Указываем, сколько сайтов парсить ДЛЯ КАЖДОГО запроса (1 для скорости)
        # scraper.py должен уметь обрабатывать список задач
        scraped_results = process_search_tasks(all_search_tasks, num_results_per_query=2, max_workers=4)
    else:
        print("\n=== Пропуск фазы сбора данных (нет поисковых задач) ===")


    # --- Фаза Индексации ---
    print(f"\n=== Фаза Индексации {len(scraped_results)} Собранных Текстов ===")
    total_chunks_added = 0
    indexed_urls = set() # Чтобы не индексировать один и тот же URL дважды, если он попался по разным запросам
    if not scraped_results:
        print("Scrapy не собрал данных для индексации.")
    else:
        for item_data in scraped_results:
            text_to_index = item_data.get('text')
            source_url = item_data.get('url')

            if text_to_index and source_url and source_url not in indexed_urls:
                # Используем метаданные, собранные Scrapy + добавляем свои
                metadata = {
                    "plan_item": item_data.get('plan_item', 'Неизвестно'), # От какого пункта плана пришел
                    "source_query": item_data.get('query', 'Неизвестно'), # По какому запросу найдено
                    "url": source_url,
                    "title": item_data.get('title'), # Заголовок страницы, если scraper его извлек
                    # Можно добавить другие поля, если scraper их возвращает
                }
                print(f"\n  Индексация контента (~{len(text_to_index)} симв.) с {metadata['url']}")
                print(f"  Связано с пунктом плана: '{metadata['plan_item']}'")
                chunks_added = add_text_chunks_to_db(text_to_index, metadata, collection, OLLAMA_EMBED_MODEL)
                total_chunks_added += chunks_added
                indexed_urls.add(source_url) # Отмечаем URL как проиндексированный
                # time.sleep(0.1) # Пауза между индексацией разных документов
            elif not text_to_index:
                 print(f"  Пропуск индексации для {source_url}: нет текста.")
            elif source_url in indexed_urls:
                 print(f"  Пропуск индексации для {source_url}: уже проиндексировано.")


    print(f"\n=== Индексация Завершена. Всего добавлено чанков: {total_chunks_added} ===")
    print(f"Текущий размер коллекции '{collection.name}': {collection.count()}")
    if collection.count() == 0 and len(all_search_tasks) > 0:
         print("ПРЕДУПРЕЖДЕНИЕ: Поисковые задачи были, но ни одного чанка не добавлено в базу. Проверьте работу скрапера и индексатора.")

    # --- Фаза Генерации Отчета с УЛУЧШЕННОЙ ЛОГИКОЙ ---
    print("\n=== Фаза Генерации Отчета (Smart Generation) ===")
    final_report_parts = []
    used_sources_list = [] # Инициализируем список для сбора источников

    final_report_parts.append(f"# Исследование по теме: {topic}\n")

    for i, item in enumerate(plan):
        # Используем новую функцию генерации
        section_text = generate_section_text_smart(
            topic,
            plan,
            item,
            i,
            collection,
            OLLAMA_EMBED_MODEL,
            used_sources_list # Передаем список для модификации
        )
        final_report_parts.append(f"\n## {i+1}. {item}\n")
        final_report_parts.append(section_text + "\n")
        time.sleep(0.5) # Пауза между генерацией разделов

    end_time_total = time.time()
    print(f"\n=== Исследование завершено за {end_time_total - start_time_total:.2f} секунд ===")

    # Очистка ChromaDB после завершения (т.к. она In-Memory)
    try:
        chroma_client.delete_collection(name=CHROMA_COLLECTION_NAME)
        print(f"In-Memory коллекция '{CHROMA_COLLECTION_NAME}' очищена.")
    except Exception as e:
        print(f"Не удалось очистить In-Memory коллекцию: {e}")


    return "".join(final_report_parts)

# Пример использования
if __name__ == '__main__':
    # Убедитесь, что Ollama запущена и модели скачаны:

    test_topic = "Влияние изменения климата на биоразнообразие Арктики"
    report = run_research(test_topic)
    print("\n\n--- ИТОГОВЫЙ ОТЧЕТ ---")
    print(report)

    # Сохранение отчета
    report_filename = f"research_report_{re.sub('[^A-Za-z0-9]+', '_', test_topic)[:50]}.md"
    try:
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\nОтчет сохранен в файл: {report_filename}")
    except IOError as e:
        print(f"\nНе удалось сохранить отчет в файл: {e}")