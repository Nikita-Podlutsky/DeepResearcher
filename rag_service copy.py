# -*- coding: utf-8 -*-
import time
import os
import json
import re
import logging # Добавляем для более гибкого логирования
from dotenv import load_dotenv
import ollama
import chromadb # Векторная база данных
from langchain.text_splitter import RecursiveCharacterTextSplitter # Для разбиения на чанки
from scrapertest import process_tasks_combined
from urllib.parse import urlparse

# (Опционально) Библиотеки для реального парсинга - ОСТАВИМ ДЛЯ scraper7.py
# Импорт requests и BeautifulSoup больше не нужен здесь, т.к. парсинг в scraper7.py

# Загружаем переменные окружения (.env файл)
load_dotenv()

# --- Конфигурация ---
OLLAMA_LLM_MODEL = os.getenv("OLLAMA_LLM_MODEL", "qwen3:0.6b-fp16") # Модель для генерации текста
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "mxbai-embed-large:latest") # Модель для эмбеддингов
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434") # Адрес сервера Ollama

# --- Настройки RAG (Retrieval-Augmented Generation) ---
CHROMA_COLLECTION_NAME = "research_docs_v3" # Новое имя, чтобы избежать конфликтов
CHUNK_SIZE = 10000 # Размер чанка (фрагмента текста)
CHUNK_OVERLAP = 1500 # Перекрытие между чанками
TOP_K_RESULTS = 10 # Количество извлекаемых релевантных чанков из БД

# --- Настройка логирования ---
# Устанавливаем базовый уровень логирования. INFO - будет показывать информационные сообщения.
# Можно изменить на logging.DEBUG для более детальной отладки.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Инициализация Клиентов ---
try:
    # Получаем список доступных моделей в Ollama
    logging.info("Получение списка доступных моделей Ollama...")
    available_models_info = ollama.list()
    available_models = [m['model'] for m in available_models_info['models']]
    logging.info(f"Доступные модели Ollama: {available_models}")

    # Проверяем наличие необходимых моделей (без учета тега :latest для гибкости)
    llm_model_base = OLLAMA_LLM_MODEL.split(':')[0]
    embed_model_base = OLLAMA_EMBED_MODEL.split(':')[0]

    if not any(m.startswith(llm_model_base) for m in available_models):
         logging.warning(f"ПРЕДУПРЕЖДЕНИЕ: Модель LLM '{OLLAMA_LLM_MODEL}' может отсутствовать или быть недоступна в Ollama!")
    if not any(m.startswith(embed_model_base) for m in available_models):
         logging.error(f"КРИТИЧЕСКАЯ ОШИБКА: Модель эмбеддингов '{OLLAMA_EMBED_MODEL}' не найдена в Ollama! RAG не будет работать.")
         exit() # Эмбеддинги критичны для RAG

except Exception as e:
    logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА при подключении к Ollama или получении списка моделей: {e}")
    logging.error("!!! Убедитесь, что сервер Ollama запущен (`ollama serve` или через приложение).")
    exit()

# Инициализация клиента векторной базы данных ChromaDB
try:
    # Используем In-Memory клиент (данные хранятся только в оперативной памяти)
    # Для постоянного хранения можно использовать PersistentClient:
    # chroma_client = chromadb.PersistentClient(path="./chroma_db")
    chroma_client = chromadb.Client()
    logging.info("Используется In-Memory клиент ChromaDB.")

    # Попытка удалить коллекцию, если она существует (для чистого старта)
    try:
        logging.info(f"Попытка удалить существующую коллекцию '{CHROMA_COLLECTION_NAME}'...")
        chroma_client.delete_collection(name=CHROMA_COLLECTION_NAME)
        logging.info(f"Существующая In-Memory коллекция '{CHROMA_COLLECTION_NAME}' успешно удалена.")
    except Exception as e:
        logging.info(f"Не удалось удалить коллекцию '{CHROMA_COLLECTION_NAME}' (возможно, ее и не было): {e}")
        pass # Игнорируем ошибку, если коллекции нет

    # Создаем новую коллекцию
    collection = chroma_client.create_collection(name=CHROMA_COLLECTION_NAME)
    logging.info(f"In-Memory ChromaDB коллекция '{CHROMA_COLLECTION_NAME}' успешно создана.")

except Exception as e:
    logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА при инициализации ChromaDB: {e}")
    exit()

# Инициализация разделителя текста
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    length_function=len,
)

# --- Вспомогательные функции ---

def call_ollama_generate(prompt: str, system_message: str = None) -> str | None:
    """
    Выполняет вызов генеративной LLM Ollama (используя ollama.chat).
    Возвращает сгенерированный текст или None в случае ошибки.
    """
    messages = []
    if system_message:
        messages.append({'role': 'system', 'content': system_message})
    messages.append({'role': 'user', 'content': prompt})

    try:
        logging.info(f"--- Запрос к LLM Ollama ({OLLAMA_LLM_MODEL}) ---")
        # Для детальной отладки можно раскомментировать следующие строки:
        # logging.debug(f"System Prompt: {system_message}")
        # logging.debug(f"User Prompt: {prompt}")

        start_time = time.time()
        response = ollama.chat(
            model=OLLAMA_LLM_MODEL,
            messages=messages,
            options={'temperature': 0.5} # Уменьшаем температуру для более предсказуемых ответов
        )
        end_time = time.time()
        logging.info(f"--- Ответ от LLM Ollama получен за {end_time - start_time:.2f} сек. ---")

        # Проверяем структуру ответа
        if 'message' in response and 'content' in response['message']:
            content = response['message']['content']
            temp = content[:150].replace('\n', ' ')
            logging.info(f"Ответ LLM (начало): {temp}...") # Логируем начало ответа
            return content.strip()
        else:
            logging.error(f"!!! Неожиданный формат ответа от LLM Ollama: {response}")
            return None

    except Exception as e:
        logging.error(f"!!! ОШИБКА при вызове LLM Ollama ({OLLAMA_LLM_MODEL}): {e}")
        return None

def add_text_chunks_to_db(text: str, metadata_base: dict, collection, embed_model: str):
    """
    Разбивает текст на чанки, генерирует эмбеддинги для каждого чанка
    и добавляет их вместе с метаданными в указанную коллекцию ChromaDB.
    Возвращает количество успешно добавленных чанков.
    """
    if not text or not text.strip():
        logging.warning("  Предупреждение: Попытка индексации пустого текста.")
        return 0

    # Разбиваем текст на чанки
    try:
        chunks = text_splitter.split_text(text)
    except Exception as e:
        logging.error(f"  Ошибка при разбиении текста на чанки: {e}. Текст (начало): {text[:100]}")
        return 0

    if not chunks:
        logging.warning("  Предупреждение: Текст не удалось разбить на чанки (возможно, слишком короткий).")
        return 0

    logging.info(f"  Текст разбит на {len(chunks)} чанков. Начинаем индексацию...")
    added_count = 0
    for i, chunk in enumerate(chunks):
        if not chunk or not chunk.strip(): # Пропускаем пустые чанки, если они образовались
            continue
        try:
            # 1. Генерируем эмбеддинг для чанка
            embed_response = ollama.embed(model=embed_model, input=chunk) # Используем 'prompt' для mxbai

            # === ВАЖНАЯ ПРОВЕРКА ===
            # if "embedding" not in embed_response:
            #     logging.error(f"!!! ОШИБКА: Ключ 'embedding' отсутствует в ответе ollama.embed для чанка {i}. "
            #                   f"Источник: {metadata_base.get('url', 'N/A')}. Ответ Ollama: {embed_response}")
            #     continue # Пропускаем этот чанк

            embedding = embed_response["embeddings"][0]

            # 2. Генерируем уникальный ID для чанка
            # Используем URL и индекс чанка для большей уникальности ID
            base_id_str = f"{metadata_base.get('url', 'no_url')}_{i}"
            # Используем стандартный hash() Python для ID, преобразуя в строку
            chunk_id = f"chunk_{hash(base_id_str)}"

            # 3. Создаем метаданные для чанка
            current_metadata = metadata_base.copy()
            current_metadata["chunk_index"] = i # Индекс чанка внутри документа
            current_metadata["chunk_text_preview"] = chunk[:150].replace('\n', ' ') # Превью текста чанка для отладки

            # 4. Добавляем чанк в коллекцию ChromaDB
            collection.add(
                ids=[str(chunk_id)], # ID должен быть строкой
                embeddings=[embedding],
                documents=[chunk], # Сам текст чанка
                metadatas=[current_metadata] # Метаданные
            )
            added_count += 1
            # time.sleep(0.05) # Небольшая пауза, может помочь при высокой нагрузке на Ollama/Chroma

        except Exception as e:
            logging.error(f"!!! ОШИБКА при индексации чанка {i} для '{metadata_base.get('url', 'URL не указан')}': {e}")
            # Пробуем пропустить проблемный чанк и продолжить
            continue

    logging.info(f"  Успешно добавлено {added_count} из {len(chunks)} чанков.")
    return added_count

def retrieve_relevant_document(query_text: str, collection, embed_model: str, k: int = TOP_K_RESULTS) -> tuple[str | None, dict | None]:
    """
    Извлекает k наиболее релевантных чанков из ChromaDB для заданного запроса.
    Возвращает текст и метаданные ЛУЧШЕГО (наиболее близкого) чанка,
    или (None, None), если ничего не найдено или произошла ошибка.
    """
    temp = query_text[:100].replace('\n',' ')
    logging.info(f"[RAG Retrieval] Поиск релевантных документов для запроса (раздела): '{temp}...'")
    try:
        # 1. Генерируем эмбеддинг для текста запроса
        # Убедитесь, что параметр называется 'prompt' или 'input' в зависимости от версии ollama-python и модели
        # Для mxbai-embed-large часто используется 'prompt'
        response = ollama.embed(model=embed_model, input=query_text)

        # === ВАЖНАЯ ПРОВЕРКА ===
        # Добавляем отладочный вывод *перед* попыткой доступа к ключу
        logging.debug(f"DEBUG: Ответ от ollama.embed для запроса '{query_text[:50]}...': {response}")

        if "embeddings" not in response:
            logging.error(f"!!! ОШИБКА: Ключ 'embedding' отсутствует в ответе ollama.embed для ЗАПРОСА '{query_text[:100]}...'. Ответ Ollama: {response}")
            # Не удалось сгенерировать эмбеддинг для запроса, поиск невозможен
            return None, None # Возвращаем None, None, как и в случае других ошибок

        query_embedding = response["embeddings"][0]

        # 2. Выполняем поиск в ChromaDB
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=k,
            include=['documents', 'metadatas', 'distances'] # Запрашиваем тексты, метаданные и расстояния
        )
        logging.debug(f"Результаты поиска в Chroma: {results}") # Отладочный вывод

        # 3. Обрабатываем результаты
        # Проверяем, что результаты не пусты и содержат ожидаемые списки
        if results and results.get('ids') and results['ids'] and results['ids'][0]:
            num_found = len(results['ids'][0])
            logging.info(f"  Найдено {num_found} релевантных чанков.")

            # Проверяем наличие всех необходимых данных для первого результата
            if (results.get('documents') and results['documents'][0] and
                results.get('metadatas') and results['metadatas'][0]):

                best_doc = results['documents'][0][0] # Текст первого (самого близкого) чанка
                best_metadata = results['metadatas'][0][0] # Метаданные первого чанка
                best_distance = results.get('distances', [[None]])[0][0] # Расстояние первого чанка (может быть None)

                if best_distance is not None:
                    logging.info(f"  Лучший чанк (ID: {results['ids'][0][0]}) имеет дистанцию: {best_distance:.4f}")
                else:
                     logging.info(f"  Используется лучший чанк (ID: {results['ids'][0][0]}), дистанция не рассчитана или не возвращена ChromaDB.")

                # Возвращаем текст и метаданные лучшего найденного чанка
                return best_doc, best_metadata
            else:
                logging.warning("  Результаты поиска найдены, но отсутствуют документы или метаданные в ожидаемом формате.")
                return None, None
        else:
            logging.info("  Релевантные документы не найдены в ChromaDB.")
            return None, None

    except KeyError as ke: # Явно ловим KeyError, если проверка выше не сработала
         logging.error(f"!!! KeyError при доступе к ответу Ollama или результатам Chroma: {ke}")
         return None, None
    except Exception as e:
        # Ловим другие возможные ошибки (сетевые, ошибки ChromaDB и т.д.)
        logging.error(f"!!! ОШИБКА при поиске в ChromaDB или генерации эмбеддинга запроса: {e}", exc_info=True) # Добавляем traceback
        return None, None


# --- Функции для шагов исследования ---

def generate_research_plan(topic: str) -> list[str]:
    """Шаг 1: Генерирует план исследования с помощью LLM."""
    logging.info(f"\n[Шаг 1] Генерация плана для темы: '{topic}'")
    system = ("Ты - опытный ИИ-ассистент для проведения исследований. Твоя задача - создать детальный, "
              "логически структурированный план для всестороннего исследования заданной темы. "
              "План должен включать стандартные разделы: Введение, несколько основных содержательных "
              "разделов (минимум 3-4), Заключение и Список использованной литературы. "
              "Выведи ТОЛЬКО нумерованный список названий разделов плана, каждый пункт на новой строке. "
              "Не добавляй никаких пояснений до или после списка.")
    prompt = f"Составь подробный план исследования на тему: \"{topic}\"."
    response = call_ollama_generate(prompt, system_message=system)
    if not response:
        logging.error("Не удалось сгенерировать план исследования.")
        return []

    plan_items = []
    # Устойчивый парсинг нумерованных/маркированных списков
    for line in response.strip().split('\n'):
        # Ищем строки, начинающиеся с цифры+точки, звездочки, дефиса (с возможными пробелами)
        match = re.match(r'^\s*[\*\-\d]+\.?\s*(.*)', line)
        if match:
            item = match.group(1).strip() # Берем текст после маркера/номера
            if item: # Добавляем только непустые строки
                plan_items.append(item)

    # Если парсинг не удался, пробуем просто разбить по строкам
    if not plan_items and response:
         logging.warning("Не удалось извлечь пункты плана с помощью регулярного выражения. Используем разбиение по строкам.")
         plan_items = [line.strip() for line in response.strip().split('\n') if line.strip()]

    # Проверка и добавление стандартных разделов, если их нет (эвристика)
    plan_lower = [p.lower() for p in plan_items]
    if not any(re.search(r'введен|introduc', item) for item in plan_lower):
        plan_items.insert(0, "Введение")
        logging.info("Добавлен раздел 'Введение' в план.")
    if not any(re.search(r'заключ|conclus|summary|вывод', item) for item in plan_lower):
        plan_items.append("Заключение")
        logging.info("Добавлен раздел 'Заключение' в план.")
    if not any(re.search(r'литератур|источник|ссылк|references|bibliography|sources', item) for item in plan_lower):
        plan_items.append("Список использованной литературы")
        logging.info("Добавлен раздел 'Список использованной литературы' в план.")


    logging.info(f"Сгенерированный и дополненный план ({len(plan_items)} пунктов):")
    for i, item in enumerate(plan_items):
        print(f"  {i+1}. {item}")
        logging.info(f"  {i+1}. {item}")
    return plan_items


def generate_search_queries(topic:str, plan_item: str, full_plan: list[str], num_queries: int = 1) -> list[str]:
    """
    Шаг 2: Генерирует поисковые запросы для конкретного пункта плана,
    учитывая общую тему и контекст всего плана.
    Пропускает генерацию для стандартных структурных разделов.
    """
    logging.info(f"\n[Шаг 2] Генерация поисковых запросов для пункта: '{plan_item}'")

    # Определяем, является ли пункт структурным (не требующим поиска)
    item_lower = plan_item.lower()
    if any(keyword in item_lower for keyword in ["введение", "introduction", "заключение", "conclusion", "summary", "выводы", "references", "bibliography", "literature", "sources", "список"]):
        logging.info("  Пропуск генерации запросов: это структурный раздел.")
        return []

    # Формируем контекст для LLM
    formatted_plan = "\n".join([f"- {item}" for item in full_plan])
    system = (f"Ты - ИИ-помощник по исследованиям. Тема исследования: '{topic}'.\n"
              f"Полный план исследования:\n{formatted_plan}\n\n"
              f"Твоя задача - сгенерировать {num_queries} точных и эффективных поисковых запроса (на русском или английском языке) "
              f"для сбора информации КОНКРЕТНО по следующему разделу плана: '{plan_item}'. "
              f"Запросы должны быть сфокусированы именно на этом разделе, но учитывать общий контекст темы. "
              f"Выведи ТОЛЬКО сами запросы, каждый на новой строке. Не добавляй нумерацию или маркеры.")
    prompt = f"Сгенерируй поисковые запросы для раздела плана: \"{plan_item}\"."

    response = call_ollama_generate(prompt, system_message=system)
    if not response:
        logging.error(f"Не удалось сгенерировать поисковые запросы для '{plan_item}'.")
        return []

    # Обрабатываем ответ: разделяем по строкам, убираем пустые
    queries = [q.strip() for q in response.strip().split('\n') if q.strip()]
    # Дополнительно убираем маркеры/нумерацию, если LLM их добавила вопреки инструкции
    queries = [re.sub(r'^\s*[\*\-\d]+\.?\s*', '', q) for q in queries]

    logging.info(f"Сгенерированные запросы ({len(queries)}): {queries}")
    return queries


def generate_section_text_smart(
    topic: str,
    full_plan: list[str],
    current_plan_item: str,
    item_index: int,
    collection, # ChromaDB коллекция
    embed_model: str, # Модель для эмбеддингов (для RAG)
    used_sources: list[dict] # Список для сбора использованных источников (модифицируется!)
    ) -> str:
    """
    Шаг 5 (Генерация): Генерирует текст для раздела отчета.
    - Использует RAG (поиск в ChromaDB) для содержательных разделов.
    - Генерирует Введение и Заключение на основе плана без RAG.
    - Форматирует Список литературы из собранных источников.
    - Добавляет найденные и использованные источники в список `used_sources`.
    """
    logging.info(f"\n[Шаг 5 - Smart Generation] Генерация текста для раздела {item_index+1}: '{current_plan_item}'")

    # Определяем тип раздела (регистронезависимо)
    item_lower = current_plan_item.lower()
    # Уточняем условия, чтобы избежать ложных срабатываний
    is_introduction = item_index == 0 and any(keyword in item_lower for keyword in ["введение", "introduction"])
    is_conclusion = (item_index == len(full_plan) - 2) and any(keyword in item_lower for keyword in ["заключение", "conclusion", "summary", "выводы"]) # Предполагаем, что список литературы последний
    is_references = (item_index == len(full_plan) - 1) and any(keyword in item_lower for keyword in ["references", "bibliography", "literature", "sources", "список", "источник"])

    # Базовый системный промпт для всех случаев
    formatted_plan = "\n".join([f"{idx+1}. {item}" for idx, item in enumerate(full_plan)])
    base_system_message = (f"Ты - ИИ-ассистент, помогающий писать структурированный научный отчет.\n"
                           f"Тема исследования: '{topic}'.\n"
                           f"Полный план отчета:\n{formatted_plan}\n")

    # --- Особая обработка структурных разделов ---

    if is_introduction:
        logging.info("  Тип раздела: Введение. Генерация без RAG.")
        system = base_system_message + ("\nТвоя задача: Напиши ВВЕДЕНИЕ для этого отчета. "
                                        "Кратко опиши актуальность темы, сформулируй цель и задачи исследования (если они подразумеваются планом), "
                                        "и представь структуру отчета согласно плану.")
        prompt = f"Напиши подробное введение для научного отчета на тему '{topic}', следуя представленному плану."
        response = call_ollama_generate(prompt, system_message=system)
        return response if response else f"Не удалось сгенерировать введение для раздела '{current_plan_item}'."

    elif is_conclusion:
        logging.info("  Тип раздела: Заключение. Генерация без RAG (на основе плана).")
        system = base_system_message + ("\nТвоя задача: Напиши ЗАКЛЮЧЕНИЕ для этого отчета. "
                                        "Подведи итоги исследования, кратко суммируя основные результаты или выводы по каждому содержательному разделу плана. "
                                        "Сделай общие выводы по теме исследования и, возможно, обозначь перспективы дальнейших исследований.")
        prompt = f"Напиши подробное заключение для научного отчета на тему '{topic}', суммируя ключевые аспекты, затронутые в плане."
        response = call_ollama_generate(prompt, system_message=system)
        return response if response else f"Не удалось сгенерировать заключение для раздела '{current_plan_item}'."

    elif is_references:
        logging.info("  Тип раздела: Список литературы. Форматирование собранных источников.")
        if not used_sources:
            return "В процессе исследования не было зафиксировано и использовано внешних источников."

        formatted_sources = []
        # Используем set для хранения уникальных URL, чтобы избежать дублирования
        unique_urls = set()
        # Сортируем источники по названию пункта плана, к которому они относятся
        sorted_sources = sorted(used_sources, key=lambda x: x.get('plan_item_id', ''))

        for source in sorted_sources:
             url = source.get('url')
             plan_item_ref = source.get('plan_item', 'Неизвестно') # Пункт плана, для которого нашли источник

             if url and url not in unique_urls:
                 # Пытаемся получить название страницы (title), если scraper его извлек
                 title = source.get('title', '').strip()
                 # Если title нет, используем доменное имя как запасной вариант
                 if not title:
                     try:
                         title = urlparse(url).netloc # Берем домен (e.g., 'example.com')
                     except:
                         title = url # Если парсинг URL не удался, используем сам URL
                 else:
                     # Очищаем title от лишних пробелов и символов новой строки
                     title = ' '.join(title.split())

                 # Формируем строку в формате Markdown: [Title](URL)
                 formatted_sources.append(f"- [{title}]({url}) (Источник для раздела: '{plan_item_ref}')")
                 unique_urls.add(url) # Добавляем URL в множество уникальных

        if not formatted_sources:
             return "Не найдено уникальных URL среди зафиксированных и использованных источников."

        # Возвращаем заголовок и отформатированный список
        return "Список использованной литературы:\n\n" + "\n".join(formatted_sources)

    # --- Обработка стандартных содержательных разделов с RAG ---
    else:
        logging.info(f"  Тип раздела: Содержательный. Попытка выполнить RAG для '{current_plan_item}'.")
        # 1. RAG Retrieval: Ищем релевантный чанк в ChromaDB
        # В качестве запроса используем название пункта плана
        retrieved_text, retrieved_metadata = retrieve_relevant_document(current_plan_item, collection, embed_model, k=TOP_K_RESULTS)

        # 2. Генерация текста: с RAG или без
        if not retrieved_text or not retrieved_metadata:
            # Если RAG не вернул контекст
            logging.warning("  Не удалось получить релевантный контекст из ChromaDB. Генерация будет выполнена без RAG, только на основе знаний LLM и плана.")
            system = base_system_message + (f"\nТвоя задача: Напиши текст для раздела плана '{current_plan_item}'. "
                                            f"Так как релевантный внешний контекст не найден, используй свои общие знания по теме '{topic}'. "
                                            f"Сосредоточься на содержании, соответствующем названию раздела, и учитывай его место в общем плане отчета. "
                                            f"Избегай фраз вроде 'Я не могу предоставить...' или 'У меня нет доступа к...'. Просто напиши содержательный текст.")
            prompt = f"Напиши текст для раздела научного отчета: \"{current_plan_item}\"."
            response = call_ollama_generate(prompt, system_message=system)
            return response if response else f"Не удалось сгенерировать текст для раздела '{current_plan_item}' (без RAG)."
        else:
            # Если RAG вернул контекст
            source_url = retrieved_metadata.get('url', 'URL не найден')
            source_title = retrieved_metadata.get('title', 'Заголовок не найден')
            logging.info(f"  Используется извлеченный контекст из источника: {source_url} (Заголовок: {source_title[:60]}...)")
            temp = retrieved_text[:200].replace('\n', ' ')
            logging.info(f"  Контекст (начало): {temp}...")

            # 3. Добавляем источник в список использованных (если есть URL)
            # Используем метаданные из *найденного чанка*, а не из исходного запроса scraper'а
            if source_url:
                 # Проверяем, что такого URL еще нет в списке
                 if not any(s.get('url') == source_url for s in used_sources):
                    source_info = {
                        'url': source_url,
                        'title': source_title,
                        'plan_item': retrieved_metadata.get('plan_item', current_plan_item), # К какому разделу относился исходный поиск
                        'plan_item_id': retrieved_metadata.get('plan_item_id', ''), # ID пункта плана
                        'source_query': retrieved_metadata.get('source_query', ''), # По какому запросу нашли
                        'retrieved_for': current_plan_item # Для какого раздела генерируем текст СЕЙЧАС
                    }
                    used_sources.append(source_info)
                    logging.info(f"  Источник {source_url} добавлен в список использованных.")
                 else:
                     logging.info(f"  Источник {source_url} уже есть в списке использованных.")


            # 4. RAG Generation: Генерируем текст с использованием найденного контекста
            system = base_system_message + (f"\nТвоя задача: Напиши текст для раздела плана '{current_plan_item}'. "
                                            f"Используй СЛЕДУЮЩИЙ КОНТЕКСТ, извлеченный из внешнего источника ({source_url}), как ОСНОВУ для твоего ответа. "
                                            f"Критически оценивай информацию из контекста, синтезируй её и адаптируй под структуру отчета и тему '{topic}'. "
                                            f"Текст должен быть связным, логичным и соответствовать научному стилю. Не копируй контекст дословно. "
                                            f"НЕ ИСПОЛЬЗУЙ фразы вроде 'На основе предоставленного контекста...', 'Согласно источнику...'. Просто излагай информацию.\n\n"
                                            f"ИЗВЛЕЧЕННЫЙ КОНТЕКСТ:\n---\n{retrieved_text}\n---")

            prompt = f"Напиши текст для раздела отчета \"{current_plan_item}\", основываясь на предоставленном контексте и общем плане исследования."
            response = call_ollama_generate(prompt, system_message=system)

            if not response:
                # Если генерация с RAG не удалась, можно попробовать сгенерировать без RAG (как запасной вариант)
                logging.warning(f"  Не удалось сгенерировать текст для раздела '{current_plan_item}' с RAG, несмотря на наличие контекста. Попытка генерации без RAG...")
                # Можно скопировать логику генерации без RAG отсюда
                system_fallback = base_system_message + (f"\nТвоя задача: Напиши текст для раздела плана '{current_plan_item}'. "
                                                        f"Используй свои общие знания по теме '{topic}'. "
                                                        f"Сосредоточься на содержании, соответствующем названию раздела, и учитывай его место в общем плане отчета.")
                prompt_fallback = f"Напиши текст для раздела научного отчета: \"{current_plan_item}\"."
                response = call_ollama_generate(prompt_fallback, system_message=system_fallback)
                return response if response else f"Не удалось сгенерировать текст для раздела '{current_plan_item}' (даже без RAG)."


            # (Опционально) Пост-обработка ответа: убрать возможные артефакты
            response = re.sub(r"^(На основе|Согласно|Исходя из) предоставленн(ого|ых) данн(ых|ым|ого контекста)[,:]?\s*", "", response, flags=re.IGNORECASE | re.MULTILINE).strip()
            response = re.sub(r"^\s*Ответ:\s*", "", response, flags=re.IGNORECASE).strip() # Убрать "Ответ:" в начале

            return response


# --- Основная функция запуска исследования ---
def run_research(topic: str) -> str:
    """
    Выполняет полный цикл исследования:
    1. Генерация плана.
    2. Генерация поисковых запросов для каждого пункта плана.
    3. Сбор данных с помощью внешнего скрапера (scraper7.py).
    4. Индексация собранных данных в ChromaDB.
    5. Генерация текста для каждого раздела отчета с использованием RAG (где применимо).
    6. Формирование и возврат итогового отчета.
    """
    start_time_total = time.time()
    logging.info(f"=== Запуск RAG-исследования по теме: '{topic}' ===")
    logging.info(f"Параметры: LLM={OLLAMA_LLM_MODEL}, Embeddings={OLLAMA_EMBED_MODEL}, DB=Chroma In-Memory '{CHROMA_COLLECTION_NAME}'")

    # --- Шаг 1: Генерация плана ---
    plan = generate_research_plan(topic)

    if not plan:
        error_message = "КРИТИЧЕСКАЯ ОШИБКА: Не удалось создать план исследования. Процесс остановлен."
        logging.error(error_message)
        return error_message

    # --- Шаг 2: Подготовка Задач для Скрапера ---
    logging.info("\n=== Шаг 2: Подготовка Задач для Скрапера ===")
    all_search_tasks = []
    for i, item in enumerate(plan):
        # Генерируем запросы только для содержательных разделов (функция сама это проверит)
        # Передаем topic и full_plan для лучшего качества запросов
        queries = generate_search_queries(topic, item, plan, num_queries=1) # Генерируем по 1 запросу для скорости
        if queries:
            for q_idx, query in enumerate(queries):
                 task = {
                     'query': query.replace('"',''),           # Поисковый запрос
                     'plan_item': item,        # Название пункта плана
                     'plan_item_id': f"plan_{i}", # Уникальный ID пункта плана
                     'query_id': f"q_{i}_{q_idx}" # Уникальный ID запроса (на случай нескольких запросов на 1 пункт)
                 }
                 all_search_tasks.append(task)
                 logging.info(f"  Подготовлена задача для скрапера: query='{query}', plan_item='{item}'")
        # time.sleep(0.2) # Небольшая пауза между запросами к LLM для генерации запросов

    if not all_search_tasks:
        logging.warning("Предупреждение: Не удалось сформировать ни одной поисковой задачи (возможно, план состоит только из Введения/Заключения/Списка литературы?).")
        # Исследование продолжится без сбора внешних данных

    # --- Шаг 3: Фаза Сбора Данных (вызов scraper7.py) ---
    scraped_results = []
    if all_search_tasks:
        logging.info(f"\n=== Шаг 3: Запуск Скрапера для {len(all_search_tasks)} задач ===")
        try:
            # Вызываем функцию из scraper7.py, передавая список задач
            # num_results_per_query=2: Пытаемся получить по 2 релевантных сайта на каждый запрос
            # max_workers=4: Используем 4 потока для параллельного скачивания/парсинга
            scraped_results = process_tasks_combined(
                                all_search_tasks,
                                num_ddg_results=20,
                                num_s2_papers_to_process=6
                            )
            logging.info(f"Скрапер завершил работу. Получено результатов: {len(scraped_results)}")
            # Дополнительная проверка статусов
            success_scrapes = sum(1 for r in scraped_results if r.get('status') == 'success' and r.get('text'))
            logging.info(f"Успешно извлечен текст из {success_scrapes} источников.")
        except ImportError:
             logging.error("!!! ОШИБКА: Не удалось импортировать 'process_search_tasks' из 'scraper7'. Убедитесь, что файл scraper7.py существует и не содержит ошибок.")
        except Exception as e:
            logging.error(f"!!! ОШИБКА во время выполнения process_search_tasks из scraper7: {e}", exc_info=True)
            # Продолжаем без данных от скрапера
    else:
        logging.info("\n=== Шаг 3: Пропуск фазы сбора данных (нет поисковых задач) ===")


    # --- Шаг 4: Фаза Индексации ---
    logging.info(f"\n=== Шаг 4: Фаза Индексации {len(scraped_results)} Собранных Результатов ===")
    total_chunks_added = 0
    indexed_urls = set() # Множество для отслеживания уже проиндексированных URL

    if not scraped_results:
        logging.info("Нет данных от скрапера для индексации.")
    else:
        for item_data in scraped_results:
            # Проверяем статус и наличие текста
            if item_data.get('status') != 'success' or not item_data.get('text'):
                logging.warning(f"  Пропуск индексации для URL {item_data.get('url', 'N/A')}: статус '{item_data.get('status', 'N/A')}' или отсутствует текст.")
                continue

            text_to_index = item_data['text']
            source_url = item_data.get('url')

            if not source_url:
                logging.warning(f"  Пропуск индексации: отсутствует URL в данных от скрапера для запроса '{item_data.get('query', 'N/A')}'.")
                continue

            # Проверяем, не индексировали ли мы этот URL ранее
            if source_url in indexed_urls:
                 logging.info(f"  Пропуск индексации для {source_url}: URL уже был проиндексирован ранее в этом запуске.")
                 continue

            # Формируем базовые метаданные для всех чанков этого документа
            # Используем информацию, которую вернул скрапер
            metadata = {
                "plan_item": item_data.get('plan_item', 'Неизвестно'),
                "plan_item_id": item_data.get('plan_item_id', ''),
                "source_query": item_data.get('query', 'Неизвестно'),
                "url": source_url,
                "title": item_data.get('title', 'Заголовок не извлечен'), # Заголовок, если скрапер его вернул
                # Доп. поля, если scraper7 их возвращает (например, 'extraction_method')
                "extraction_method": item_data.get('extraction_method', 'Неизвестно'),
            }
            logging.info(f"\n  Индексация контента (~{len(text_to_index)} симв.) с URL: {metadata['url']}")
            logging.info(f"  Связано с пунктом плана: '{metadata['plan_item']}' (ID: {metadata['plan_item_id']})")

            # Вызываем функцию добавления чанков в БД
            chunks_added = add_text_chunks_to_db(text_to_index, metadata, collection, OLLAMA_EMBED_MODEL)
            if chunks_added > 0:
                total_chunks_added += chunks_added
                indexed_urls.add(source_url) # Отмечаем URL как успешно проиндексированный
            # time.sleep(0.1) # Пауза между индексацией разных документов, если нужно

    logging.info(f"\n=== Индексация Завершена ===")
    logging.info(f"Всего добавлено новых чанков в ChromaDB: {total_chunks_added}")
    try:
        collection_count = collection.count()
        logging.info(f"Текущий размер коллекции '{collection.name}': {collection_count} чанков.")
        if collection_count == 0 and len(all_search_tasks) > 0:
             logging.warning("ПРЕДУПРЕЖДЕНИЕ: Поисковые задачи были, но ни одного чанка не добавлено в базу данных. Проверьте работу скрапера и/или процесса индексации.")
    except Exception as e:
        logging.error(f"Не удалось получить размер коллекции ChromaDB: {e}")


    # --- Шаг 5: Фаза Генерации Отчета ---
    logging.info("\n=== Шаг 5: Фаза Генерации Отчета (Smart Generation) ===")
    final_report_parts = []
    used_sources_list = [] # Список для сбора информации об использованных источниках (будет заполняться в generate_section_text_smart)

    # Добавляем заголовок отчета
    final_report_parts.append(f"# Исследование по теме: {topic}\n")

    # Генерируем текст для каждого пункта плана
    for i, item in enumerate(plan):
        # Вызываем улучшенную функцию генерации раздела
        section_text = generate_section_text_smart(
            topic=topic,
            full_plan=plan,
            current_plan_item=item,
            item_index=i,
            collection=collection, # Передаем коллекцию ChromaDB
            embed_model=OLLAMA_EMBED_MODEL, # Передаем имя модели эмбеддингов
            used_sources=used_sources_list # Передаем список для ИЗМЕНЕНИЯ (добавления источников)
        )
        # Добавляем заголовок раздела (Markdown H2) и сгенерированный текст
        # final_report_parts.append(f"\n## {i+1}. {item}\n") # Используем ## для подзаголовков
        final_report_parts.append(section_text if section_text else f"[Не удалось сгенерировать текст для этого раздела: {item}]\n")
        final_report_parts.append("\n") # Добавляем пустую строку для лучшего форматирования
        time.sleep(0.5) # Небольшая пауза между генерацией разделов

    # --- Завершение ---
    end_time_total = time.time()
    total_duration = end_time_total - start_time_total
    logging.info(f"\n=== Исследование завершено за {total_duration:.2f} секунд ===")

    # --- Очистка ChromaDB (если используется In-Memory) ---
    # Важно: Если вы используете PersistentClient, эту часть нужно закомментировать или убрать,
    # чтобы данные сохранялись между запусками.
    try:
        if isinstance(chroma_client, chromadb.Client): # Проверяем, что это не PersistentClient (у него нет delete_collection таким способом)
            logging.info(f"Очистка In-Memory коллекции ChromaDB '{CHROMA_COLLECTION_NAME}'...")
            chroma_client.delete_collection(name=CHROMA_COLLECTION_NAME)
            logging.info(f"In-Memory коллекция '{CHROMA_COLLECTION_NAME}' успешно очищена.")
    except Exception as e:
        logging.error(f"Не удалось очистить In-Memory коллекцию '{CHROMA_COLLECTION_NAME}': {e}")


    # Собираем все части отчета в одну строку
    return "".join(final_report_parts)

# --- Точка входа для запуска скрипта ---
if __name__ == '__main__':
    # Убедитесь, что Ollama сервер запущен локально
    # и необходимые модели скачаны:
    # ollama pull gemma2:latest
    # ollama pull mxbai-embed-large:latest

    # Пример темы исследования
    # test_topic = "Влияние изменения климата на биоразнообразие Арктики"
    test_topic = "нейросети для поиска картинок по текстовому описанию"


    # Запускаем основной процесс исследования
    final_report = run_research(test_topic)

    # Выводим итоговый отчет в консоль
    print("\n\n" + "="*20 + " ИТОГОВЫЙ ОТЧЕТ " + "="*20)
    print(final_report)
    print("="*58)


    # --- Сохранение отчета в файл ---
    # Генерируем имя файла из темы, заменяя недопустимые символы
    safe_topic_name = re.sub(r'[^\w\-_.]', '_', test_topic.replace(' ', '_'))[:60] # Ограничиваем длину
    report_filename = f"research_report_{safe_topic_name}.md" # Сохраняем в формате Markdown

    try:
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(final_report)
        print(f"\nОтчет успешно сохранен в файл: {report_filename}")
    except IOError as e:
        print(f"\n!!! ОШИБКА: Не удалось сохранить отчет в файл '{report_filename}': {e}")