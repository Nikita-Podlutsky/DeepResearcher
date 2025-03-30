import concurrent.futures
import logging
from duckduckgo_search import DDGS
from newspaper import Article, ArticleException

# Configure logging for better visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_url(url: str, task_info: dict) -> dict:
    """
    Downloads and parses a single URL using newspaper3k.
    Returns a dictionary with scraped data or error information.
    """
    logging.info(f"Attempting to scrape: {url} for query_id: {task_info['query_id']}")
    result = {
        **task_info,  # Include original task info
        "url": url,
        "title": "N/A",
        "text": "",
        "extraction_method": "newspaper3k",
        "content_length": 0,
        "status": "error",
        "error_message": None
    }
    try:
        # Initialize newspaper Article
        # You might need to specify language for better results on non-English pages
        # article = Article(url, language='ru') # Example for Russian
        article = Article(url)

        # Download the HTML content (newspaper3k handles requests internally)
        # Note: Default timeouts apply. For long-running requests, advanced handling might be needed.
        article.download()

        # Parse the article to extract title, text, etc.
        article.parse()

        # Check if extraction was successful
        if not article.text or not article.title:
            result["error_message"] = "Newspaper3k failed to extract sufficient content."
            logging.warning(f"Extraction failed for {url}: {result['error_message']}")
            return result # Return with status="error"

        # Successfully extracted content
        result["title"] = article.title
        result["text"] = article.text
        result["content_length"] = len(article.text)
        result["status"] = "success"
        result.pop("error_message") # Remove error message key on success
        logging.info(f"Successfully scraped: {url} (Title: {article.title[:50]}...)")

    except ArticleException as e:
        result["error_message"] = f"Newspaper3k ArticleException: {e}"
        logging.error(f"Newspaper3k error scraping {url}: {e}")
    except Exception as e:
        # Catch other potential errors (network issues, timeouts, etc.)
        result["error_message"] = f"General scraping error: {e}"
        logging.error(f"General error scraping {url}: {e}")

    return result


def process_search_tasks(tasks: list[dict], num_results_per_query: int = 3, max_workers: int = 5) -> list[dict]:
    """
    Processes a list of search tasks. For each task, performs a web search,
    scrapes the top results, and returns a list of scraped page details.

    Args:
        tasks: A list of dictionaries, each containing 'query', 'plan_item', etc.
        num_results_per_query: The maximum number of search results to scrape per query.
        max_workers: The number of concurrent threads to use for scraping.

    Returns:
        A list of dictionaries, where each dictionary represents one successfully
        or unsuccessfully scraped URL, including original task info.
    """
    all_results = []
    urls_to_scrape = [] # List of tuples: (url, original_task_dict)

    # --- 1. Perform Searches ---
    logging.info(f"Starting search phase for {len(tasks)} tasks...")
    # Using DDGS context manager is recommended
    with DDGS() as ddgs:
        for task in tasks:
            query = task['query']
            logging.info(f"Searching for: '{query}' (query_id: {task['query_id']})")
            try:
                # Fetch search results using DuckDuckGo
                # Using region='wt-wt' can sometimes yield more diverse international results
                search_results = list(ddgs.text(query, region='wt-wt', max_results=num_results_per_query))

                if not search_results:
                    logging.warning(f"No search results found for query: '{query}'")
                    continue

                for i, result in enumerate(search_results):
                     # result format is like {'title': '...', 'href': '...', 'body': '...'}
                     url = result.get('href')
                     if url:
                         # Add the URL and its corresponding task info to the list
                         urls_to_scrape.append((url, task))
                     else:
                         logging.warning(f"Search result {i+1} for '{query}' missing 'href' key.")

            except Exception as e:
                logging.error(f"Error during DDG search for query '{query}': {e}")
                # Optionally add an error entry for the task itself if search fails completely
                # Example: all_results.append({... task info ..., 'status': 'search_error', 'error_message': str(e)})

    logging.info(f"Search phase complete. Found {len(urls_to_scrape)} URLs to scrape.")

    if not urls_to_scrape:
        return [] # Return empty list if no URLs were found

    # --- 2. Scrape URLs Concurrently ---
    logging.info(f"Starting scraping phase with {max_workers} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a mapping from future back to the url and task info for logging/error handling
        future_to_url_task = {
            executor.submit(scrape_url, url, task_info): (url, task_info)
            for url, task_info in urls_to_scrape
        }

        for future in concurrent.futures.as_completed(future_to_url_task):
            url, task_info = future_to_url_task[future]
            try:
                # Get the result dictionary from the scrape_url function
                result_dict = future.result()
                if result_dict: # scrape_url should always return a dict
                    all_results.append(result_dict)
            except Exception as exc:
                # Catch potential exceptions raised *during future execution itself*
                # (less likely if scrape_url has good try/except, but good for robustness)
                logging.error(f"Exception processing future for URL {url}: {exc}")
                # Create an error entry if the future itself failed unexpectedly
                error_result = {
                    **task_info,
                    "url": url,
                    "title": "N/A", "text": "", "extraction_method": "newspaper3k",
                    "content_length": 0, "status": "error",
                    "error_message": f"Concurrency execution error: {exc}"
                }
                all_results.append(error_result)

    logging.info(f"Scraping phase complete. Collected {len(all_results)} results.")
    return all_results

# --- Example Usage ---
if __name__ == "__main__":
    test_tasks = [
        {'query': "применение трансформеров в NLP", 'plan_item': "Обзор трансформеров", 'plan_item_id': "plan_0", 'query_id': "q_0_0"},
        {'query': "React component lifecycle hooks", 'plan_item': "React Lifecycle", 'plan_item_id': "plan_1", 'query_id': "q_1_0"},
        {'query': "python dynamic content loading example", 'plan_item': "Dynamic Content", 'plan_item_id': "plan_4", 'query_id': "q_4_0"},
        {'query': "методы кластеризации данных", 'plan_item': "Кластеризация", 'plan_item_id': "plan_2", 'query_id': "q_2_0"},
        {'query': "fastapi background tasks tutorial", 'plan_item': "FastAPI Tasks", 'plan_item_id': "plan_5", 'query_id': "q_5_0"},
        {'query': "asyncio python web scraping", 'plan_item': "Asyncio Scraping", 'plan_item_id': "plan_6", 'query_id': "q_6_0"},
        {'query': "что такое vector database", 'plan_item': "Vector DB Intro", 'plan_item_id': "plan_7", 'query_id': "q_7_0"},
        {'query': "несуществующая чепуха абракадабра xyzzy фываолдж", 'plan_item': "Тест ошибки", 'plan_item_id': "plan_3", 'query_id': "q_3_0"} # Expect no results or errors
    ]

    # Process the tasks, scraping top 2 results per query using 4 worker threads
    scraped_data = process_search_tasks(test_tasks, num_results_per_query=2, max_workers=4)

    print("\n--- Scraping Results ---")
    # Print results (or process them further)
    import json
    print(json.dumps(scraped_data, indent=2, ensure_ascii=False))

    print(f"\nTotal results collected: {len(scraped_data)}")

    # You can analyze the results, e.g., count successes/failures
    success_count = sum(1 for r in scraped_data if r['status'] == 'success')
    error_count = len(scraped_data) - success_count
    print(f"Successful scrapes: {success_count}")
    print(f"Failed scrapes/errors: {error_count}")