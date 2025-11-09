#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OLX Scraper - Multi-threaded Main Orchestrator
Generates tasks and runs them in parallel using ThreadPoolExecutor
"""
import time
import logging
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from scraper_test import OlxScraper, PageState

# ============================================================================
# CONFIGURATION
# ============================================================================

MAX_WORKERS = 2  # Number of parallel browser instances
PAGES_PER_TASK = 50  # Pages to scrape per task
MAX_PAGES = 100  # Maximum pages per URL (OLX limit)

# Transaction types to scrape
TRANSACTION_TYPES = ["venda", "aluguel"]

# Sample regions to scrape (can be expanded)
REGIONS = [
    "estado-df/distrito-federal-e-regiao/brasilia/ra-xvi---lago-sul",
    "estado-df/distrito-federal-e-regiao/brasilia/ra-i---plano-piloto"
]

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Suppress noisy libraries
for lib in ["seleniumbase", "selenium", "urllib3"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

# ============================================================================
# GLOBALS
# ============================================================================

shutdown_event = threading.Event()
stats_lock = threading.Lock()
total_stats = {
    'tasks_completed': 0,
    'pages_scraped': 0,
    'urls_found': 0,
    'errors': 0
}

# ============================================================================
# SIGNAL HANDLERS
# ============================================================================

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    logger.info("⚠️  Shutdown requested - finishing current tasks...")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================================
# TASK GENERATION
# ============================================================================

def generate_tasks() -> list:
    """
    Generate scraping tasks from configuration.

    Each task contains:
    - transaction_type: "venda" or "aluguel"
    - region: URL path to region
    - start_page: First page to scrape
    - end_page: Last page to scrape
    - task_key: Unique identifier

    Returns:
        List of task dictionaries
    """
    logger.info("="*60)
    logger.info("📋 GENERATING TASKS")
    logger.info("="*60)

    tasks = []

    for transaction in TRANSACTION_TYPES:
        for region in REGIONS:
            # For now, create one task per region/transaction
            # (In future, could split into chunks based on detection)

            # Build base URL
            base_url = f"https://www.olx.com.br/imoveis/{transaction}/{region}"

            task = {
                'transaction_type': transaction,
                'region': region,
                'base_url': base_url,
                'start_page': 1,
                'end_page': PAGES_PER_TASK,
                'task_key': f"{transaction}_{region.split('/')[-1]}"
            }

            tasks.append(task)
            logger.info(f"✓ Task: {task['task_key']} (pages 1-{PAGES_PER_TASK})")

    logger.info("="*60)
    logger.info(f"✓ Generated {len(tasks)} tasks")
    logger.info("="*60)

    return tasks

# ============================================================================
# TASK EXECUTION
# ============================================================================

def scrape_task_worker(task: dict):
    """
    Worker function to scrape a single task.

    Args:
        task: Task dictionary with scraping parameters
    """
    task_key = task['task_key']
    base_url = task['base_url']
    start_page = task['start_page']
    end_page = task['end_page']

    scraper = None
    pages_scraped = 0
    urls_found = 0

    try:
        logger.info(f"[{task_key}] Starting task: {base_url}")

        # Initialize scraper
        scraper = OlxScraper()
        scraper.init_browser()

        # Navigate to first page
        if start_page == 1:
            url = base_url
        else:
            url = f"{base_url}?o={start_page}"

        state = scraper.goto_url(url)

        if state == PageState.NO_RESULTS:
            logger.warning(f"[{task_key}] No results found")
            return

        if state != PageState.SUCCESS:
            logger.error(f"[{task_key}] Failed to load first page: {state}")
            with stats_lock:
                total_stats['errors'] += 1
            return

        # Scrape pages
        current_page = start_page

        while current_page <= end_page:
            if shutdown_event.is_set():
                logger.info(f"[{task_key}] Shutdown requested at page {current_page}")
                break

            # Extract data from current page
            page_data = scraper.get_page_data()

            if page_data:
                pages_scraped += 1
                urls_found += len(page_data)

                # Log sample URLs (first 3)
                sample_urls = [url for url, price in page_data[:3]]
                logger.info(f"[{task_key}] Page {current_page}: {len(page_data)} URLs found")
                for url in sample_urls:
                    logger.debug(f"  - {url}")
            else:
                logger.warning(f"[{task_key}] Page {current_page}: No data extracted")
                with stats_lock:
                    total_stats['errors'] += 1

            # Navigate to next page if not at end
            if current_page < end_page:
                state = scraper.goto_next_page()

                if state == PageState.SUCCESS:
                    current_page += 1
                elif state == PageState.NO_RESULTS:
                    logger.info(f"[{task_key}] Reached end of results at page {current_page}")
                    break
                else:
                    logger.error(f"[{task_key}] Navigation failed at page {current_page}: {state}")
                    with stats_lock:
                        total_stats['errors'] += 1
                    break

                time.sleep(1)  # Polite delay between pages
            else:
                current_page += 1

        # Update stats
        with stats_lock:
            total_stats['tasks_completed'] += 1
            total_stats['pages_scraped'] += pages_scraped
            total_stats['urls_found'] += urls_found

        logger.info(f"[{task_key}] ✓ Complete: {pages_scraped} pages, {urls_found} URLs")

    except Exception as e:
        logger.error(f"[{task_key}] ✗ Task failed: {str(e)[:200]}")
        with stats_lock:
            total_stats['errors'] += 1

    finally:
        if scraper:
            try:
                scraper.close_browser()
            except:
                pass

# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def main():
    """Main orchestrator - generates tasks and runs them in parallel"""

    print("\n" + "="*60)
    print("OLX SCRAPER - MULTI-THREADED ORCHESTRATOR")
    print("="*60)
    print(f" Workers: {MAX_WORKERS}")
    print(f" Transactions: {len(TRANSACTION_TYPES)}")
    print(f" Regions: {len(REGIONS)}")
    print(f" Pages per task: {PAGES_PER_TASK}")
    print("="*60 + "\n")

    start_time = time.time()

    # ========================================================================
    # PHASE 1: Generate Tasks
    # ========================================================================

    tasks = generate_tasks()

    if not tasks:
        logger.error("✗ No tasks generated! Exiting.")
        sys.exit(1)

    # ========================================================================
    # PHASE 2: Execute Tasks in Parallel
    # ========================================================================

    logger.info("="*60)
    logger.info(f"🚀 EXECUTING {len(tasks)} TASKS")
    logger.info("="*60)

    completed = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(scrape_task_worker, task): task for task in tasks}

            while futures:
                if shutdown_event.is_set():
                    # Cancel remaining tasks
                    for f in futures:
                        f.cancel()
                    break

                # Wait for any task to complete
                done, not_done = wait(futures, timeout=1.0, return_when=FIRST_COMPLETED)

                for f in done:
                    try:
                        f.result()
                        completed += 1
                        logger.info(f"📊 Progress: {completed}/{len(tasks)} tasks complete")
                    except Exception as e:
                        logger.error(f"Task error: {e}")
                        completed += 1

                # Update futures dict to only track not done
                futures = {f: futures[f] for f in not_done}

    except KeyboardInterrupt:
        logger.warning("⚠️  Keyboard interrupt - shutting down...")
        shutdown_event.set()

    # ========================================================================
    # SUMMARY
    # ========================================================================

    elapsed = time.time() - start_time

    print("\n" + "="*60)
    print("SCRAPING COMPLETE")
    print("="*60)
    print(f" Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f" Tasks: {total_stats['tasks_completed']}/{len(tasks)} completed")
    print(f" Pages: {total_stats['pages_scraped']:,}")
    print(f" URLs: {total_stats['urls_found']:,}")
    print(f" Errors: {total_stats['errors']}")
    print("="*60 + "\n")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
