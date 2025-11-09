#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OLX Scraper - Multi-threaded Task-Based Orchestrator
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

SITE_NAME = "olx"
MAX_WORKERS = 3

LOCATIONS = [
    "",  # Base state-level
    "/distrito-federal-e-regiao",
    "/distrito-federal-e-regiao/brasilia",
    "/distrito-federal-e-regiao/outras-cidades",
    "/distrito-federal-e-regiao/outras-cidades/formosa",
    "/distrito-federal-e-regiao/outras-cidades/novo-gama",
    "/distrito-federal-e-regiao/outras-cidades/valparaiso-de-goias",
    "/distrito-federal-e-regiao/outras-cidades/cidade-ocidental",
    "/distrito-federal-e-regiao/outras-cidades/santo-antonio-do-descoberto",
    "/distrito-federal-e-regiao/outras-cidades/luziania",
    "/distrito-federal-e-regiao/outras-cidades/aguas-lindas-de-goias",
    "/distrito-federal-e-regiao/brasilia/ra-xvii---riacho-fundo-i",
    "/distrito-federal-e-regiao/brasilia/ra-xv---recanto-das-emas",
    "/distrito-federal-e-regiao/brasilia/ra-x---guara",
    "/distrito-federal-e-regiao/brasilia/ra-xii---samambaia",
    "/distrito-federal-e-regiao/brasilia/ra-viii---nucleo-bandeirante",
    "/distrito-federal-e-regiao/brasilia/ra-xiv---sao-sebastiao",
    "/distrito-federal-e-regiao/brasilia/ra-iv---brazlandia",
    "/distrito-federal-e-regiao/brasilia/ra-vi---planaltina",
    "/distrito-federal-e-regiao/brasilia/ra-vii---paranoa",
    "/distrito-federal-e-regiao/brasilia/ra-xxx---vicente-pires",
    "/distrito-federal-e-regiao/brasilia/ra-xx---aguas-claras",
    "/distrito-federal-e-regiao/brasilia/ra-xix---candangolandia",
    "/distrito-federal-e-regiao/brasilia/ra-ix---ceilandia",
    "/distrito-federal-e-regiao/brasilia/ra-xxiv---park-way",
    "/distrito-federal-e-regiao/brasilia/ra-xxii---sudoeste-e-octogonal",
    "/distrito-federal-e-regiao/brasilia/ra-v---sobradinho",
    "/distrito-federal-e-regiao/brasilia/ra-xi---cruzeiro",
    "/distrito-federal-e-regiao/brasilia/ra-xxvii---jardim-botanico",
    "/distrito-federal-e-regiao/brasilia/ra-ii---gama",
    "/distrito-federal-e-regiao/brasilia/ra-xviii---lago-norte",
    "/distrito-federal-e-regiao/brasilia/ra-xxviii---itapoa",
    "/distrito-federal-e-regiao/brasilia/ra-i---brasilia",
    "/distrito-federal-e-regiao/brasilia/ra-xxi---riacho-fundo-ii",
    "/distrito-federal-e-regiao/brasilia/ra-xiii---santa-maria",
    "/distrito-federal-e-regiao/brasilia/ra-xvi---lago-sul",
    "/distrito-federal-e-regiao/brasilia/ra-iii---taguatinga"
]

PROPERTY_TYPES = ["apartamentos", "casas"]

TRANSACTION_TYPES = ["venda", "aluguel"]

MAX_RETRIES = 3
MAX_NAV_RETRIES = 3  # Retries for navigation failures

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
    'pages': 0,
    'urls': 0,
    'errors': 0
}

# ============================================================================
# SIGNAL HANDLERS
# ============================================================================

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    logger.info("Shutdown requested")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================================
# TASK GENERATION
# ============================================================================

def generate_tasks() -> list:
    """
    Generate simple task list without page detection.

    Each task represents a complete scraping job for:
    - One location
    - One property type
    - One transaction type

    Workers will detect pages at runtime (max 100 for OLX).

    Returns:
        List of task dicts with keys: location, prop_type, transaction_type, task_key
    """
    logger.info("="*60)
    logger.info("📋 GENERATING TASKS")
    logger.info("="*60)

    tasks = []

    for location in LOCATIONS:
        for prop_type in PROPERTY_TYPES:
            for transaction_type in TRANSACTION_TYPES:
                # Use "base" for empty location to avoid leading underscore in task_key
                location_name = location if location else "base"

                task = {
                    'location': location,  # Keep original (empty string) for URL building
                    'prop_type': prop_type,
                    'transaction_type': transaction_type,
                    'task_key': f"{location_name}_{prop_type}_{transaction_type}"
                }
                tasks.append(task)

    logger.info(f"✓ Generated {len(tasks)} tasks")
    logger.info(f"  Locations: {len(LOCATIONS)} | Property Types: {len(PROPERTY_TYPES)} | Transactions: {len(TRANSACTION_TYPES)}")
    logger.info("="*60)

    return tasks

# ============================================================================
# TASK EXECUTION
# ============================================================================

def scrape_task(task: dict):
    """
    Scrape all pages for a single task (location + prop_type + transaction_type).

    Each worker handles the complete task:
    1. Navigate to page 1
    2. Detect total pages (max 100 for OLX)
    3. Scrape pages 1 through total_pages

    Args:
        task: Dict with 'location', 'prop_type', 'transaction_type', 'task_key'
    """
    key = task['task_key']
    scraper = None

    try:
        # Create scraper
        scraper = OlxScraper()
        scraper.init_browser()

        logger.info(f"[{key}] Starting task...")

        # Navigate to page 1 with retries
        page_1_url = scraper.get_page_url(task, 1)
        nav_success = False
        for attempt in range(MAX_NAV_RETRIES):
            state = scraper.goto_url(page_1_url)

            if state == PageState.SUCCESS:
                nav_success = True
                break
            elif state == PageState.NO_RESULTS:
                logger.warning(f"[{key}] No results found")
                return

            logger.warning(f"[{key}] Navigation attempt {attempt+1}/{MAX_NAV_RETRIES} failed: {state}")

            if attempt < MAX_NAV_RETRIES - 1:
                logger.info(f"[{key}] Restarting browser and retrying...")
                scraper.restart_browser()

        if not nav_success:
            logger.error(f"[{key}] Failed to navigate to page 1 after {MAX_NAV_RETRIES} attempts, skipping task")
            with stats_lock:
                total_stats['errors'] += 1
            return

        # Detect total pages (max 100 for OLX)
        total_pages = scraper.get_total_pages()

        if total_pages == 0:
            logger.warning(f"[{key}] No pages detected, skipping task")
            with stats_lock:
                total_stats['errors'] += 1
            return

        logger.info(f"[{key}] Detected {total_pages} pages, starting scrape")

        # Loop through all pages (starting from page 1, already loaded)
        for page in range(1, total_pages + 1):
            if shutdown_event.is_set():
                logger.info(f"[{key}] Shutdown requested at page {page}")
                break

            # Extract URLs from current page with retry logic
            urls = []
            for attempt in range(MAX_RETRIES):
                try:
                    urls = scraper.get_page_data()
                    if urls:
                        break
                except Exception as e:
                    logger.error(f"[{key}] Page {page} extraction error (attempt {attempt+1}): {e}")

                if attempt < MAX_RETRIES - 1:
                    logger.debug(f"[{key}] Restarting browser and retrying page {page}")
                    scraper.restart_browser()
                    scraper.goto_url(scraper.get_page_url(task, page))

            # Check for "no results" page - stop scraping if detected
            if scraper.check_no_results_page():
                logger.warning(f"[{key}] Page {page}/{total_pages}: 'No results' message detected - stopping early")
                break

            # Log URLs found
            if urls:
                with stats_lock:
                    total_stats['urls'] += len(urls)
                    total_stats['pages'] += 1

                # Log sample URLs (first 3)
                sample_urls = [url for url, price in urls[:3]]
                logger.info(f"[{key}] Page {page}/{total_pages}: {len(urls)} URLs")
                for url in sample_urls:
                    logger.debug(f"  - {url}")
            else:
                with stats_lock:
                    total_stats['errors'] += 1
                logger.warning(f"[{key}] Page {page}/{total_pages}: No URLs extracted after {MAX_RETRIES} attempts")

            # Navigate to next page (if not last page)
            if page < total_pages:
                expected_next_page = page + 1

                time.sleep(1)  # Polite delay

                state = scraper.goto_next_page()

                if state == PageState.SUCCESS:
                    # Verify we're on expected page
                    actual_page = scraper.get_page_number()
                    if actual_page != expected_next_page:
                        logger.warning(f"[{key}] Page mismatch: expected {expected_next_page}, got {actual_page}")
                        # Try direct navigation
                        scraper.goto_url(scraper.get_page_url(task, expected_next_page))
                elif state == PageState.NO_RESULTS:
                    logger.info(f"[{key}] Reached end at page {page}")
                    break
                else:
                    logger.warning(f"[{key}] Navigation failed, using direct URL for page {expected_next_page}")
                    next_page_url = scraper.get_page_url(task, expected_next_page)

                    # Try navigation with retries
                    nav_success = False
                    for attempt in range(MAX_NAV_RETRIES):
                        state = scraper.goto_url(next_page_url)
                        if state == PageState.SUCCESS:
                            nav_success = True
                            break

                        logger.warning(f"[{key}] Navigation attempt {attempt+1}/{MAX_NAV_RETRIES} failed for page {expected_next_page}")

                        if attempt < MAX_NAV_RETRIES - 1:
                            logger.info(f"[{key}] Restarting browser and retrying...")
                            scraper.restart_browser()

                    if not nav_success:
                        logger.error(f"[{key}] Failed to navigate to page {expected_next_page} after {MAX_NAV_RETRIES} attempts, stopping task")
                        with stats_lock:
                            total_stats['errors'] += 1
                        break

                time.sleep(1)

        logger.info(f"[{key}] Complete - scraped {page}/{total_pages} pages")

    except Exception as e:
        logger.error(f"[{key}] Failed: {e}")
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

    print(f"\n{'='*60}\n {SITE_NAME.upper()} SCRAPER - TASK-BASED WORKFLOW\n{'='*60}")
    print(f" Workers: {MAX_WORKERS} | Locations: {len(LOCATIONS)} | Transactions: {len(TRANSACTION_TYPES)} | Types: {len(PROPERTY_TYPES)}")
    print(f" Max Pages per Task: 100 (OLX limit)")
    print(f"{'='*60}\n")

    start_time = time.time()

    # ========================================================================
    # GENERATE TASKS
    # ========================================================================

    tasks = generate_tasks()

    if not tasks:
        logger.error("✗ No tasks generated! Exiting.")
        sys.exit(1)

    # ========================================================================
    # EXECUTE TASKS IN PARALLEL
    # ========================================================================

    logger.info("="*60)
    logger.info(f"🚀 EXECUTING {len(tasks)} TASKS")
    logger.info("="*60)

    completed = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(scrape_task, task): task for task in tasks}

            while futures:
                if shutdown_event.is_set():
                    for f in futures:
                        f.cancel()
                    break

                done, not_done = wait(futures, timeout=1.0, return_when=FIRST_COMPLETED)

                for f in done:
                    try:
                        f.result()
                        completed += 1
                        logger.info(f"Progress: {completed}/{len(tasks)} tasks complete")
                    except Exception as e:
                        logger.error(f"Task error: {e}")
                        completed += 1

                futures = {f: futures[f] for f in not_done}

    except KeyboardInterrupt:
        shutdown_event.set()

    # ========================================================================
    # SUMMARY
    # ========================================================================

    elapsed = time.time() - start_time

    print(f"\n{'='*60}\n SCRAPING COMPLETE\n{'='*60}")
    print(f" Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f" Tasks: {completed}/{len(tasks)} completed")
    print(f" Pages: {total_stats['pages']:,} | URLs: {total_stats['urls']:,}")
    print(f" Errors: {total_stats['errors']}")
    print(f"{'='*60}\n")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        sys.exit(1)
