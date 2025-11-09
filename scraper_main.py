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
from redis_client import create_redis_clients

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
redis_clients = None
stats_lock = threading.Lock()
total_stats = {
    'pages': 0,
    'urls': 0,
    'new': 0,
    'price_changes': 0,
    'duplicates': 0,
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
    Scrape all pages for a single task following scraper_test.py workflow.

    Workflow:
    - Handles different page states (SUCCESS, NO_RESULTS, CAPTCHA, TIMEOUT, UNKNOWN)
    - Retry strategy: on error restart browser, navigate to failed page, continue
    - Up to 3 total attempts per error
    - No retry on graceful stops (NO_RESULTS, pagination limit)

    Args:
        task: Dict with 'location', 'prop_type', 'transaction_type', 'task_key'
    """
    key = task['task_key']
    MAX_RETRIES = 3
    MAX_PAGES = 100  # OLX limit

    # Persistent state across retries
    pagination_limit = MAX_PAGES
    start_page = 1  # Which page to start from (updated on retry)

    for attempt in range(MAX_RETRIES):
        scraper = None

        try:
            if attempt > 0:
                logger.info(f"[{key}] 🔄 RETRY ATTEMPT {attempt + 1}/{MAX_RETRIES} from page {start_page}")
                time.sleep(2)  # Brief delay before retry

            # Initialize scraper with Redis clients
            scraper = OlxScraper(redis_clients)
            scraper.init_browser()
            logger.debug(f"[{key}] Browser initialized")

            # Construct URL for starting page
            start_url = scraper.get_page_url(task, start_page)

            # Navigate and get page state
            state = scraper.goto_url(start_url)
            logger.debug(f"[{key}] Page state: {state}")

            # Handle initial navigation result
            if state == PageState.NO_RESULTS:
                # Graceful stop - don't retry
                logger.info(f"[{key}] No results found")
                return

            elif state in [PageState.CAPTCHA, PageState.TIMEOUT, PageState.UNKNOWN]:
                # Error - will retry
                logger.error(f"[{key}] Initial navigation failed: {state}")
                scraper.save_debug_snapshot(f"{key}_initial_{state}_attempt{attempt + 1}")
                continue  # Next retry attempt

            elif state != PageState.SUCCESS:
                # Unexpected state
                logger.error(f"[{key}] Unexpected initial state: {state}")
                continue  # Next retry attempt

            # SUCCESS - proceed with scraping
            logger.info(f"[{key}] Starting scrape from page {start_page}")

            # Extract data from starting page
            page_num = scraper.get_page_number()
            page_data = scraper.get_page_data()

            # Store URLs with metadata
            metadata = {'location': task['location'], 'prop_type': task['prop_type'], 'transaction_type': task['transaction_type']}

            if page_data:
                stats = scraper.store_urls_batch(page_data, metadata)
                with stats_lock:
                    total_stats['urls'] += len(page_data)
                    total_stats['new'] += stats['new']
                    total_stats['price_changes'] += stats['price_changes']
                    total_stats['duplicates'] += stats['duplicates']
                    total_stats['pages'] += 1
                logger.info(f"[{key}] Page {page_num}: {len(page_data)} URLs | 🆕{stats['new']} 💰{stats['price_changes']} 🔄{stats['duplicates']}")

            # Detect total pages (only on first attempt from page 1)
            if attempt == 0 and start_page == 1:
                total_pages = scraper.get_total_pages()
                pagination_limit = min(total_pages, MAX_PAGES) if total_pages > 0 else MAX_PAGES
                logger.info(f"[{key}] Detected {pagination_limit} pages")

            current_page = start_page

            # Track if we should retry (error occurred)
            should_retry = False

            # Pagination loop
            while current_page < pagination_limit:
                if shutdown_event.is_set():
                    logger.info(f"[{key}] Shutdown requested at page {current_page}")
                    break

                logger.debug(f"[{key}] Navigating to next page from {current_page}...")

                # Navigate to next page
                next_state = scraper.goto_next_page()

                if next_state == PageState.SUCCESS:
                    # Extract actual page number from URL
                    page_num = scraper.get_page_number()
                    expected_page = current_page + 1

                    # Verify we landed on the expected page
                    if page_num != expected_page:
                        logger.warning(f"[{key}] Page mismatch! Expected {expected_page}, got {page_num}")
                        logger.info(f"[{key}] Attempting to navigate directly to page {expected_page}...")

                        # Construct URL for expected page
                        target_url = scraper.get_page_url(task, expected_page)

                        # Try to navigate directly to the correct page
                        recovery_state = scraper.goto_url(target_url)

                        if recovery_state != PageState.SUCCESS:
                            logger.error(f"[{key}] Failed to recover: could not navigate to page {expected_page}")
                            scraper.save_debug_snapshot(f"{key}_recovery_failed_page{expected_page}_attempt{attempt + 1}")
                            # Set retry from this page
                            start_page = expected_page
                            should_retry = True
                            break

                        logger.info(f"[{key}] ✓ Recovered: successfully navigated to page {expected_page}")
                        page_num = expected_page  # Update to expected page

                    # Update current page and extract data
                    current_page = page_num
                    page_data = scraper.get_page_data()

                    if page_data:
                        stats = scraper.store_urls_batch(page_data, metadata)
                        with stats_lock:
                            total_stats['urls'] += len(page_data)
                            total_stats['new'] += stats['new']
                            total_stats['price_changes'] += stats['price_changes']
                            total_stats['duplicates'] += stats['duplicates']
                            total_stats['pages'] += 1
                        logger.info(f"[{key}] Page {page_num}: {len(page_data)} URLs | 🆕{stats['new']} 💰{stats['price_changes']} 🔄{stats['duplicates']}")

                elif next_state == PageState.NO_RESULTS:
                    # Graceful stop - don't retry
                    logger.info(f"[{key}] Reached end of listings at page {current_page}")
                    break

                elif next_state in [PageState.TIMEOUT, PageState.UNKNOWN]:
                    # Error - will retry from this page
                    logger.warning(f"[{key}] Navigation failed on page {current_page}: {next_state}")
                    scraper.save_debug_snapshot(f"{key}_pagination_{next_state}_page{current_page}_attempt{attempt + 1}")
                    start_page = current_page + 1  # Retry from next page
                    should_retry = True
                    break

                elif next_state == PageState.CAPTCHA:
                    # Error - will retry from this page
                    logger.error(f"[{key}] Captcha on page {current_page}")
                    scraper.save_debug_snapshot(f"{key}_captcha_page{current_page}_attempt{attempt + 1}")
                    start_page = current_page + 1  # Retry from next page
                    should_retry = True
                    break

            # Check if we should retry or if pagination completed
            if should_retry:
                logger.warning(f"[{key}] Error occurred, will retry from page {start_page}")
                continue  # Next retry attempt

            # Pagination completed successfully
            logger.info(f"[{key}] ✅ Complete: scraped {current_page} of {pagination_limit} pages")
            return  # Success - exit retry loop

        except Exception as e:
            # Exception during workflow - will retry
            logger.error(f"[{key}] Exception: {str(e)[:200]}")
            if scraper:
                scraper.save_debug_snapshot(f"{key}_exception_attempt{attempt + 1}")

            # Set retry from current page if known
            if 'current_page' in locals():
                start_page = current_page
            logger.warning(f"[{key}] Will retry from page {start_page}")

            with stats_lock:
                total_stats['errors'] += 1
            continue  # Next retry attempt

        finally:
            # Always close browser after each attempt
            if scraper:
                try:
                    scraper.close_browser()
                    logger.debug(f"[{key}] Browser closed")
                except:
                    pass

    # All retries exhausted
    logger.error(f"[{key}] ❌ TASK FAILED after {MAX_RETRIES} attempts (last page: {start_page})")
    with stats_lock:
        total_stats['errors'] += 1

# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def main():
    """Main orchestrator - generates tasks and runs them in parallel"""
    global redis_clients

    print(f"\n{'='*60}\n {SITE_NAME.upper()} SCRAPER - TASK-BASED WORKFLOW\n{'='*60}")
    print(f" Workers: {MAX_WORKERS} | Locations: {len(LOCATIONS)} | Transactions: {len(TRANSACTION_TYPES)} | Types: {len(PROPERTY_TYPES)}")
    print(f" Max Pages per Task: 100 (OLX limit)")
    print(f"{'='*60}\n")

    start_time = time.time()

    # ========================================================================
    # SETUP: Connect to Redis
    # ========================================================================

    try:
        redis_clients = create_redis_clients(
            site_name=SITE_NAME,
            host="5.161.248.214",
            port=6379,
            password="redispass"
        )
        logger.info("✅ Redis clients connected")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        sys.exit(1)

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
    # CLEANUP: Close Redis connections
    # ========================================================================

    try:
        if redis_clients:
            for name, client in redis_clients.items():
                client.close()
            logger.info("✅ All Redis connections closed")
    except Exception as e:
        logger.warning(f"Error closing Redis connections: {e}")

    # ========================================================================
    # SUMMARY
    # ========================================================================

    elapsed = time.time() - start_time

    print(f"\n{'='*60}\n SCRAPING COMPLETE\n{'='*60}")
    print(f" Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f" Tasks: {completed}/{len(tasks)} completed")
    print(f" Pages: {total_stats['pages']:,} | URLs: {total_stats['urls']:,}")
    print(f" New: {total_stats['new']:,} | Price Changes: {total_stats['price_changes']:,}")
    print(f" Duplicates: {total_stats['duplicates']:,} | Errors: {total_stats['errors']}")
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
