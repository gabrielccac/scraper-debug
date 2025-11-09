#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Simple Thread-Based Scraper using new Redis-only architecture
"""
import time, logging, signal, sys, threading, queue
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from scraper_worker import OlxScraper

# Config
SITE_NAME = "olx"
MAX_WORKERS = 3

LOCATIONS = [
    "",
    "/distrito-federal-e-regiao",
    # "/distrito-federal-e-regiao/brasilia",
    # "/distrito-federal-e-regiao/outras-cidades",
    # "/distrito-federal-e-regiao/outras-cidades/formosa",
    # "/distrito-federal-e-regiao/outras-cidades/novo-gama",
    # "/distrito-federal-e-regiao/outras-cidades/valparaiso-de-goias",
    # "/distrito-federal-e-regiao/outras-cidades/cidade-ocidental",
    # "/distrito-federal-e-regiao/outras-cidades/santo-antonio-do-descoberto",
    # "/distrito-federal-e-regiao/outras-cidades/luziania",
    # "/distrito-federal-e-regiao/outras-cidades/aguas-lindas-de-goias",
    # "/distrito-federal-e-regiao/brasilia/ra-xvii---riacho-fundo-i",
    # "/distrito-federal-e-regiao/brasilia/ra-xv---recanto-das-emas",
    # "/distrito-federal-e-regiao/brasilia/ra-x---guara",
    # "/distrito-federal-e-regiao/brasilia/ra-xii---samambaia",
    # "/distrito-federal-e-regiao/brasilia/ra-viii---nucleo-bandeirante",
    # "/distrito-federal-e-regiao/brasilia/ra-xiv---sao-sebastiao",
    # "/distrito-federal-e-regiao/brasilia/ra-iv---brazlandia",
    # "/distrito-federal-e-regiao/brasilia/ra-vi---planaltina",
    # "/distrito-federal-e-regiao/brasilia/ra-vii---paranoa",
    # "/distrito-federal-e-regiao/brasilia/ra-xxx---vicente-pires",
    # "/distrito-federal-e-regiao/brasilia/ra-xx---aguas-claras",
    # "/distrito-federal-e-regiao/brasilia/ra-xix---candangolandia",
    # "/distrito-federal-e-regiao/brasilia/ra-ix---ceilandia",
    # "/distrito-federal-e-regiao/brasilia/ra-xxiv---park-way",
    # "/distrito-federal-e-regiao/brasilia/ra-xxii---sudoeste-e-octogonal",
    # "/distrito-federal-e-regiao/brasilia/ra-v---sobradinho",
    # "/distrito-federal-e-regiao/brasilia/ra-xi---cruzeiro",
    # "/distrito-federal-e-regiao/brasilia/ra-xxvii---jardim-botanico",
    # "/distrito-federal-e-regiao/brasilia/ra-ii---gama",
    # "/distrito-federal-e-regiao/brasilia/ra-xviii---lago-norte",
    # "/distrito-federal-e-regiao/brasilia/ra-xxviii---itapoa",
    # "/distrito-federal-e-regiao/brasilia/ra-i---brasilia",
    # "/distrito-federal-e-regiao/brasilia/ra-xxi---riacho-fundo-ii",
    # "/distrito-federal-e-regiao/brasilia/ra-xiii---santa-maria",
    # "/distrito-federal-e-regiao/brasilia/ra-xvi---lago-sul",
    # "/distrito-federal-e-regiao/brasilia/ra-iii---taguatinga"
]

PROPERTY_TYPES = ["apartamentos", "casas"]

TRANSACTION_TYPES = ["venda", "aluguel"]

MAX_RETRIES = 3
MAX_NAV_RETRIES = 3  # Retries for navigation failures

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(threadName)s] - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
for l in ["seleniumbase", "selenium", "pika"]: logging.getLogger(l).setLevel(logging.WARNING)

# Globals
shutdown_event = threading.Event()
stats_lock = threading.Lock()
total_stats = {'pages': 0, 'urls': 0, 'new': 0, 'price_changes': 0, 'duplicates': 0, 'errors': 0}  # CHANGED: Stats names

# Redis writer queue - decouples Redis I/O from scraping
redis_queue = queue.Queue(maxsize=1000)  # Buffer up to 1000 batches

def signal_handler(sig, frame):
    logger.info("Shutdown requested")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def redis_writer_worker():
    """
    Background thread that consumes URL batches from queue and writes to Redis.

    This decouples Redis network I/O from scraper threads, allowing scrapers
    to continue immediately after extraction without blocking on Redis writes.
    """
    logger.info("🔌 Redis writer thread started")

    while not shutdown_event.is_set():
        try:
            # Get batch from queue (timeout so we can check shutdown)
            batch_data = redis_queue.get(timeout=1.0)

            if batch_data is None:  # Poison pill for shutdown
                break

            urls, metadata = batch_data

            # Write to Redis (this blocks, but doesn't block scrapers!)
            try:
                # Use the scraper's store method via redis_clients
                stats = redis_clients['url_stream'].store_batch(urls, metadata)

                # Update global stats
                with stats_lock:
                    total_stats['urls'] += len(urls)
                    total_stats['new'] += stats['new']
                    total_stats['price_changes'] += stats['price_changes']
                    total_stats['duplicates'] += stats['duplicates']

                logger.debug(f"✓ Redis wrote {len(urls)} URLs: 🆕{stats['new']} 💰{stats['price_changes']} 🔄{stats['duplicates']}")

            except Exception as e:
                logger.error(f"Redis write error: {e}")
                with stats_lock:
                    total_stats['errors'] += 1

            redis_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Redis writer unexpected error: {e}")

    logger.info("🔌 Redis writer thread stopped")

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
    total_combinations = len(LOCATIONS) * len(PROPERTY_TYPES) * len(TRANSACTION_TYPES)

    for location in LOCATIONS:
        for prop_type in PROPERTY_TYPES:
            for transaction_type in TRANSACTION_TYPES:
                # Use "nacional" for empty location to avoid leading underscore in task_key
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

def click_next_with_verification(scraper, expected_page_num: int, max_retries: int = 3) -> bool:
    """
    Click next page and verify we landed on the expected page.

    For Olx: Waits for page url to change dynamically, then checks page number url.

    Args:
        scraper: Scraper instance
        expected_page_num: Expected page number after click
        max_retries: Number of retry attempts

    Returns:
        True if successfully navigated to expected page
    """
    for attempt in range(max_retries):
        if shutdown_event.is_set():
            logger.debug("Shutdown requested, aborting click navigation")
            return False

        try:
            # Store current page source before clicking
            current_url = scraper.sb.get_current_url()

            # Click the next button
            if not scraper.click_next_page():
                logger.debug(f"Next button click failed (attempt {attempt+1})")
                continue

            # Wait for page source to change (with timeout)
            url_changed = False
            start_wait = time.time()

            while time.time() - start_wait < 8:
                if shutdown_event.is_set():
                    return False

                new_url = scraper.sb.get_current_url()
                if new_url != current_url:
                    url_changed = True
                    break

                time.sleep(0.3)

            if not url_changed:
                logger.debug(f"Page url didn't change after click (attempt {attempt+1}/{max_retries})")
                continue

            # Now verify page number using DOM
            current_page = scraper.get_current_page_number()

            if current_page == expected_page_num:
                logger.debug(f"✓ Successfully navigated to page {expected_page_num}")
                return True
            else:
                logger.debug(f"Page number mismatch: expected {expected_page_num}, got {current_page}")
                if expected_page_num == 1 and current_page == 1:
                    logger.debug(f"✓ On page 1 (verified via DOM)")
                    return True

        except Exception as e:
            logger.debug(f"Click verification error (attempt {attempt+1}/{max_retries}): {str(e)[:100]}")

    logger.warning(f"Failed to verify navigation to page {expected_page_num} after {max_retries} attempts")
    return False

def scrape_task(task):
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
        # Create scraper with Redis clients
        scraper = OlxScraper()
        scraper.init_browser(headless=False)

        logger.info(f"[{key}] Starting task...")

        # Navigate to page 1 with retries
        first_page = scraper.get_page_url(task, 1)
        for attempt in range(MAX_NAV_RETRIES):
            if scraper.navigate(first_page):
                nav_success = True
                break

            logger.warning(f"[{key}] Navigation attempt {attempt+1}/{MAX_NAV_RETRIES} failed for page 1")

            if attempt < MAX_NAV_RETRIES - 1:
                logger.info(f"[{key}] Restarting browser and retrying...")
                scraper.restart_browser()

        if not nav_success:
            logger.error(f"[{key}] Failed to navigate to page 1 after {MAX_NAV_RETRIES} attempts, skipping task")
            return

        # Detect total pages (max 100 for OLX)
        total_pages = scraper.detect_total_pages()

        if total_pages == 0:
            logger.warning(f"[{key}] No pages detected, skipping task")
            return

        logger.info(f"[{key}] Detected {total_pages} pages, starting scrape")

        # Metadata for Redis storage
        metadata = {'location': task['location'], 'prop_type': task['prop_type'], 'transaction_type': task['transaction_type']}

        # Loop through all pages (starting from page 1, already loaded)
        for page in range(1, total_pages + 1):
            if shutdown_event.is_set():
                logger.info(f"[{key}] Shutdown requested at page {page}")
                break

            if not scraper.verify_page_loaded():
                logger.warning(f"[{key}] Page {page} content not loaded, retrying...")
                # Retry logic here
                continue

            # Extract URLs from current page with retry logic
            urls = []
            for attempt in range(MAX_RETRIES):
                try:
                    urls = scraper.extract_page_data()
                    if urls:
                        break
                except Exception as e:
                    logger.error(f"[{key}] Page {page} extraction error (attempt {attempt+1}): {e}")

                if attempt < MAX_RETRIES - 1:
                    logger.debug(f"[{key}] Restarting browser and retrying page {page}")
                    scraper.restart_browser()
                    scraper.navigate(scraper.get_page_url(task, page))

            # Check for "no results" page - stop scraping if detected
            if scraper.is_no_results_page():
                logger.warning(f"[{key}] Page {page}/{total_pages}: 'No results' message detected - stopping chunk early")
                break

            # Queue URLs for Redis (non-blocking)
            if urls:
                # Put in queue instead of blocking on Redis write
                redis_queue.put((urls, metadata))

                with stats_lock:
                    total_stats['pages'] += 1

                logger.info(f"[{key}] Page {page}/{total_pages}: {len(urls)} URLs queued for Redis")
            else:
                with stats_lock:
                    total_stats['errors'] += 1
                logger.warning(f"[{key}] Page {page}/{total_pages}: No URLs extracted after {MAX_RETRIES} attempts")

            # Navigate to next page (if not last page)
            if page < total_pages:
                expected_next_page = page + 1

                if click_next_with_verification(scraper, expected_next_page):
                    logger.debug(f"[{key}] ✓ Clicked to page {expected_next_page}")
                else:
                    logger.warning(f"[{key}] Click failed, using direct URL for page {expected_next_page}")
                    next_page_url = scraper.get_page_url(task, expected_next_page)

                    # Try navigation with retries
                    nav_success = False
                    for attempt in range(MAX_NAV_RETRIES):
                        if scraper.navigate(next_page_url):
                            nav_success = True
                            break

                        logger.warning(f"[{key}] Navigation attempt {attempt+1}/{MAX_NAV_RETRIES} failed for page {expected_next_page}")

                        if attempt < MAX_NAV_RETRIES - 1:
                            logger.info(f"[{key}] Restarting browser and retrying...")
                            scraper.restart_browser()

                    if not nav_success:
                        logger.error(f"[{key}] Failed to navigate to page {expected_next_page} after {MAX_NAV_RETRIES} attempts, stopping task")
                        break

        logger.info(f"[{key}] Complete - scraped {page}/{total_pages} pages")

    except Exception as e:
        logger.info(f"[{key}] Failed: {e}")
    finally:
        if scraper:
            try:
                scraper.close_browser()
            except:
                pass

def main():
    global redis_clients  # CHANGED: Now multiple Redis clients

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

                futures = {f: futures[f] for f in not_done}
    
    except KeyboardInterrupt:
        shutdown_event.set()
    
    # CHANGED: No bulk publishing needed - URLs published in real-time to Redis Stream
    if not shutdown_event.is_set():
        # Just log stream statistics
        pending_count = redis_clients['url_stream'].get_pending_count()
        processed_count = len(redis_clients['processed_urls'].get_all_urls())
        logger.info(f"📊 Stream stats: {pending_count} pending URLs, {processed_count} total processed")
    
    # Cleanup
    try:
        # Close all Redis connections
        for name, client in redis_clients.items():
            client.close()
        logger.info("✅ All Redis connections closed")
    except:
        pass
    
    elapsed = time.time() - start_time

    print(f"\n{'='*60}\n SCRAPING COMPLETE\n{'='*60}")
    print(f" Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f" Tasks: {completed}/{len(tasks)} completed")
    print(f" Pages: {total_stats['pages']:,} | URLs: {total_stats['urls']:,}")
    print(f" New: {total_stats['new']:,} | Price Changes: {total_stats['price_changes']:,}")
    print(f" Duplicates: {total_stats['duplicates']:,} | Errors: {total_stats['errors']}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        sys.exit(1)