#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OLX Scraper - Main Workflow with Task Distribution
Uses new scraper_class.py with retry logic and state-based navigation
"""
import time
import logging
import signal
import sys
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from scraper_class import scrape_task_with_retry
from redis_client import create_redis_clients

# ============================================================================
# CONFIGURATION
# ============================================================================

SITE_NAME = "olx"
MAX_WORKERS = 1
MAX_PAGES_PER_TASK = 100  # OLX limit

# Location configurations
LOCATIONS = [
    # "",
    "/distrito-federal-e-regiao",
    "/distrito-federal-e-regiao/brasilia",
    # "/distrito-federal-e-regiao/outras-cidades",
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
for lib in ["seleniumbase", "selenium", "pika", "urllib3"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

# ============================================================================
# GLOBALS
# ============================================================================

shutdown_event = threading.Event()
redis_clients = None
stats_lock = threading.Lock()
global_stats = {
    'tasks_completed': 0,
    'tasks_failed': 0,
    'tasks_no_results': 0,
    'total_pages': 0,
    'total_urls': 0,
    'new_urls': 0,
    'price_changes': 0,
    'duplicates': 0
}

# Redis writer queue - decouples network I/O from scraping
redis_queue = queue.Queue(maxsize=1000)  # Buffer up to 1000 batches

# ============================================================================
# SIGNAL HANDLING
# ============================================================================

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    logger.info("Shutdown requested, finishing current tasks...")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================================
# REDIS WRITER WORKER
# ============================================================================

def redis_writer_worker():
    """
    Background thread that consumes URL batches from queue and writes to Redis.

    This decouples Redis network I/O from scraper threads, allowing scrapers
    to continue immediately after extraction without blocking on Redis writes.

    Workflow:
    1. Scraper extracts URLs → puts (url_price_pairs, metadata) in queue
    2. This worker pulls from queue → writes to Redis
    3. Updates global stats with results
    """
    logger.info("🔌 Redis writer thread started")

    while not shutdown_event.is_set():
        try:
            # Get batch from queue (timeout so we can check shutdown)
            batch_data = redis_queue.get(timeout=1.0)

            if batch_data is None:  # Poison pill for shutdown
                break

            url_price_pairs, metadata = batch_data

            # Write to Redis (this blocks, but doesn't block scrapers!)
            try:
                # Call the Redis deduplication logic
                batch_stats = _store_urls_to_redis(url_price_pairs, metadata)

                # Update global stats
                num_urls = len(url_price_pairs)
                with stats_lock:
                    global_stats['total_urls'] += num_urls
                    global_stats['new_urls'] += batch_stats['new']
                    global_stats['price_changes'] += batch_stats['price_changes']
                    global_stats['duplicates'] += batch_stats['duplicates']

                logger.info(f"✓ Redis wrote {num_urls} URLs: "
                           f"🆕{batch_stats['new']} 💰{batch_stats['price_changes']} "
                           f"🔄{batch_stats['duplicates']}")

            except Exception as e:
                logger.error(f"Redis write error: {e}")

            redis_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Redis writer unexpected error: {e}")

    logger.info("🔌 Redis writer thread stopped")


def _store_urls_to_redis(url_price_pairs: list, metadata: dict = None) -> dict:
    """
    Store URLs to Redis with deduplication and price change detection.

    This is called by the Redis writer thread, not by scrapers directly.

    Deduplication logic:
    1. Check scrape_session (within-run deduplication)
    2. Check processed_urls (cross-run deduplication + price changes)
    3. Publish to url_stream + airtable_tasks as needed

    Args:
        url_price_pairs: List of (url, price) tuples
        metadata: Optional metadata dict (location, prop_type, etc.)

    Returns:
        Dict with keys: new, price_changes, duplicates
    """
    if not url_price_pairs:
        return {'new': 0, 'price_changes': 0, 'duplicates': 0}

    # Normalize prices: Convert None to 0 (Redis doesn't accept None values)
    normalized_pairs = []
    none_count = 0
    for url, price in url_price_pairs:
        if price is None:
            normalized_pairs.append((url, 0))
            none_count += 1
        else:
            normalized_pairs.append((url, price))

    if none_count > 0:
        logger.debug(f"⚠️ {none_count} URLs had no price, storing as 0")

    # Build dict for batch operations
    url_price_dict = {url: price for url, price in normalized_pairs}

    # Step 1: Check scrape_session (HSETNX batch - only adds if new)
    session_results = redis_clients['scrape_session'].add_urls_if_new_batch(url_price_dict)

    # Step 2: For URLs new to this session, check processed_urls
    new_in_session = [url for url, is_new in session_results.items() if is_new]

    if not new_in_session:
        # All duplicates within current session
        return {'new': 0, 'price_changes': 0, 'duplicates': len(url_price_pairs)}

    # Batch lookup in processed_urls
    existing_processed = redis_clients['processed_urls'].get_urls_batch(new_in_session)

    # Classify URLs
    new_urls = []
    price_changed_urls = []
    stream_publish_list = []
    airtable_publish_list = []

    for url in new_in_session:
        price = url_price_dict[url]

        if url not in existing_processed:
            # Completely new URL
            new_urls.append(url)

            # Update processed_urls with initial data
            redis_clients['processed_urls'].update_url_price(url, price, metadata)

            # Publish to streams
            stream_publish_list.append((url, 'process'))
            airtable_publish_list.append(('add', url))

        else:
            # URL exists, check price change
            old_price = existing_processed[url].get('price')

            if old_price is None or old_price != price:
                price_changed_urls.append(url)

                # Update price in processed_urls
                redis_clients['processed_urls'].update_url_price(url, price, metadata)

                # Publish price change tasks
                stream_publish_list.append((url, 'process'))
                airtable_publish_list.append(('update', url))

    # Batch publish to Redis Stream
    if stream_publish_list:
        redis_clients['url_stream'].publish_urls_batch(stream_publish_list)

    # Batch publish to Airtable tasks
    if airtable_publish_list:
        for action, url in airtable_publish_list:
            redis_clients['airtable_tasks'].publish_task(
                site=redis_clients['scrape_session'].site_name,
                action=action,
                url=url
            )

    # Calculate duplicates (URLs already in session)
    duplicates = len(normalized_pairs) - len(new_in_session)

    return {
        'new': len(new_urls),
        'price_changes': len(price_changed_urls),
        'duplicates': duplicates
    }

# ============================================================================
# TASK GENERATION
# ============================================================================

def generate_tasks() -> list:
    """
    Generate task list from configuration.

    Each task represents a complete scraping job for:
    - One location
    - One property type
    - One transaction type

    Returns:
        List of task dicts with keys:
        - location: URL location path
        - prop_type: Property type (apartamentos/casas)
        - transaction_type: Transaction type (venda/aluguel)
        - task_id: Unique identifier for the task
    """
    logger.info("="*60)
    logger.info("📋 GENERATING TASKS")
    logger.info("="*60)

    tasks = []

    for location in LOCATIONS:
        for prop_type in PROPERTY_TYPES:
            for transaction_type in TRANSACTION_TYPES:
                # Use "base" for empty location to avoid leading underscore
                location_name = location if location else "base"

                task = {
                    'location': location,  # Keep original (empty string) for URL building
                    'prop_type': prop_type,
                    'transaction_type': transaction_type,
                    'task_id': f"{location_name}_{prop_type}_{transaction_type}"
                }
                tasks.append(task)

    logger.info(f"✓ Generated {len(tasks)} tasks")
    logger.info(f"  Locations: {len(LOCATIONS)}")
    logger.info(f"  Property Types: {len(PROPERTY_TYPES)}")
    logger.info(f"  Transaction Types: {len(TRANSACTION_TYPES)}")
    logger.info("="*60)

    return tasks

# ============================================================================
# TASK EXECUTION WRAPPER
# ============================================================================

def execute_task(task: dict):
    """
    Wrapper function to execute a single task and update global stats.

    Args:
        task: Task dict with location, prop_type, transaction_type, task_id
    """
    if shutdown_event.is_set():
        logger.info(f"[{task['task_id']}] Shutdown requested, skipping task")
        return

    try:
        # Execute task with new retry workflow + queue for async Redis writes
        stats = scrape_task_with_retry(
            task=task,
            redis_clients=redis_clients,
            max_pages=MAX_PAGES_PER_TASK,
            redis_queue=redis_queue  # Pass queue for async writes
        )

        # Update global stats (URLs stats handled by writer thread)
        with stats_lock:
            global_stats['total_pages'] += stats['pages_scraped']
            # Note: total_urls, new_urls, price_changes, duplicates are updated by writer thread

            if stats['status'] == 'success':
                global_stats['tasks_completed'] += 1
            elif stats['status'] == 'no_results':
                global_stats['tasks_no_results'] += 1
            elif stats['status'] == 'failed':
                global_stats['tasks_failed'] += 1

        logger.info(f"[{task['task_id']}] Task complete: {stats['status']}")

    except Exception as e:
        logger.error(f"[{task['task_id']}] Task exception: {str(e)[:200]}")
        with stats_lock:
            global_stats['tasks_failed'] += 1

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution flow with Redis setup and task distribution"""
    global redis_clients

    print(f"\n{'='*60}")
    print(f" {SITE_NAME.upper()} SCRAPER - PRODUCTION WORKFLOW")
    print(f"{'='*60}")
    print(f" Workers: {MAX_WORKERS}")
    print(f" Locations: {len(LOCATIONS)}")
    print(f" Property Types: {len(PROPERTY_TYPES)}")
    print(f" Transaction Types: {len(TRANSACTION_TYPES)}")
    print(f" Max Pages per Task: {MAX_PAGES_PER_TASK}")
    print(f"{'='*60}\n")

    start_time = time.time()

    # ========================================================================
    # SETUP: Connect to Redis
    # ========================================================================

    try:
        logger.info("Connecting to Redis...")
        redis_clients = create_redis_clients(
            site_name=SITE_NAME,
            host="5.161.248.214",  # Your Redis host
            port=6379,
            password="redispass"   # Your Redis password
        )

        # Optional: Clear previous scrape session
        # redis_clients['scrape_session'].clear_session()
        logger.info("✅ Redis clients connected")

    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        sys.exit(1)

    # ========================================================================
    # START REDIS WRITER THREAD
    # ========================================================================

    redis_writer_thread = threading.Thread(
        target=redis_writer_worker,
        daemon=True,
        name="RedisWriter"
    )
    redis_writer_thread.start()
    logger.info("✅ Redis writer thread started")

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

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            futures = {executor.submit(execute_task, task): task for task in tasks}

            # Wait for completion
            while futures:
                if shutdown_event.is_set():
                    logger.info("Cancelling remaining tasks...")
                    for f in futures:
                        f.cancel()
                    break

                done, not_done = wait(futures, timeout=1.0, return_when=FIRST_COMPLETED)

                for f in done:
                    try:
                        f.result()
                        with stats_lock:
                            completed = global_stats['tasks_completed'] + global_stats['tasks_failed'] + global_stats['tasks_no_results']
                        logger.info(f"Progress: {completed}/{len(tasks)} tasks processed")
                    except Exception as e:
                        logger.error(f"Task execution error: {e}")

                # Update futures dict
                futures = {f: futures[f] for f in not_done}

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
        shutdown_event.set()

    # ========================================================================
    # CLEANUP & STATS
    # ========================================================================

    # Wait for Redis writer queue to drain
    queue_size = redis_queue.qsize()
    if queue_size > 0:
        logger.info(f"⏳ Waiting for Redis queue to drain ({queue_size} batches pending)...")
        redis_queue.join()  # Blocks until all items processed
        logger.info("✅ Redis queue drained")

    # Stop Redis writer thread gracefully
    redis_queue.put(None)  # Poison pill
    redis_writer_thread.join(timeout=10)
    if redis_writer_thread.is_alive():
        logger.warning("Redis writer thread did not stop cleanly")
    else:
        logger.info("✅ Redis writer thread stopped")

    # Log Redis stream stats
    if not shutdown_event.is_set():
        try:
            pending_count = redis_clients['url_stream'].get_pending_count()
            processed_count = len(redis_clients['processed_urls'].get_all_urls())
            logger.info(f"📊 Stream stats: {pending_count} pending URLs, {processed_count} total processed")
        except Exception as e:
            logger.debug(f"Could not get stream stats: {e}")

    # Close Redis connections
    try:
        for name, client in redis_clients.items():
            client.close()
        logger.info("✅ All Redis connections closed")
    except Exception as e:
        logger.debug(f"Error closing Redis: {e}")

    elapsed = time.time() - start_time

    # ========================================================================
    # FINAL REPORT
    # ========================================================================

    print(f"\n{'='*60}")
    print(f" SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f" Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f" Tasks: {global_stats['tasks_completed']} succeeded, "
          f"{global_stats['tasks_failed']} failed, "
          f"{global_stats['tasks_no_results']} no results")
    print(f" Pages: {global_stats['total_pages']:,}")
    print(f" URLs: {global_stats['total_urls']:,}")
    print(f" New: {global_stats['new_urls']:,}")
    print(f" Price Changes: {global_stats['price_changes']:,}")
    print(f" Duplicates: {global_stats['duplicates']:,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
