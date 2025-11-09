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
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from scraper_class import scrape_task_with_retry
from redis_client import create_redis_clients

# ============================================================================
# CONFIGURATION
# ============================================================================

SITE_NAME = "olx"
MAX_WORKERS = 1
MAX_PAGES_PER_TASK = 100  # OLX limit

# Redis connection settings
REDIS_HOST = "5.161.248.214"
REDIS_PORT = 6379
REDIS_PASSWORD = "redispass"

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

    Each worker creates its own Redis clients to avoid lock contention.

    Args:
        task: Task dict with location, prop_type, transaction_type, task_id
    """
    if shutdown_event.is_set():
        logger.info(f"[{task['task_id']}] Shutdown requested, skipping task")
        return

    worker_redis_clients = None

    try:
        # Create per-worker Redis clients
        logger.debug(f"[{task['task_id']}] Creating Redis clients for worker")
        worker_redis_clients = create_redis_clients(
            site_name=SITE_NAME,
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD
        )

        # Execute task with new retry workflow
        stats = scrape_task_with_retry(
            task=task,
            redis_clients=worker_redis_clients,
            max_pages=MAX_PAGES_PER_TASK
        )

        # Update global stats
        with stats_lock:
            global_stats['total_pages'] += stats['pages_scraped']
            global_stats['total_urls'] += stats['urls_found']
            global_stats['new_urls'] += stats['new_urls']
            global_stats['price_changes'] += stats['price_changes']
            global_stats['duplicates'] += stats['duplicates']

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

    finally:
        # Always cleanup worker's Redis connections
        if worker_redis_clients:
            try:
                for name, client in worker_redis_clients.items():
                    client.close()
                logger.debug(f"[{task['task_id']}] Redis clients closed")
            except Exception as e:
                logger.debug(f"[{task['task_id']}] Error closing Redis: {e}")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution flow with task distribution (per-worker Redis clients)"""

    print(f"\n{'='*60}")
    print(f" {SITE_NAME.upper()} SCRAPER - PRODUCTION WORKFLOW")
    print(f"{'='*60}")
    print(f" Workers: {MAX_WORKERS}")
    print(f" Locations: {len(LOCATIONS)}")
    print(f" Property Types: {len(PROPERTY_TYPES)}")
    print(f" Transaction Types: {len(TRANSACTION_TYPES)}")
    print(f" Max Pages per Task: {MAX_PAGES_PER_TASK}")
    print(f" Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"{'='*60}\n")

    start_time = time.time()

    # ========================================================================
    # SETUP: Test Redis connection
    # ========================================================================

    try:
        logger.info("Testing Redis connection...")
        test_client = create_redis_clients(
            site_name=SITE_NAME,
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD
        )
        # Close test client
        for client in test_client.values():
            client.close()
        logger.info("✅ Redis connection verified")

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

    # Log Redis stream stats (create temporary client)
    if not shutdown_event.is_set():
        try:
            temp_clients = create_redis_clients(
                site_name=SITE_NAME,
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD
            )
            pending_count = temp_clients['url_stream'].get_pending_count()
            processed_count = len(temp_clients['processed_urls'].get_all_urls())
            logger.info(f"📊 Stream stats: {pending_count} pending URLs, {processed_count} total processed")

            # Close temp client
            for client in temp_clients.values():
                client.close()
        except Exception as e:
            logger.debug(f"Could not get stream stats: {e}")

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
