import time
import logging
import threading
import os
import sys
import signal
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

# Import from local modules
from redis_client import create_redis_clients

# Site-specific configuration
SITE_NAME = 'dfimoveis'
REDIS_HOST = '5.161.248.214'
REDIS_PORT = 6379
REDIS_PASSWORD = 'redispass'

# --- Configuration ---

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(threadName)s] - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
for l in ["seleniumbase", "selenium", "pika"]: logging.getLogger(l).setLevel(logging.WARNING)

# Configuration
MAX_RETRIES = 3
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '3'))

# Worker ID
WORKER_ID = os.getpid()
FAILED_DIR = "failed_urls"

# Ensure directories exist
os.makedirs(FAILED_DIR, exist_ok=True)

# Thread-safe storage & stats
results_lock = threading.Lock()
total_stats = {
    'success': 0,
    'errors': 0,
    'retries': 0,
    'failed_permanent': 0,
    'captchas_solved': 0,
    'browser_restarts': 0,
    'offline': 0,
}

# NEW: Track all browser instances for force-close on shutdown
active_browsers = []
browsers_lock = threading.Lock()

# Global flag for graceful shutdown
shutdown_event = threading.Event()
force_shutdown = False  # NEW: Track force shutdown (second Ctrl+C)

# NEW: Function to force-close all browsers
def close_all_browsers():
    """Force-close all active browsers (called on shutdown)."""
    with browsers_lock:
        if not active_browsers:
            return
        
        logger.info(f"[SHUTDOWN] Force-closing {len(active_browsers)} browser(s)...")
        
        for browser in active_browsers:
            try:
                browser.driver.stop()
            except Exception as e:
                logger.debug(f"Error force-closing browser: {e}")
        
        active_browsers.clear()
        logger.info("[SHUTDOWN] All browsers closed")

# Signal handler for Ctrl+C
def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global force_shutdown
    
    if shutdown_event.is_set():
        # Second Ctrl+C - force shutdown
        logger.warning("\n[FORCE SHUTDOWN] Second Ctrl+C detected!")
        logger.warning("[FORCE SHUTDOWN] Closing all browsers and exiting immediately...")
        force_shutdown = True
        close_all_browsers()
        sys.exit(1)
    
    # First Ctrl+C - graceful shutdown
    shutdown_event.set()
    logger.info("\n[SHUTDOWN] Ctrl+C detected! Initiating graceful shutdown...")
    logger.info("[SHUTDOWN] Workers will finish current URL and exit")
    logger.info("[SHUTDOWN] Press Ctrl+C again to force quit immediately")
    
    # Close browsers after a timeout if workers don't finish
    def delayed_browser_close():
        time.sleep(10)  # Wait 10 seconds for graceful shutdown
        if not force_shutdown:
            logger.warning("[SHUTDOWN] Timeout reached - force-closing browsers")
            close_all_browsers()
    
    threading.Thread(target=delayed_browser_close, daemon=True).start()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- Captcha Handling Functions ---
def is_captcha_page(sb):
    """Check if current page is a captcha page"""
    try:
        title = sb.get_title()
        # Check for both Portuguese and English captcha titles
        captcha_title_keywords = ["Um momento", "Just a moment"]
        if any(keyword in title for keyword in captcha_title_keywords):
            return True
        page_source = sb.get_page_source()
        captcha_indicators = [
            "cf-challenge",
            "Verificando se você é humano",
            "Confirme que você é humano"
        ]
        for indicator in captcha_indicators:
            if indicator in page_source:
                return True
    except Exception as e:
        logger.debug(f"Error checking for captcha: {e}")
    return False

def handle_captcha(sb, max_wait: int = 15) -> bool:
    """
    Handle captcha with multiple fallback methods.

    Strategy:
    1. Wait for UC mode auto-solve (60s)
    2. If fails, try gui_click_captcha()
    3. If fails, try solve_captcha()

    Args:
        max_wait: Maximum time to wait for UC mode (seconds, default 60)

    Returns:
        True if captcha cleared
    """
    logger.warning("⚠️  Captcha detected! Trying UC mode auto-solve...")

    # Captcha keywords to check for both Portuguese and English
    captcha_title_keywords = ["Um momento", "Just a moment"]

    # Method 1: UC mode auto-solve (passive waiting)
    start_time = time.time()
    while time.time() - start_time < max_wait:
        # Check if title changed (captcha cleared)
        try:
            title = sb.get_title()
            # Check if any captcha keyword is still in title
            captcha_present = any(keyword in title for keyword in captcha_title_keywords)
            if not captcha_present:
                logger.info("✅ Captcha cleared by UC mode!")
                return True
        except:
            pass

        time.sleep(1)

    logger.warning("UC mode timeout - trying fallback methods...")

    # Method 2: gui_click_captcha()
    try:
        logger.info("Trying gui_click_captcha()...")
        sb.gui_click_captcha()
        time.sleep(3)  # Wait for captcha to process

        # Check if cleared
        title = sb.get_title()
        captcha_present = any(keyword in title for keyword in captcha_title_keywords)
        if not captcha_present:
            logger.info("✅ Captcha cleared by gui_click_captcha()!")
            return True
    except Exception as e:
        logger.debug(f"gui_click_captcha() failed: {str(e)[:100]}")

    # Method 3: solve_captcha()
    try:
        logger.info("Trying solve_captcha()...")
        sb.solve_captcha()
        time.sleep(3)  # Wait for captcha to process

        # Check if cleared
        title = sb.get_title()
        captcha_present = any(keyword in title for keyword in captcha_title_keywords)
        if not captcha_present:
            logger.info("✅ Captcha cleared by solve_captcha()!")
            return True
    except Exception as e:
        logger.debug(f"solve_captcha() failed: {str(e)[:100]}")

    logger.error("🚫 All captcha solving methods failed")
    return False

def is_listing_active(soup, url):
    """Check if the listing is active by confirming the main container exists."""
    try:
        # Check if offline modal is visible
        offline_modal = soup.select_one("#anuncioInativoModal.show")
        if offline_modal:
            logger.warning("Listing is offline (modal detected)")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error checking listing status: {e}")
        return True 

def parse_property_data(html, url):
    return html != None

# --- Worker Function ---
def persistent_worker(worker_id, redis_clients):
    """
    Worker that maintains ONE browser session.
    Pulls URLs from Redis Stream and processes them.
    """
    thread_name = f"Worker-{worker_id}"
    consumer_name = f"worker-{WORKER_ID}-{worker_id}"
    logger.info(f"[{thread_name}] 🚀 Starting persistent worker (consumer: {consumer_name})")

    sb = None
    urls_processed = 0

    # Unpack Redis clients
    url_stream = redis_clients['url_stream']
    processed_urls = redis_clients['processed_urls']
    failed_urls = redis_clients['failed_urls']
    airtable_tasks = redis_clients['airtable_tasks']

    try:
        sb = sb_cdp.Chrome(
            uc=True,
            locale="pt-br",
            headless=False,
            window_size="1920,1080",
            ad_block=True,
        )

        # Register browser for tracking
        with browsers_lock:
            active_browsers.append(sb)

        logger.info(f"[{thread_name}] ✅ Browser initialized")

        while not shutdown_event.is_set():
            url = None
            message_id = None
            retry_count = 0
            action = None

            # Get next URL from Redis Stream
            try:
                # Check shutdown before consuming
                if shutdown_event.is_set():
                    logger.info(f"[{thread_name}] 🛑 Shutdown detected, exiting")
                    break

                # Consume from stream (blocking with timeout)
                messages = url_stream.consume_urls(
                    consumer_name=consumer_name,
                    count=1,
                    block_ms=5000  # 5 second timeout
                )

                if not messages:
                    # No messages available, check again
                    continue

                # Unpack message
                message_id, url, fields = messages[0]
                action = fields.get('action', 'new')
                retry_count = int(fields.get('retry_count', 0))

            except Exception as e:
                if shutdown_event.is_set():
                    logger.debug(f"[{thread_name}] Expected error during shutdown: {e}")
                    break
                logger.error(f"[{thread_name}] ❌ Error consuming from stream: {e}")
                time.sleep(5)
                continue

            # Check shutdown before processing
            if shutdown_event.is_set():
                logger.info(f"[{thread_name}] 🛑 Shutdown detected, message stays in stream")
                break

            # Process this URL
            logger.info(f"[{thread_name}] Processing: {url} (Action: {action}, Retry: {retry_count})")
            url_start_time = time.time()

            try:
                sb.open(url)

                # Check shutdown after navigation
                if shutdown_event.is_set():
                    logger.info(f"[{thread_name}] 🛑 Shutdown during navigation, message stays in stream")
                    break

                # Check for captcha BEFORE verifying page load (improved flow)
                if is_captcha_page(sb):
                    logger.debug(f"[{thread_name}] Captcha detected after navigation")
                    if not handle_captcha(sb):
                        error_msg = "Captcha resolution failed"
                        logger.error(f"[{thread_name}] {error_msg} for {url}")

                        # ACK and retry or fail
                        url_stream.ack_message(message_id)
                        handle_retry_or_fail(url, error_msg, retry_count, url_stream, failed_urls)

                        # Restart browser after captcha failure
                        try:
                            sb.driver.stop()
                            with browsers_lock:
                                if sb in active_browsers:
                                    active_browsers.remove(sb)
                        except:
                            pass

                        try:
                            logger.info(f"[{thread_name}] 🔄 Restarting browser after captcha failure...")
                            sb = sb_cdp.Chrome(
                                uc=True,
                                locale="pt-br",
                                headless=False,
                                window_size="1920,1080",
                                ad_block=True,
                            )
                            with browsers_lock:
                                active_browsers.append(sb)
                            with results_lock:
                                total_stats['browser_restarts'] += 1
                            logger.info(f"[{thread_name}] ✅ Browser restarted successfully")
                        except Exception as restart_error:
                            logger.error(f"[{thread_name}] 🚨 Failed to restart browser: {restart_error}")
                            break

                        continue

                # Now get page source after captcha is cleared
                html = sb.get_page_source()
                soup = BeautifulSoup(html, 'lxml')

                # Check if listing is offline
                if not is_listing_active(soup, url):
                    logger.warning(f"[{thread_name}] 🛑 Listing is OFFLINE, removing: {url}")
                    url_stream.ack_message(message_id)  # ACK to remove from stream
                    processed_urls.remove_url(url)  # Remove from processed_urls
                    with results_lock:
                        total_stats['offline'] += 1
                    continue

                # Parse property data
                property_data = parse_property_data(html, url)

                if not property_data or not property_data.get("titulo"):
                    error_msg = "No valid data extracted"
                    logger.warning(f"[{thread_name}] {error_msg} for {url}")
                    sb.save_screenshot(os.path.join(FAILED_DIR, f"no_data_{time.time_ns()}.png"))

                    # ACK and retry or fail
                    url_stream.ack_message(message_id)
                    handle_retry_or_fail(url, error_msg, retry_count, url_stream, failed_urls)
                    continue

                # Save to Redis processed_urls Hash
                try:
                    processed_urls.update_full_data(url, property_data)

                    # Queue Airtable sync task for new URL
                    airtable_tasks.publish_task(
                        site=SITE_NAME,
                        action='add',
                        url=url
                    )
                except Exception as e:
                    error_msg = f"Redis save failed: {str(e)[:100]}"
                    logger.error(f"[{thread_name}] {error_msg}")

                    # ACK and retry or fail
                    url_stream.ack_message(message_id)
                    handle_retry_or_fail(url, error_msg, retry_count, url_stream, failed_urls)
                    continue

                # Success!
                url_elapsed = time.time() - url_start_time
                logger.info(f"[{thread_name}] ✅ Success for {url} in {url_elapsed:.2f}s")

                with results_lock:
                    total_stats['success'] += 1

                # ACK message
                url_stream.ack_message(message_id)

                urls_processed += 1

            except Exception as e:
                error_msg = f"Exception: {str(e)[:200]}"
                logger.error(f"[{thread_name}] ❌ Error processing {url}: {error_msg}")

                # ACK and retry or fail
                if message_id:
                    url_stream.ack_message(message_id)
                if url:
                    handle_retry_or_fail(url, error_msg, retry_count, url_stream, failed_urls)

                # Restart browser after error
                try:
                    sb.driver.stop()
                    with browsers_lock:
                        if sb in active_browsers:
                            active_browsers.remove(sb)
                except:
                    pass

                try:
                    logger.info(f"[{thread_name}] 🔄 Restarting browser...")
                    sb = sb_cdp.Chrome(
                        uc=True,
                        locale="pt-br",
                        headless=False,
                        window_size="1920,1080",
                        ad_block=True,
                    )
                    with browsers_lock:
                        active_browsers.append(sb)
                    with results_lock:
                        total_stats['browser_restarts'] += 1
                    logger.info(f"[{thread_name}] ✅ Browser restarted successfully")
                except Exception as restart_error:
                    logger.error(f"[{thread_name}] 🚨 Failed to restart browser: {restart_error}")
                    break

    finally:
        # Clean up browser
        if sb:
            try:
                logger.info(f"[{thread_name}] 🧹 Closing browser (processed {urls_processed} URLs)")
                sb.driver.stop()
                
                # Remove from tracking
                with browsers_lock:
                    if sb in active_browsers:
                        active_browsers.remove(sb)
                        
            except Exception as e:
                logger.debug(f"[{thread_name}] Error closing browser: {e}")
        
        logger.info(f"[{thread_name}] 👋 Worker exiting")

def handle_retry_or_fail(url, error_msg, retry_count, url_stream, failed_urls):
    """Handle a failed URL with retry logic (max 3 retries)"""
    if shutdown_event.is_set():
        return  # Don't retry during shutdown

    with results_lock:
        total_stats['errors'] += 1

        if retry_count < MAX_RETRIES:
            # Retry: re-publish to stream with incremented retry count
            try:
                url_stream.client.xadd(
                    url_stream.stream_key,
                    {
                        'url': url,
                        'action': 'retry',
                        'retry_count': str(retry_count + 1),
                        'timestamp': str(time.time())
                    }
                )
                total_stats['retries'] += 1
                logger.info(f"🔄 Retry {retry_count + 1}/{MAX_RETRIES} for {url}: {error_msg}")
            except Exception as e:
                logger.error(f"Error re-publishing to stream: {e}")
        else:
            # Failed permanently: store in failed_urls
            try:
                failed_urls.add_failed_url(url, error_msg)
                total_stats['failed_permanent'] += 1
                logger.error(f"🚫 Max retries reached for {url}, stored in failed_urls")
            except Exception as e:
                logger.error(f"Error storing failed URL: {e}")

# --- Main Thread ---

if __name__ == "__main__":
    start_time = time.time()

    logger.info("Connecting to Redis...")

    try:
        redis_clients = create_redis_clients(
            site_name=SITE_NAME,
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD
        )
        logger.info("✅ Connected to Redis")
        logger.info(f"   Stream: {redis_clients['url_stream'].stream_key}")
        logger.info(f"   Consumer Group: {redis_clients['url_stream'].consumer_group}")
        logger.info(f"   Processed URLs: {len(redis_clients['processed_urls'].get_all_urls()):,}")
        logger.info(f"   Failed URLs: {redis_clients['failed_urls'].get_failed_count()}")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")
        logger.error("Cannot proceed without Redis. Exiting.")
        exit(1)

    logger.info("=" * 60)
    logger.info(f"Starting DFImoveis Redis Stream Processor")
    logger.info(f"Worker ID: {WORKER_ID} | Max Workers: {MAX_WORKERS}")
    logger.info("=" * 60)

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for worker_id in range(MAX_WORKERS):
                future = executor.submit(
                    persistent_worker,
                    worker_id,
                    redis_clients
                )
                futures.append(future)

            # Wait for workers with timeout check
            for future in futures:
                try:
                    # Wait with timeout to allow checking shutdown
                    while not future.done():
                        if shutdown_event.is_set():
                            break
                        time.sleep(0.5)

                    if not shutdown_event.is_set():
                        future.result(timeout=1)

                except Exception as e:
                    if not shutdown_event.is_set():
                        logger.error(f"Worker failed with exception: {e}")

    except KeyboardInterrupt:
        pass  # Already handled by signal handler

    finally:
        # Ensure all browsers are closed
        close_all_browsers()

    elapsed = time.time() - start_time

    try:
        for name, client in redis_clients.items():
            client.close()
        logger.info("✅ Redis connections closed")
    except Exception as e:
        logger.debug(f"Error closing Redis: {e}")

    logger.info("=" * 60)
    logger.info("PROCESSING COMPLETE" if not shutdown_event.is_set() else "PROCESSING STOPPED")
    logger.info("=" * 60)
    logger.info(f"Time elapsed:           {elapsed:.2f}s ({elapsed/60:.1f} min)")
    logger.info(f"Workers used:           {MAX_WORKERS}")
    logger.info(f"Worker ID:              {WORKER_ID}")
    logger.info(f"Successfully processed: {total_stats['success']}")
    logger.info(f"Offline/Removed:        {total_stats['offline']}")
    logger.info(f"Retries:                {total_stats['retries']}")
    logger.info(f"Failed permanently:     {total_stats['failed_permanent']}")
    logger.info(f"Total errors:           {total_stats['errors']}")
    logger.info(f"Captchas solved:        {total_stats['captchas_solved']}")
    logger.info(f"Browser restarts:       {total_stats['browser_restarts']}")
    logger.info("=" * 60)