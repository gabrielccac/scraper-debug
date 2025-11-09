#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
DFImoveis Worker - Property Detail Consumer
Processes URLs from Redis stream and extracts property details
"""
import time
import re
import sys
import json
import logging
import os
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Suppress noisy libraries
for lib in ["seleniumbase", "selenium", "urllib3"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

# Import parser for data extraction
try:
    from parser import parse_property_data
    logger.debug("Parser module loaded successfully")
except ImportError:
    logger.warning("Parser module not found - using mock parser")
    def parse_property_data(soup, url, html):
        return {"url": url, "titulo": "Mock Data"}


# ============================================================================
# CONFIGURATION
# ============================================================================

# Site configuration
SITE_NAME = 'dfimoveis'
MAX_RETRIES = 3

# ============================================================================
# PAGE STATE CONSTANTS
# ============================================================================

class PageState:
    """Page state constants for property page results"""
    SUCCESS = "success"           # Property details loaded successfully
    OFFLINE = "offline"           # Listing expired/removed
    CAPTCHA = "captcha"           # Captcha encountered
    TIMEOUT = "timeout"           # Page failed to load
    UNKNOWN = "unknown"           # Loaded but unexpected state


class DfimoveisWorker:
    """
    Worker for processing DFimoveis property detail pages.
    Consumes URLs from queue, extracts property details, stores in DB.
    """

    # ========================================================================
    # CONSTANTS - Site Configuration
    # ========================================================================

    SITE_NAME = "dfimoveis"

    # Selectors (to be defined as we implement)
    PROPERTY_LOADED_SELECTOR = 'div.info-section '  # TODO: Update with actual selector
    OFFLINE_SELECTOR = '#anuncioInativoModal.show'  # TODO: Verify actual message

    # Captcha detection patterns
    CAPTCHA_TITLE_KEYWORDS = ["Um momento", "Just a moment"]
    CAPTCHA_TEXT_KEYWORDS = ["Confirme que você é humano", "cf-challenge"]

    # Settings
    LOAD_TIMEOUT = 10
    BROWSER_LOCALE = "pt-br"

    # ========================================================================
    # INITIALIZATION
    # ========================================================================

    def __init__(self, redis_clients: dict = None):
        """
        Initialize worker instance.

        Args:
            redis_clients: Dict with Redis clients for queue and storage
        """
        self.sb = None
        self.debug_dir = "debug_urls"
        os.makedirs(self.debug_dir, exist_ok=True)

        # Redis integration
        if redis_clients:
            self.url_stream = redis_clients.get('url_stream')
            self.processed_urls = redis_clients.get('processed_urls')
            self.failed_urls = redis_clients.get('failed_urls')
            self.airtable_tasks = redis_clients.get('airtable_tasks')
            logger.debug("DfimoveisWorker instance created with Redis clients")
        else:
            self.url_stream = None
            self.processed_urls = None
            self.failed_urls = None
            self.airtable_tasks = None
            logger.debug("DfimoveisWorker instance created without Redis (mock mode)")

    # ========================================================================
    # BROWSER MANAGEMENT - TODO
    # ========================================================================

    def init_browser(self, max_retries: int = 3):
        """
        Initialize SeleniumBase browser instance with retry logic.

        Args:
            max_retries: Maximum number of retry attempts (default 3)
        """
        for attempt in range(max_retries):
            try:
                self.sb = sb_cdp.Chrome(
                    uc=True,
                    headless=False,
                    locale=self.BROWSER_LOCALE,
                    window_size="1920,1080",
                )
                logger.debug(f"Browser initialized successfully (attempt {attempt + 1}/{max_retries})")
                return True

            except Exception as e:
                logger.warning(f"Browser init attempt {attempt + 1}/{max_retries} failed: {str(e)[:100]}")

                # Cleanup failed browser instance before retry
                try:
                    if self.sb:
                        self.sb.driver.quit()
                        self.sb = None
                except:
                    pass

                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = 1.5 ** attempt
                    logger.info(f"Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to initialize browser after {max_retries} attempts")
                    raise

        return False

    def close_browser(self):
        """Safely close browser instance."""
        try:
            if self.sb:
                self.sb.driver.stop()
                logger.debug("Browser closed")
        except Exception as e:
            logger.debug(f"Error closing browser: {str(e)[:100]}")

    def restart_browser(self) -> bool:
        """
        Close and reinitialize browser.

        Returns:
            True if restart successful
        """
        try:
            logger.info("Restarting browser...")

            # Close existing browser
            self.close_browser()
            time.sleep(1)

            # Reinitialize browser
            self.init_browser()

            logger.info("Browser restarted successfully")
            return True

        except Exception as e:
            logger.error(f"Browser restart failed: {str(e)[:100]}")
            return False

    def save_debug_snapshot(self, reason: str) -> str:
        """
        Save screenshot and URL for debugging.

        Args:
            reason: Reason for snapshot (e.g., "timeout", "captcha", "offline")

        Returns:
            Path to saved screenshot
        """
        try:
            if not self.sb:
                logger.warning("No browser instance to snapshot")
                return ""

            # Get current URL
            try:
                current_url = self.sb.get_current_url()
            except:
                current_url = "unknown"

            # Create filename with timestamp and reason
            timestamp = int(time.time())
            safe_url = current_url.replace('https://', '').replace('http://', '').replace('/', '_')[:50]
            filename = f"{reason}_{timestamp}_{safe_url}.png"
            filepath = os.path.join(self.debug_dir, filename)

            # Save screenshot
            self.sb.save_screenshot(filepath)

            # Save URL to text file
            url_file = filepath.replace('.png', '.txt')
            with open(url_file, 'w') as f:
                f.write(f"Reason: {reason}\n")
                f.write(f"URL: {current_url}\n")
                f.write(f"Timestamp: {timestamp}\n")

            logger.info(f"📸 Debug snapshot saved: {filename}")
            logger.info(f"   URL: {current_url}")

            return filepath

        except Exception as e:
            logger.error(f"Failed to save debug snapshot: {str(e)[:100]}")
            return ""

    # ========================================================================
    # NAVIGATION & PAGE STATE CHECKING - TODO
    # ========================================================================

    def _wait_for_base_load(self, timeout: int = 10) -> bool:
        """
        Ensure basic page load before checking page state using JS evaluation.

        Verifies:
        - Body element exists
        - Not on about:blank
        - Page title exists and not empty
        - Not a captcha page

        Args:
            timeout: Time to wait for base load

        Returns:
            True if page has basic content loaded
        """
        # TODO: Implement JS evaluation polling (same pattern as scraper)
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # Check if body exists via JS
                body_exists = self.sb.evaluate("return document.body !== null")

                # Check URL is not about:blank via JS
                current_url = self.sb.evaluate("return window.location.href")
                url_loaded = current_url and "about:blank" not in current_url.lower()

                # Check if title is loaded via JS
                title = self.sb.evaluate("return document.title")
                title_loaded = title and len(title) > 0 and title.lower() != "about:blank"

                # Check if it's a captcha page
                is_captcha = False
                if title:
                    for keyword in self.CAPTCHA_TITLE_KEYWORDS:
                        if keyword in title:
                            is_captcha = True
                            break

                # All conditions met and not captcha
                if body_exists and url_loaded and title_loaded and not is_captcha:
                    logger.debug(f"Base page loaded: {title[:50]}")
                    return True

            except Exception as e:
                # Continue waiting if evaluation fails
                logger.debug(f"Base load evaluation failed: {str(e)[:100]}")

            time.sleep(0.1)  # Poll every 100ms

        logger.debug(f"Base load timeout after {timeout}s")
        return False

    def check_captcha_page(self) -> bool:
        """
        Check if current page is a captcha challenge.

        Returns:
            True if captcha detected
        """
        # TODO: Implement
        try:
            # Check page title
            title = self.sb.get_title()
            for keyword in self.CAPTCHA_TITLE_KEYWORDS:
                if keyword in title:
                    logger.debug(f"Captcha detected in title: '{title}'")
                    return True

            # Check page source
            page_source = self.sb.get_page_source()
            for keyword in self.CAPTCHA_TEXT_KEYWORDS:
                if keyword in page_source:
                    logger.debug(f"Captcha detected in page source: '{keyword}'")
                    return True

            return False

        except Exception as e:
            logger.debug(f"Error checking captcha: {str(e)[:100]}")
            return False

    def check_offline_page(self) -> bool:
        """
        Check if listing is offline/expired/removed using selector.

        Returns:
            True if offline message detected
        """
        try:
            # Check for offline modal using selector
            offline_element = self.sb.evaluate(
                f"return document.querySelector('{self.OFFLINE_SELECTOR}') !== null"
            )

            if offline_element:
                logger.debug("Offline listing detected via selector")
                return True

            return False

        except Exception as e:
            logger.debug(f"Error checking offline: {str(e)[:100]}")
            return False

    def handle_captcha(self, max_wait: int = 30) -> bool:
        """
        Handle captcha with multiple strategies.

        Strategy:
        1. Wait for UC mode auto-solve (30s)
        2. Try gui_click_captcha()
        3. Try solve_captcha()

        Args:
            max_wait: Maximum time to wait for UC mode (default 30s)

        Returns:
            True if captcha cleared
        """
        # TODO: Implement (can reuse from scraper)
        logger.warning("⚠️  Captcha detected! Trying UC mode auto-solve...")

        # Method 1: UC mode auto-solve
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                title = self.sb.get_title()
                captcha_present = any(keyword in title for keyword in self.CAPTCHA_TITLE_KEYWORDS)
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
            self.sb.gui_click_captcha()
            time.sleep(3)

            title = self.sb.get_title()
            captcha_present = any(keyword in title for keyword in self.CAPTCHA_TITLE_KEYWORDS)
            if not captcha_present:
                logger.info("✅ Captcha cleared by gui_click_captcha()!")
                return True
        except Exception as e:
            logger.debug(f"gui_click_captcha() failed: {str(e)[:100]}")

        # Method 3: solve_captcha()
        try:
            logger.info("Trying solve_captcha()...")
            self.sb.solve_captcha()
            time.sleep(3)

            title = self.sb.get_title()
            captcha_present = any(keyword in title for keyword in self.CAPTCHA_TITLE_KEYWORDS)
            if not captcha_present:
                logger.info("✅ Captcha cleared by solve_captcha()!")
                return True
        except Exception as e:
            logger.debug(f"solve_captcha() failed: {str(e)[:100]}")

        logger.error("🚫 All captcha solving methods failed")
        return False

    def get_page_state(self, base_load_timeout: int = 5) -> str:
        """
        Determine current property page state after navigation.

        Performs ordered checks:
        1. Base load (body + title)
        2. Captcha (handle if detected)
        3. Offline/expired listing
        4. Property details loaded

        Args:
            base_load_timeout: Timeout for base load check (default 5s)

        Returns:
            PageState constant indicating current state
        """
        # TODO: Implement ordered state checking
        # 1. Wait for basic page load
        if not self._wait_for_base_load(timeout=base_load_timeout):
            logger.warning("Base page load timeout")
            return PageState.TIMEOUT

        # 2. Check captcha (FIRST - blocks everything else)
        if self.check_captcha_page():
            logger.warning("Captcha detected")
            if self.handle_captcha():
                logger.info("Captcha solved, re-checking page state")
                # Recursively check page state after solving captcha
                return self.get_page_state(base_load_timeout)
            else:
                return PageState.CAPTCHA

        # 3. Check offline/expired
        if self.check_offline_page():
            logger.info("Offline listing detected")
            return PageState.OFFLINE

        # 4. Check property details loaded (TODO: implement selector check)
        # For now, assume SUCCESS if we got here
        logger.debug("✓ Property page loaded")
        return PageState.SUCCESS

    def goto_url(self, url: str) -> str:
        """
        Navigate to property URL and determine page state.

        Args:
            url: Property URL to navigate to

        Returns:
            PageState constant indicating result
        """
        # TODO: Implement navigation with state checking
        logger.info(f"Navigating to: {url}")

        try:
            # Navigate
            self.sb.get(url)
            logger.debug("Navigation command sent")

            # Check page state
            state = self.get_page_state(base_load_timeout=10)

            # logger.info(f"✓ Navigation complete: {state}")
            return state

        except Exception as e:
            logger.error(f"Navigation error: {str(e)[:100]}")
            return PageState.TIMEOUT

    # ========================================================================
    # DATA EXTRACTION - TODO
    # ========================================================================

    def parse_property(self) -> dict:
        """
        Parse property details from current page HTML.

        Returns:
            Dict with property details, or None if parsing fails
        """
        # TODO: Implement property detail extraction
        try:
            # Get page HTML via JS
            logger.debug("Getting page HTML via JS...")
            page_html = self.sb.evaluate("return document.documentElement.outerHTML")

            # Parse with BeautifulSoup
            soup = BeautifulSoup(page_html, 'lxml')

            # TODO: Extract property details
            # - Title
            # - Price
            # - Description
            # - Address/Location
            # - Features (bedrooms, bathrooms, etc.)
            # - Images
            # - Contact info

            property_data = {
                'url': self.sb.get_current_url(),
                'title': None,  # TODO
                'price': None,  # TODO
                'description': None,  # TODO
                # ... more fields
            }

            logger.debug(f"Parsed property data")
            return property_data

        except Exception as e:
            logger.error(f"Error parsing property: {str(e)[:100]}")
            return None

    # ========================================================================
    # QUEUE MANAGEMENT - Redis Stream Integration
    # ========================================================================

    def consume_next_url(self, consumer_name: str = "worker-1") -> tuple:
        """
        Consume next URL from Redis stream.

        Args:
            consumer_name: Name of this worker consumer

        Returns:
            Tuple of (message_id, url, retry_count, action) or (None, None, 0, None) if queue is empty
        """
        try:
            if not self.url_stream:
                logger.warning("No URL stream client configured")
                return None, None, 0, None

            # Try to consume one URL (block for 5 seconds)
            messages = self.url_stream.consume_urls(
                consumer_name=consumer_name,
                count=1,
                block_ms=5000
            )

            if messages:
                message_id, url, fields = messages[0]
                action = fields.get('action', 'process')
                retry_count = int(fields.get('retry_count', 0))

                logger.debug(f"📥 Consumed URL: {url[:80]} (Action: {action}, Retry: {retry_count})")
                return message_id, url, retry_count, action
            else:
                # No messages available
                return None, None, 0, None

        except Exception as e:
            logger.error(f"Error consuming from stream: {str(e)[:100]}")
            return None, None, 0, None

    def ack_url(self, message_id: str):
        """
        Acknowledge URL processing completion.

        Args:
            message_id: Message ID from Redis stream
        """
        try:
            if self.url_stream and message_id:
                self.url_stream.ack_message(message_id)
                logger.debug(f"✓ ACK message: {message_id}")
        except Exception as e:
            logger.error(f"Error acknowledging message: {str(e)[:100]}")

    def handle_retry_or_fail(self, url: str, error_msg: str, retry_count: int):
        """
        Handle a failed URL with retry logic (max 3 retries).

        Args:
            url: Failed URL
            error_msg: Error message
            retry_count: Current retry count
        """
        try:
            if retry_count < MAX_RETRIES:
                # Retry: re-publish to stream with incremented retry count
                self.url_stream.client.xadd(
                    self.url_stream.stream_key,
                    {
                        'url': url,
                        'action': 'retry',
                        'retry_count': str(retry_count + 1),
                        'timestamp': str(time.time())
                    }
                )
                logger.info(f"🔄 Retry {retry_count + 1}/{MAX_RETRIES} for {url[:80]}: {error_msg}")
            else:
                # Failed permanently: store in failed_urls
                if self.failed_urls:
                    self.failed_urls.add_failed_url(url, error_msg)
                logger.error(f"🚫 Max retries reached for {url[:80]}, stored in failed_urls")
        except Exception as e:
            logger.error(f"Error handling retry/fail: {str(e)[:100]}")

    # ========================================================================
    # WORKFLOW - TODO
    # ========================================================================

    def process_url(self, url: str, message_id: str = None, retry_count: int = 0) -> bool:
        """
        Process a single property URL.

        Workflow:
        1. Navigate to URL
        2. Check page state
        3. If SUCCESS: parse property details and save to Redis
        4. ACK message on success
        5. Handle retries on failure

        Args:
            url: Property URL to process
            message_id: Redis stream message ID (for acknowledgment)
            retry_count: Current retry count

        Returns:
            True if processed successfully (including offline listings)
        """
        logger.info(f"Processing: {url[:80]} (Retry: {retry_count})")

        try:
            # Navigate and get state
            state = self.goto_url(url)

            if state == PageState.SUCCESS:
                # Get page HTML for parser
                html = self.sb.get_page_source()
                soup = BeautifulSoup(html, 'lxml')

                # Parse property details using parser module
                property_data = parse_property_data(soup, url, html)

                if not property_data or not property_data.get("titulo"):
                    error_msg = "No valid data extracted"
                    logger.warning(f"❌ [PARSE_FAILED] {error_msg}")
                    self.save_debug_snapshot("no_data")

                    # ACK and retry or fail
                    if message_id:
                        self.ack_url(message_id)
                    self.handle_retry_or_fail(url, error_msg, retry_count)
                    return False

                # Save to Redis processed_urls
                if self.processed_urls:
                    try:
                        self.processed_urls.update_full_data(url, property_data)
                        logger.debug(f"   Saved to processed_urls")
                    except Exception as e:
                        error_msg = f"Redis save failed: {str(e)[:100]}"
                        logger.error(f"❌ {error_msg}")

                        # ACK and retry or fail
                        if message_id:
                            self.ack_url(message_id)
                        self.handle_retry_or_fail(url, error_msg, retry_count)
                        return False

                # Queue Airtable sync task
                if self.airtable_tasks:
                    try:
                        self.airtable_tasks.publish_task(
                            site=SITE_NAME,
                            action='add',
                            url=url
                        )
                        logger.debug(f"   Queued Airtable sync")
                    except Exception as e:
                        logger.warning(f"Failed to queue Airtable task: {str(e)[:100]}")

                # Success!
                logger.info(f"✅ [SUCCESS] Parsed: {property_data.get('titulo', 'Unknown')[:60]}")

                # ACK message
                if message_id:
                    self.ack_url(message_id)

                return True

            elif state == PageState.OFFLINE:
                # Listing expired/removed - remove from processed_urls
                logger.warning(f"⚠️  [OFFLINE] Listing removed: {url[:80]}")

                if self.processed_urls:
                    try:
                        self.processed_urls.remove_url(url)
                        logger.debug(f"   Removed from processed_urls")
                    except Exception as e:
                        logger.debug(f"Error removing URL: {str(e)[:100]}")

                # ACK message (don't reprocess offline listings)
                if message_id:
                    self.ack_url(message_id)

                return True  # Treat as success since we handled it

            elif state == PageState.CAPTCHA:
                # Captcha couldn't be solved - needs retry
                error_msg = "Captcha resolution failed"
                logger.error(f"❌ [CAPTCHA] {error_msg}")

                # ACK and retry or fail
                if message_id:
                    self.ack_url(message_id)
                self.handle_retry_or_fail(url, error_msg, retry_count)

                return False  # Browser restart needed

            else:
                # TIMEOUT or UNKNOWN
                error_msg = f"Navigation failed: {state}"
                logger.error(f"❌ [{state}] {error_msg}")

                # ACK and retry or fail
                if message_id:
                    self.ack_url(message_id)
                self.handle_retry_or_fail(url, error_msg, retry_count)

                return False

        except Exception as e:
            error_msg = f"Exception: {str(e)[:200]}"
            logger.error(f"❌ [EXCEPTION] {error_msg}")

            # ACK and retry or fail
            if message_id:
                self.ack_url(message_id)
            self.handle_retry_or_fail(url, error_msg, retry_count)

            return False


# ============================================================================
# WORKER MAIN LOOP - Persistent Worker Pattern with Redis Stream
# ============================================================================

def worker_main(redis_clients: dict, consumer_name: str = "worker-1"):
    """
    Main persistent worker loop consuming from Redis stream.

    Pattern:
    1. Initialize browser once (persistent)
    2. Consume URLs from Redis stream in loop
    3. Keep browser persistent until:
       - Error occurs (restart browser and retry)
       - Queue is empty (finish gracefully)

    Args:
        redis_clients: Dict with Redis clients for queue and storage
        consumer_name: Unique name for this worker consumer
    """
    logger.info("="*60)
    logger.info("PERSISTENT WORKER STARTED")
    logger.info(f"Consumer: {consumer_name}")
    logger.info("="*60)

    worker = None
    consecutive_errors = 0
    max_consecutive_errors = 3
    empty_queue_checks = 0
    max_empty_checks = 3  # Exit after 3 consecutive empty checks
    urls_processed = 0

    try:
        # Initialize worker with Redis clients
        worker = DfimoveisWorker(redis_clients=redis_clients)

        # Initialize browser (persistent)
        worker.init_browser()
        logger.info("✓ Persistent worker initialized")
        logger.info("")

        # Main processing loop - keep consuming until queue is empty
        while True:
            try:
                # Consume next URL from Redis stream
                message_id, url, retry_count, action = worker.consume_next_url(consumer_name=consumer_name)

                if not url:
                    # Queue is empty (or timeout)
                    empty_queue_checks += 1
                    logger.info(f"📭 Queue empty ({empty_queue_checks}/{max_empty_checks})")

                    if empty_queue_checks >= max_empty_checks:
                        logger.info("Queue empty after multiple checks - exiting")
                        break

                    # Wait a bit before checking again
                    time.sleep(2)
                    continue

                # Reset empty queue counter when we get a URL
                empty_queue_checks = 0

                # Process URL
                success = worker.process_url(url, message_id=message_id, retry_count=retry_count)

                if success:
                    consecutive_errors = 0  # Reset error counter on success
                    urls_processed += 1
                else:
                    # Processing failed - might need browser restart
                    consecutive_errors += 1
                    logger.warning(f"Failed ({consecutive_errors}/{max_consecutive_errors})")

                    if consecutive_errors >= max_consecutive_errors:
                        # Too many errors - restart browser
                        logger.warning(f"⚠️  {max_consecutive_errors} consecutive errors - restarting browser...")

                        if worker.restart_browser():
                            logger.info("✓ Browser restarted successfully")
                            consecutive_errors = 0  # Reset counter after restart
                        else:
                            logger.error("❌ Browser restart failed - exiting")
                            break

                # Small delay between URLs
                time.sleep(1)

            except KeyboardInterrupt:
                logger.info("\n⚠️  Keyboard interrupt - shutting down gracefully...")
                break

            except Exception as e:
                consecutive_errors += 1
                logger.error(f"❌ Error in processing loop: {str(e)[:200]}")
                logger.error(f"   Consecutive errors: {consecutive_errors}/{max_consecutive_errors}")

                if consecutive_errors >= max_consecutive_errors:
                    logger.error("Too many consecutive errors - exiting")
                    break

                # Try to restart browser after error
                try:
                    if worker.restart_browser():
                        logger.info("✓ Browser restarted after error")
                        consecutive_errors = 0
                    else:
                        logger.error("❌ Browser restart failed")
                        break
                except Exception as restart_err:
                    logger.error(f"❌ Error during restart: {str(restart_err)[:100]}")
                    break

        logger.info("="*60)
        logger.info("PERSISTENT WORKER COMPLETE")
        logger.info(f"📊 URLs processed: {urls_processed}")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Fatal worker error: {e}", exc_info=True)

    finally:
        if worker:
            logger.info("Closing browser...")
            worker.close_browser()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    start_time = time.time()

    try:
        # Get configuration from environment variables
        redis_host = os.getenv("REDIS_HOST")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        redis_password = os.getenv("REDIS_PASSWORD", "")
        consumer_name = os.getenv("CONSUMER_NAME", f"worker-{os.getpid()}")

        if not redis_host:
            logger.error("❌ REDIS_HOST environment variable not set")
            logger.error("Cannot proceed without Redis. Exiting.")
            sys.exit(1)

        # Initialize Redis clients
        logger.info("Connecting to Redis...")
        try:
            from redis_client import create_redis_clients

            redis_clients = create_redis_clients(
                site_name=SITE_NAME,
                host=redis_host,
                port=redis_port,
                password=redis_password
            )

            logger.info("✅ Connected to Redis")
            logger.info(f"   Stream: {redis_clients['url_stream'].stream_key}")
            logger.info(f"   Consumer Group: {redis_clients['url_stream'].consumer_group}")
            logger.info(f"   Processed URLs: {len(redis_clients['processed_urls'].get_all_urls()):,}")

            # Check if failed_urls client exists (might be in different db)
            if 'failed_urls' in redis_clients:
                try:
                    failed_count = redis_clients['failed_urls'].get_failed_count()
                    logger.info(f"   Failed URLs: {failed_count}")
                except:
                    pass

        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            logger.error("Cannot proceed without Redis. Exiting.")
            sys.exit(1)

        # Start worker
        worker_main(redis_clients=redis_clients, consumer_name=consumer_name)

        # Calculate elapsed time
        elapsed = time.time() - start_time
        logger.info(f"⏱️  Total time: {elapsed:.2f}s ({elapsed/60:.1f} min)")

    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Close Redis connections
        try:
            if 'redis_clients' in locals():
                for name, client in redis_clients.items():
                    client.close()
                logger.info("✅ Redis connections closed")
        except Exception as e:
            logger.debug(f"Error closing Redis: {e}")
