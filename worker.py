#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OLX Worker - Property Detail Consumer
Processes URLs from queue and extracts property details
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
            redis_clients: Optional dict with Redis clients (not used yet)
        """
        self.sb = None
        self.debug_dir = "debug_urls"
        os.makedirs(self.debug_dir, exist_ok=True)

        # Redis integration will be added later
        logger.debug("DfimoveisWorker instance created (mock queue mode)")

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
    # QUEUE MANAGEMENT - Mock Queue (for testing)
    # ========================================================================
    # Note: Redis integration will be added later

    # ========================================================================
    # WORKFLOW - TODO
    # ========================================================================

    def process_url(self, url: str) -> bool:
        """
        Process a single property URL.

        Workflow:
        1. Navigate to URL
        2. Check page state
        3. If SUCCESS: parse property details
        4. Store in database (log for now)

        Args:
            url: Property URL to process

        Returns:
            True if processed successfully (including offline listings)
        """
        logger.info(f"Processing URL: {url[:80]}")

        try:
            # Navigate and get state
            state = self.goto_url(url)

            if state == PageState.SUCCESS:
                # Parse property details
                property_data = self.parse_property()

                if property_data:
                    # TODO: Store in database (for now, just log)
                    logger.info(f"✅ [SUCCESS] Parsed property: {property_data.get('title', 'Unknown')}")
                    logger.debug(f"   Data: {json.dumps(property_data, indent=2)}")
                    return True
                else:
                    logger.error(f"❌ [PARSE_FAILED] Could not parse property")
                    return False

            elif state == PageState.OFFLINE:
                # Listing expired/removed - this is a valid state
                logger.warning(f"⚠️  [OFFLINE] Listing removed: {url[:80]}")
                # TODO: Mark as offline in DB
                return True  # Treat as success since we handled it

            elif state == PageState.CAPTCHA:
                # Captcha couldn't be solved - need browser restart
                logger.error(f"❌ [CAPTCHA] Could not solve captcha: {url[:80]}")
                return False

            else:
                # TIMEOUT or UNKNOWN
                logger.error(f"❌ [{state}] Navigation failed: {url[:80]}")
                return False

        except Exception as e:
            logger.error(f"❌ [EXCEPTION] Error processing URL: {str(e)[:200]}")
            return False


# ============================================================================
# WORKER MAIN LOOP - Persistent Worker Pattern (Mock Queue)
# ============================================================================

def worker_main(url_queue: list = None):
    """
    Main persistent worker loop with mock queue.

    Pattern:
    1. Initialize browser once (persistent)
    2. Consume URLs from queue in loop
    3. Keep browser persistent until:
       - Error occurs (restart browser and retry)
       - Queue is empty (finish gracefully)

    Args:
        url_queue: List of URLs to process (mock queue for testing)
    """
    logger.info("="*60)
    logger.info("PERSISTENT WORKER STARTED")
    logger.info("="*60)

    # Default test URLs if none provided
    if url_queue is None:
        url_queue = [
            "https://www.dfimoveis.com.br/imovel/casa-4-quartos-venda-jardim-botanico-brasilia-df-condominio-morada-de-deus-1179418",
            "https://www.dfimoveis.com.br/imovel/casa-4-quartos-venda-jardim-botanico-brasilia-df-travessa-ipe-roxo-1253567",
            "https://www.dfimoveis.com.br/imovel/apartamento-2-quartos-venda-asa-sul-brasilia-df-sqs-412-1252084",
            "https://www.dfimoveis.com.br/imovel/loja-0-quartos-venda-noroeste-brasilia-df-clnw-10-11-lote-c-1214670",
            "https://www.dfimoveis.com.br/imovel/apartamento-3-quartos-venda-sudoeste-brasilia-df-sqsw-504-1245327",
            "https://www.dfimoveis.com.br/imovel/lancamento-link-noroeste-334299"
        ]

    logger.info(f"📋 Queue size: {len(url_queue)} URLs")

    worker = None
    consecutive_errors = 0
    max_consecutive_errors = 3

    try:
        # Initialize worker (no Redis clients for now)
        worker = DfimoveisWorker(redis_clients=None)

        # Initialize browser (persistent)
        worker.init_browser()
        logger.info("✓ Persistent worker initialized")

        # Main processing loop - keep consuming until queue is empty
        while url_queue:
            try:
                url = url_queue.pop(0)
                # logger.info(f"📥 Dequeued ({len(url_queue)} remaining): {url[:80]}")

                # Process URL
                success = worker.process_url(url)

                if success:
                    logger.info(f"✅ Completed successfully")
                    consecutive_errors = 0  # Reset error counter on success
                else:
                    # Processing failed - might need browser restart
                    consecutive_errors += 1
                    logger.warning(f"❌ Failed ({consecutive_errors}/{max_consecutive_errors})")

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
        logger.info(f"📊 Processed: {len(url_queue) if url_queue else 'All'} URLs remaining in queue")
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
    try:
        # You can provide custom URLs via command line or use defaults
        # Example: python worker.py "url1" "url2" "url3"

        custom_urls = sys.argv[1:] if len(sys.argv) > 1 else None

        if custom_urls:
            logger.info(f"Using {len(custom_urls)} URLs from command line")
            worker_main(url_queue=custom_urls)
        else:
            logger.info("Using default test URLs")
            worker_main()

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
