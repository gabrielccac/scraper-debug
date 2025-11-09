#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OLX Scraper - Clean Rebuild with Test-Driven Approach
Testing each component before building the next
"""
import time
import re
import logging
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
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
    """Page state constants for navigation results"""
    SUCCESS = "success"           # Listings found, ready to scrape
    NO_RESULTS = "no_results"     # Valid page but no listings (stop task)
    CAPTCHA = "captcha"           # Captcha couldn't be solved
    TIMEOUT = "timeout"           # Page failed to load
    UNKNOWN = "unknown"           # Loaded but unexpected state


class OlxScraper:
    """
    Clean implementation of OLX property scraper.
    Built incrementally with testing at each step.
    """

    # ========================================================================
    # CONSTANTS - Site Configuration
    # ========================================================================

    BASE_URL = "https://www.olx.com.br/"
    SITE_NAME = "olx"

    # Selectors
    PAGE_LOADED_SELECTOR = 'div.AdListing_adListContainer__ALQla'
    LISTING_CARD_SELECTOR = 'section.olx-adcard'
    NEXT_PAGE_BUTTON = '//a[text()="Próxima página"]'
    NO_RESULTS_MESSAGE = 'Ops! Nenhum anúncio foi encontrado.'

    # Captcha detection patterns
    CAPTCHA_TITLE_KEYWORDS = ["Um momento", "Just a moment"]
    CAPTCHA_TEXT_KEYWORDS = ["Confirme que você é humano", "cf-challenge"]

    # Settings
    LOAD_TIMEOUT = 15
    BROWSER_LOCALE = "pt-br"

    # ========================================================================
    # INITIALIZATION
    # ========================================================================

    def __init__(self):
        """Initialize scraper instance."""
        self.sb = None
        logger.debug("OlxScraper instance created")

    # ========================================================================
    # METHODS - To be implemented incrementally
    # ========================================================================

    # TODO: Browser management
    def init_browser(self, max_retries: int = 3):
        """
        Initialize SeleniumBase browser instance with retry logic.

        Args:
            headless: Run in headless mode
            max_retries: Maximum number of retry attempts (default 3)
        """
        for attempt in range(max_retries):
            try:
                self.sb = sb_cdp.Chrome(
                    uc=True,
                    headless=False,
                    locale=self.BROWSER_LOCALE,
                    window_size="1920,1080",
                    ad_block=True,
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
                    # Exponential backoff: 1s, 2s, 4s
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

    # ========================================================================
    # NAVIGATION
    # ========================================================================

    def _wait_for_base_load(self, timeout: int = 10) -> bool:
        """
        Ensure basic page load before checking page state.

        Verifies:
        - Body element exists
        - Not on about:blank
        - Page title exists and not empty

        Args:
            timeout: Time to wait for base load

        Returns:
            True if page has basic content loaded
        """
        try:
            # Wait for body tag
            self.sb.wait_for_element("body", timeout=timeout)

            # Check we're not on about:blank
            current_url = self.sb.get_current_url()
            if "about:blank" in current_url:
                logger.debug("Still on about:blank")
                return False

            # Check title exists and not empty
            title = self.sb.get_title()
            if not title or len(title) == 0:
                logger.debug("Page title is empty")
                return False

            logger.debug(f"Base page loaded: {title[:50]}")
            return True

        except Exception as e:
            logger.debug(f"Base load check failed: {str(e)[:100]}")
            return False

    def check_captcha_page(self) -> bool:
        """
        Check if current page is a captcha challenge.

        Returns:
            True if captcha detected
        """
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

    def check_no_results_page(self) -> bool:
        """
        Check if page shows "no results" message.

        Returns:
            True if no results message detected
        """
        try:
            # Check for no results message in page text
            return self.sb.is_text_visible(self.NO_RESULTS_MESSAGE, selector="body")
        except Exception as e:
            logger.debug(f"Error checking no results: {str(e)[:100]}")
            return False

    def check_listings_present(self) -> bool:
        """
        Check if listing elements are present on page.

        Verifies both:
        - Listing container exists
        - At least one listing card exists

        Returns:
            True if listings found
        """
        try:
            # Check for listing container
            if not self.sb.is_element_present(self.PAGE_LOADED_SELECTOR):
                logger.debug(f"Listing container not found: {self.PAGE_LOADED_SELECTOR}")
                return False

            # Check for at least one listing card
            if not self.sb.is_element_present(self.LISTING_CARD_SELECTOR):
                logger.debug(f"No listing cards found: {self.LISTING_CARD_SELECTOR}")
                return False

            return True

        except Exception as e:
            logger.debug(f"Error checking listings: {str(e)[:100]}")
            return False

    def handle_captcha(self, max_wait: int = 60) -> bool:
        """
        Handle captcha with multiple strategies.

        Strategy:
        1. Wait for UC mode auto-solve (60s)
        2. Try gui_click_captcha()
        3. Try solve_captcha()

        Args:
            max_wait: Maximum time to wait for UC mode

        Returns:
            True if captcha cleared
        """
        logger.warning("⚠️  Captcha detected! Trying UC mode auto-solve...")

        # Method 1: UC mode auto-solve (passive waiting)
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                title = self.sb.get_title()
                # Check if captcha keywords still in title
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

    def goto_url(self, url: str, max_retries: int = 3) -> str:
        """
        Navigate to URL and determine page state.

        Returns page state (does not make decisions):
        - PageState.SUCCESS: Listings found
        - PageState.NO_RESULTS: Valid page but no listings
        - PageState.CAPTCHA: Captcha couldn't be solved
        - PageState.TIMEOUT: Page failed to load
        - PageState.UNKNOWN: Unexpected state

        Args:
            url: URL to navigate to
            max_retries: Maximum retry attempts

        Returns:
            PageState constant indicating result
        """
        logger.info(f"Navigating to: {url}")

        for attempt in range(max_retries):
            try:
                # Navigate
                self.sb.get(url)
                logger.debug(f"Navigation command sent (attempt {attempt + 1}/{max_retries})")

                # 1. Wait for basic page load
                if not self._wait_for_base_load(timeout=10):
                    logger.warning("Base page load timeout")
                    if attempt < max_retries - 1:
                        continue
                    return PageState.TIMEOUT

                # 2. Check captcha (FIRST - blocks everything else)
                if self.check_captcha_page():
                    logger.warning("Captcha detected")
                    if self.handle_captcha():
                        logger.info("Captcha solved, re-checking page state")
                        # Don't increment attempt, re-check page
                        continue
                    else:
                        return PageState.CAPTCHA

                # 3. Check no results (valid but empty page)
                if self.check_no_results_page():
                    logger.info("No results page detected")
                    return PageState.NO_RESULTS

                # 4. Check listings present (expected success state)
                if self.check_listings_present():
                    logger.info("✓ Listings found on page")
                    return PageState.SUCCESS

                # Unknown state - page loaded but unexpected content
                logger.warning("Unknown page state (no listings, no captcha, no 'no results')")
                if attempt < max_retries - 1:
                    logger.info("Retrying...")
                    time.sleep(2)  # Brief delay before retry
                    continue

                return PageState.UNKNOWN

            except Exception as e:
                logger.error(f"Navigation error (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}")
                if attempt < max_retries - 1:
                    continue
                return PageState.TIMEOUT

        return PageState.TIMEOUT

    # TODO: Data extraction
    # - get_page_data()
    # - get_total_pages()

    # TODO: Pagination
    # - click_next_page()
    # - get_page_number()
    # - get_page_url()


# ============================================================================
# SCRAPE TASK - Main workflow function
# ============================================================================

def scrape_task(url: str):
    """
    Orchestrate scraping workflow based on page state.

    Handles different page states:
    - SUCCESS: Continue with scraping
    - NO_RESULTS: Stop gracefully (no listings)
    - CAPTCHA: Captcha couldn't be solved
    - TIMEOUT: Page failed to load
    - UNKNOWN: Unexpected page state

    Args:
        url: URL to test with
    """
    logger.info("="*60)
    logger.info("STARTING SCRAPE TASK")
    logger.info(f"Test URL: {url}")
    logger.info("="*60)

    scraper = None

    try:
        # Initialize scraper
        scraper = OlxScraper()
        logger.info("✓ Scraper instance created")

        # Initialize browser
        scraper.init_browser()
        logger.info("✓ Browser initialized")

        # Navigate and get page state
        state = scraper.goto_url(url)
        logger.info(f"Page state: {state}")

        # Orchestrate actions based on state
        if state == PageState.SUCCESS:
            logger.info("="*60)
            logger.info("✓ SUCCESS - Ready to scrape")
            logger.info("="*60)
            # TODO: Add scraping logic here
            # - Extract data
            # - Navigate to next page
            # - Loop through pages

        elif state == PageState.NO_RESULTS:
            logger.info("="*60)
            logger.info("⚠️  NO RESULTS - No listings found")
            logger.info("Stopping task gracefully")
            logger.info("="*60)
            return

        elif state == PageState.CAPTCHA:
            logger.error("="*60)
            logger.error("✗ CAPTCHA FAILED - Couldn't solve captcha")
            logger.error("Consider:")
            logger.error("  - Restart browser and retry")
            logger.error("  - Add delay before retry")
            logger.error("  - Check captcha solving methods")
            logger.error("="*60)
            return

        elif state == PageState.TIMEOUT:
            logger.error("="*60)
            logger.error("✗ TIMEOUT - Page failed to load")
            logger.error("Consider:")
            logger.error("  - Restart browser")
            logger.error("  - Check network connection")
            logger.error("  - Increase timeout values")
            logger.error("="*60)
            return

        elif state == PageState.UNKNOWN:
            logger.warning("="*60)
            logger.warning("⚠️  UNKNOWN STATE - Unexpected page content")
            logger.warning("Page loaded but doesn't match expected patterns")
            logger.warning("="*60)
            return

        logger.info("✓ Task completed successfully")

    except Exception as e:
        logger.error(f"✗ Task failed with exception: {e}")

    finally:
        # Always close browser
        if scraper:
            scraper.close_browser()
            logger.info("✓ Browser closed")


# ============================================================================
# MANUAL TESTS - Run each test as we build methods
# ============================================================================

def test_browser_init():
    """Test 1: Browser initialization"""
    print("\n" + "="*60)
    print("TEST 1: Browser Initialization")
    print("="*60)

    scraper = OlxScraper()
    # TODO: Implement init_browser() first, then uncomment:
    # scraper.init_browser(headless=False)
    # print("✓ Browser initialized successfully")
    # scraper.close_browser()
    # print("✓ Browser closed successfully")

    print("⏸️  Test pending - implement init_browser() first")


def test_navigation():
    """Test 2: Navigate to OLX page and verify load"""
    print("\n" + "="*60)
    print("TEST 2: Navigation & Page Load")
    print("="*60)

    # TODO: Implement after browser init works
    print("⏸️  Test pending - implement navigate() and verify_page_loaded()")


def test_extract_data():
    """Test 3: Extract URLs and prices from a page"""
    print("\n" + "="*60)
    print("TEST 3: Data Extraction")
    print("="*60)

    # TODO: Implement after navigation works
    print("⏸️  Test pending - implement extract_page_data()")


def test_pagination():
    """Test 4: Click next page and verify navigation"""
    print("\n" + "="*60)
    print("TEST 4: Pagination")
    print("="*60)

    # TODO: Implement after data extraction works
    print("⏸️  Test pending - implement click_next_page()")


def test_full_task():
    """Test 5: Complete mini-task (scrape 3 pages)"""
    print("\n" + "="*60)
    print("TEST 5: Full Task (3 pages)")
    print("="*60)

    # TODO: Implement after all components work
    print("⏸️  Test pending - implement full workflow")


# ============================================================================
# MAIN - Run tests
# ============================================================================

if __name__ == "__main__":
    print("\n🧪 OLX SCRAPER - INCREMENTAL TESTING")
    print("="*60)

    # Test URL for development
    TEST_URL = "https://www.olx.com.br/imoveis/venda/casas/estado-df/distrito-federal-e-regiao/brasilia/ra-xvi---lago-sul"

    # Run scrape task
    scrape_task(TEST_URL)

    print("\n" + "="*60)
    print("Testing session complete")
    print("="*60 + "\n")
