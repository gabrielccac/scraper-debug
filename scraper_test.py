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

    # TODO: Navigation
    # - goto_url()
    # - check_page_loaded()

    # TODO: Captcha handling
    # - check_captcha_page()
    # - handle_captcha()

    # TODO: Data extraction
    # - get_page_data()
    # - get_total_pages()

    # TODO: Pagination
    # - click_next_page()
    # - get_page_number()
    # - get_page_url()

    # TODO: Utilities
    # - check_no_results_page()


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

    # Run tests one by one as we build
    test_browser_init()
    test_navigation()
    test_extract_data()
    test_pagination()
    test_full_task()

    print("\n" + "="*60)
    print("Testing session complete")
    print("="*60 + "\n")
