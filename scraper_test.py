#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OLX Scraper - Clean Rebuild with Test-Driven Approach
Testing each component before building the next
"""
import time
import re
import logging
import os
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
        self.debug_dir = "debug_screenshots"
        os.makedirs(self.debug_dir, exist_ok=True)
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

    def save_debug_snapshot(self, reason: str) -> str:
        """
        Save screenshot and URL for debugging stopped workflows.

        Args:
            reason: Reason for stopping (e.g., "timeout", "captcha", "unknown")

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

    def check_page_resources(self) -> bool:
        """
        Check if all required page resources are present.

        Verifies all required elements:
        - Listing container
        - At least one listing card
        - Next page button

        Returns:
            True if all resources found, False otherwise
        """
        required_elements = [
            (self.PAGE_LOADED_SELECTOR, "Listing container"),
            (self.LISTING_CARD_SELECTOR, "Listing card"),
            (self.NEXT_PAGE_BUTTON, "Next page button")
        ]

        try:
            missing = []

            for selector, name in required_elements:
                if not self.sb.is_element_present(selector):
                    logger.debug(f"{name} not found: {selector}")
                    missing.append(name)

            if missing:
                logger.debug(f"Missing elements: {', '.join(missing)}")
                return False

            logger.debug("✓ All page resources present")
            return True

        except Exception as e:
            logger.debug(f"Error checking page resources: {str(e)[:100]}")
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
        logger.warning("⚠️  Captcha detected! Trying UC mode auto-solve.")

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

        logger.warning("UC mode timeout - trying fallback methods.")

        # Method 2: gui_click_captcha()
        try:
            logger.info("Trying gui_click_captcha().")
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
            logger.info("Trying solve_captcha().")
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
        Determine current page state after navigation.

        Performs ordered checks:
        1. Base load (body + title)
        2. Captcha (handle if detected)
        3. No results page
        4. Page resources present

        Args:
            base_load_timeout: Timeout for base load check (default 5s)

        Returns:
            PageState constant indicating current state
        """
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

        # 3. Check no results (valid but empty page)
        if self.check_no_results_page():
            logger.info("No results page detected")
            return PageState.NO_RESULTS

        # 4. Check all page resources present (expected success state)
        if self.check_page_resources():
            logger.debug("✓ All page resources present")
            return PageState.SUCCESS

        # Unknown state - page loaded but unexpected content
        logger.warning("Unknown page state (no captcha, no 'no results', no resources)")
        return PageState.UNKNOWN

    def _wait_for_url_change(self, original_url: str, timeout: int = 2) -> bool:
        """
        Actively wait for URL to change from original URL.

        Args:
            original_url: The URL before transition
            timeout: Maximum time to wait (default 2s)

        Returns:
            True if URL changed within timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                current_url = self.sb.get_current_url()
                if current_url != original_url:
                    logger.debug(f"URL changed in {time.time() - start_time:.2f}s")
                    return True
            except:
                pass
            time.sleep(0.1)  # Poll every 100ms

        logger.debug(f"URL didn't change within {timeout}s")
        return False

    def goto_url(self, url: str, max_retries: int = 2) -> str:
        """
        Navigate to URL and determine page state.

        Args:
            url: URL to navigate to
            max_retries: Maximum retry attempts (default 2)

        Returns:
            PageState constant indicating result
        """
        logger.info(f"Navigating to: {url}")

        for attempt in range(max_retries):
            try:
                # Navigate
                self.sb.get(url)
                logger.debug(f"Navigation command sent (attempt {attempt + 1}/{max_retries})")

                # Check page state
                state = self.get_page_state(base_load_timeout=5)

                # If timeout or unknown, retry
                if state in [PageState.TIMEOUT, PageState.UNKNOWN]:
                    if attempt < max_retries - 1:
                        logger.info("Retrying navigation...")
                        time.sleep(1)
                        continue

                # Return state (SUCCESS, NO_RESULTS, CAPTCHA, TIMEOUT, UNKNOWN)
                logger.info(f"✓ Navigation complete: {state}")
                return state

            except Exception as e:
                logger.error(f"Navigation error (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return PageState.TIMEOUT

        return PageState.TIMEOUT

    def goto_next_page(self, max_retries: int = 2) -> str:
        """
        Click next page button and verify page transition.

        Args:
            max_retries: Maximum retry attempts (default 2)

        Returns:
            PageState constant indicating result
        """
        logger.debug("Attempting to go to next page")

        for attempt in range(max_retries):
            try:
                # Store current URL to verify transition
                current_url = self.sb.get_current_url()

                logger.debug("Clicking next page button")
                self.sb.click(self.NEXT_PAGE_BUTTON)

                # Wait for URL to change (with timeout)
                if not self._wait_for_url_change(current_url, timeout=3):
                    logger.warning("URL didn't change after click (3s timeout)")
                    if attempt < max_retries - 1:
                        continue
                    return PageState.UNKNOWN

                # Log new URL
                new_url = self.sb.get_current_url()
                logger.debug(f"Page transitioned: {new_url}")

                # Check new page state
                state = self.get_page_state(base_load_timeout=5)

                # If timeout or unknown, retry
                if state in [PageState.TIMEOUT, PageState.UNKNOWN]:
                    if attempt < max_retries - 1:
                        logger.info("Retrying next page click...")
                        continue

                logger.info(f"✓ Next page navigation complete: {state}")
                return state

            except Exception as e:
                logger.error(f"Next page error (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return PageState.TIMEOUT

        return PageState.TIMEOUT

    # ========================================================================
    # DATA EXTRACTION
    # ========================================================================

    def get_page_number(self) -> int:
        """
        Extract page number from URL using JS.

        URL format: ?o=2 means page 2, no ?o= means page 1

        Returns:
            Current page number (1-based)
        """
        try:
            # Get URL via JS
            url = self.sb.evaluate("return window.location.href")

            # Extract o={page_num} parameter
            match = re.search(r'[?&]o=(\d+)', url)
            page_num = int(match.group(1)) if match else 1

            logger.debug(f"Extracted page number: {page_num}")
            return page_num

        except Exception as e:
            logger.error(f"Error extracting page number: {str(e)[:100]}")
            return 1

    def get_page_data(self) -> list:
        """
        Extract listing URLs with prices using JS to get HTML.

        Uses JS evaluation to get page HTML, then BeautifulSoup to parse.

        Returns:
            List of tuples: [(url, price), ...]
            Price is int or None if not found
        """
        try:
            # Get HTML via JS
            logger.debug("Getting page HTML via JS...")
            page_html = self.sb.evaluate("return document.documentElement.outerHTML")

            # Parse with BeautifulSoup
            soup = BeautifulSoup(page_html, 'lxml')

            urls_with_prices = []
            listing_elements = soup.select(self.LISTING_CARD_SELECTOR)

            logger.debug(f"Found {len(listing_elements)} listing cards")

            for listing in listing_elements:
                # Extract URL
                link_element = listing.select_one('a[data-testid="adcard-link"]')
                if not link_element:
                    continue

                href = link_element.get('href')
                if not href:
                    continue

                # Build and clean URL
                full_url = href if href.startswith('http') else self.BASE_URL.rstrip('/') + href
                clean_url = full_url.split('?')[0]  # Remove query params

                # Extract price
                price = None
                price_h3 = listing.select_one('h3.olx-adcard__price')
                if price_h3:
                    price_text = price_h3.get_text(strip=True)
                    # Remove "R$", dots, spaces
                    price_clean = price_text.replace('R$', '').replace('.', '').replace(' ', '').strip()
                    try:
                        price = int(price_clean)
                    except (ValueError, AttributeError):
                        pass  # Keep price as None

                urls_with_prices.append((clean_url, price))

            logger.debug(f"Extracted {len(urls_with_prices)} listings with URLs and prices")
            return urls_with_prices

        except Exception as e:
            logger.error(f"Error extracting page data: {str(e)[:100]}")
            return []

    def get_total_pages(self) -> int:
        """
        Detect total number of pages from current page.

        Extracts total results count from OLX's results text and calculates pages.
        Website blocks beyond 100 pages, so we respect that limit.

        Returns:
            Total pages (int), or 0 if detection fails
        """
        MAX_PAGES = 100  # Website blocks beyond this
        LISTINGS_PER_PAGE = 50

        logger.info("🔍 Detecting total pages from current page...")

        try:
            # Get HTML via JS
            page_html = self.sb.evaluate("return document.documentElement.outerHTML")
            soup = BeautifulSoup(page_html, 'lxml')

            # Try to find results text element using multiple strategies
            results_text_element = None

            # Strategy 1: Look for data-testid attribute
            results_text_element = soup.select_one('span[data-testid="results-count-text"]')

            # Strategy 2: Find span/p containing "resultados" pattern
            if not results_text_element:
                results_text_element = soup.find(['span', 'p'], string=re.compile(r'de\s+[\d.]+\s+resultados?'))

            if results_text_element:
                results_text = results_text_element.get_text()
                logger.debug(f"Found results text: {results_text}")

                # Extract the total number using regex
                # Pattern matches: "1 - 50 de 4.086 resultados" -> 4.086
                match = re.search(r'de\s+([\d\.]+)\s+resultados?', results_text)

                if match:
                    total_listings_str = match.group(1).replace('.', '').replace(',', '')
                    total_listings = int(total_listings_str)

                    # Calculate pages (ceiling division)
                    expected_pages = (total_listings + LISTINGS_PER_PAGE - 1) // LISTINGS_PER_PAGE

                    # Apply max pages limit
                    total_pages = min(expected_pages, MAX_PAGES)

                    logger.info(f"✅ Detected {total_pages} pages ({total_listings} listings)")
                    return total_pages
                else:
                    logger.warning(f"Could not extract number from results text: {results_text}")
                    return 0
            else:
                logger.warning("Could not find results text element")
                return 0

        except Exception as e:
            logger.error(f"Error detecting total pages: {str(e)[:100]}")
            return 0


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

            # Extract data from initial page (page 1)
            page_num = scraper.get_page_number()
            page_data = scraper.get_page_data()
            logger.info(f"📄 Page {page_num}: Extracted {len(page_data)} listings")

            # Detect total pages from the website
            total_pages = scraper.get_total_pages()

            # Calculate pagination limit: min(detected_pages, max_allowed)
            MAX_TEST_PAGES = 50
            pagination_limit = min(total_pages, MAX_TEST_PAGES) if total_pages > 0 else MAX_TEST_PAGES

            current_page = 1

            logger.info(f"Starting pagination (limit: {pagination_limit} pages)")

            while current_page < pagination_limit:
                logger.info(f"On page {current_page}, navigating to next page...")

                # Navigate to next page
                next_state = scraper.goto_next_page()

                if next_state == PageState.SUCCESS:
                    current_page += 1

                    # Extract data from new page
                    page_num = scraper.get_page_number()
                    page_data = scraper.get_page_data()
                    logger.info(f"📄 Page {page_num}: Extracted {len(page_data)} listings")

                elif next_state == PageState.NO_RESULTS:
                    logger.info(f"Reached end of listings at page {current_page}")
                    break

                elif next_state in [PageState.TIMEOUT, PageState.UNKNOWN]:
                    logger.warning(f"Navigation failed on page {current_page}: {next_state}")
                    scraper.save_debug_snapshot(f"pagination_{next_state}_page{current_page}")
                    break

                elif next_state == PageState.CAPTCHA:
                    logger.error(f"Captcha on page {current_page}")
                    scraper.save_debug_snapshot(f"pagination_captcha_page{current_page}")
                    break

            logger.info(f"Pagination complete: scraped {current_page} of {pagination_limit} pages")

        elif state == PageState.NO_RESULTS:
            logger.info("="*60)
            logger.info("⚠️  NO RESULTS - No listings found")
            logger.info("Stopping task gracefully")
            logger.info("="*60)
            return

        elif state == PageState.CAPTCHA:
            logger.error("="*60)
            logger.error("✗ CAPTCHA FAILED - Couldn't solve captcha")
            scraper.save_debug_snapshot("initial_captcha")
            logger.error("Consider:")
            logger.error("  - Restart browser and retry")
            logger.error("  - Add delay before retry")
            logger.error("  - Check captcha solving methods")
            logger.error("="*60)
            return

        elif state == PageState.TIMEOUT:
            logger.error("="*60)
            logger.error("✗ TIMEOUT - Page failed to load")
            scraper.save_debug_snapshot("initial_timeout")
            logger.error("Consider:")
            logger.error("  - Restart browser")
            logger.error("  - Check network connection")
            logger.error("  - Increase timeout values")
            logger.error("="*60)
            return

        elif state == PageState.UNKNOWN:
            logger.warning("="*60)
            logger.warning("⚠️  UNKNOWN STATE - Unexpected page content")
            scraper.save_debug_snapshot("initial_unknown")
            logger.warning("Page loaded but doesn't match expected patterns")
            logger.warning("="*60)
            return

        logger.info("✓ Task completed successfully")

    except Exception as e:
        logger.error(f"✗ Task failed with exception: {e}")
        if scraper:
            scraper.save_debug_snapshot("exception")

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
