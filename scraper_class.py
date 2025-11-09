#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OLX Scraper - Production Class with Redis Integration
Rebuilt with state-based navigation, retry logic, and DB integration
"""
import time
import re
import json
import logging
import os
from bs4 import BeautifulSoup
from seleniumbase import sb_cdp

logger = logging.getLogger(__name__)


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
    Production scraper for olx.com.br property listings.

    Features:
    - State-based navigation with ordered checks
    - Task-level retry mechanism with browser restart
    - Page validation and mismatch recovery
    - JS-based HTML extraction
    - Redis integration for data persistence
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

    def __init__(self, redis_clients: dict = None):
        """
        Initialize scraper with Redis clients for data persistence.

        Args:
            redis_clients: Dict with 'scrape_session', 'processed_urls',
                          'url_stream', 'airtable_tasks' clients
                          If None, runs in test mode without DB integration
        """
        # Redis clients (optional for testing)
        self.redis_clients = redis_clients
        if redis_clients:
            self.scrape_session = redis_clients['scrape_session']
            self.processed_urls = redis_clients['processed_urls']
            self.url_stream = redis_clients['url_stream']
            self.airtable_tasks = redis_clients['airtable_tasks']
            self.site_name = redis_clients['scrape_session'].site_name
        else:
            self.scrape_session = None
            self.processed_urls = None
            self.url_stream = None
            self.airtable_tasks = None
            self.site_name = self.SITE_NAME

        self.sb = None
        self.debug_dir = os.getenv('FAILED_SCREENSHOTS_DIR', 'debug_screenshots')
        os.makedirs(self.debug_dir, exist_ok=True)
        logger.debug("OlxScraper instance created")

    # ========================================================================
    # BROWSER MANAGEMENT
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
    # NAVIGATION & STATE DETECTION
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

    # ========================================================================
    # REDIS STORAGE (Production Only)
    # ========================================================================

    def store_urls_batch(self, url_price_pairs: list, metadata: dict = None) -> dict:
        """
        Store URLs to Redis with deduplication and price change detection.

        Deduplication logic:
        1. Check scrape_session (within-run deduplication)
        2. Check processed_urls (historical deduplication)
        3. New URL → publish to stream
        4. Same price → skip (deduplicate)
        5. Price change → update processed_urls + Airtable task

        Args:
            url_price_pairs: List of (url, price) tuples
            metadata: Optional metadata dict (unused, for compatibility)

        Returns:
            Dict with stats: {'new': int, 'price_changes': int, 'duplicates': int}
        """
        if not self.redis_clients:
            logger.debug("Redis not configured, skipping storage")
            return {'new': 0, 'price_changes': 0, 'duplicates': 0}

        if not url_price_pairs:
            return {'new': 0, 'price_changes': 0, 'duplicates': 0}

        # Convert None prices to 0 (Redis doesn't accept None values)
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

        stats = {'new': 0, 'price_changes': 0, 'duplicates': 0}
        urls_to_publish = []
        processed_updates = {}

        # ✅ BATCH 1: Add to scrape_session with HSETNX (deduplication at source)
        url_price_dict = {url: price for url, price in normalized_pairs}
        hsetnx_results = self.scrape_session.add_urls_if_new_batch(url_price_dict)

        # ✅ BATCH 2: Get existing data only for non-duplicate URLs
        urls_to_check = [url for url, _ in normalized_pairs if hsetnx_results[url]]
        existing_data = self.processed_urls.get_urls_batch(urls_to_check) if urls_to_check else {}

        # Process in batch
        for url, current_price in normalized_pairs:
            # Check if URL was newly added to scrape_session
            is_new_in_session = hsetnx_results[url]

            if not is_new_in_session:
                # URL already in scrape_session from previous page in this run
                stats['duplicates'] += 1
                continue  # Skip entirely - don't check processed_urls, don't publish to stream

            # URL is new in scrape_session, check processed_urls for historical data
            historical_data = existing_data.get(url)

            if historical_data:
                # URL exists in processed_urls (worker already scraped it)
                historical_price = historical_data.get('price')

                if historical_price == current_price:
                    # DUPLICATE: same price, no changes needed
                    stats['duplicates'] += 1
                    continue  # Skip: no stream publish, no processed_urls update
                else:
                    # PRICE CHANGE: update price in processed_urls only
                    # (worker already has full details, just update the price)
                    stats['price_changes'] += 1

                    # Update only the price, preserve all other worker data
                    processed_updates[url] = {
                        **historical_data,  # Keep all existing worker data
                        'price': current_price  # Update only price
                    }
            else:
                # NEW URL: publish to stream for worker to add to processed_urls
                # Scraper does NOT add to processed_urls for new URLs
                stats['new'] += 1
                urls_to_publish.append((url, 'new'))
                # Note: No processed_updates for NEW urls - worker will add the full record

        # ✅ BATCH 3: Publish to stream using pipeline (single round-trip)
        if urls_to_publish:
            self.url_stream.publish_urls_batch(urls_to_publish)

        # ✅ BATCH 4: Update processed_urls ONLY for price changes (not new URLs)
        if processed_updates:
            serialized_updates = {url: json.dumps(data) for url, data in processed_updates.items()}
            self.processed_urls.client.hset(
                self.processed_urls.processed_key,
                mapping=serialized_updates
            )

        # ✅ BATCH 5: Queue Airtable tasks for price changes
        if processed_updates:
            for url, data in processed_updates.items():
                self.airtable_tasks.publish_task(
                    site=self.site_name,
                    action='update',
                    url=url,
                    fields={'price': data['price']}
                )

        logger.info(f"📊 Batch: {len(normalized_pairs)} URLs → "
                f"🆕{stats['new']} 💰{stats['price_changes']} 🔄{stats['duplicates']}")

        return stats

    # ========================================================================
    # TASK HELPERS
    # ========================================================================

    def get_page_url(self, task: dict, page_num: int) -> str:
        """
        Construct URL for a specific page number from task metadata.

        Args:
            task: Dict with 'prop_type', 'transaction_type', 'location'
            page_num: Page number (1-based)

        Returns:
            Full URL for the page

        Example:
            task = {
                'prop_type': 'casas',
                'transaction_type': 'venda',
                'location': '/distrito-federal-e-regiao/brasilia'
            }
            get_page_url(task, 2) →
            "https://www.olx.com.br/imoveis/venda/casas/estado-df/distrito-federal-e-regiao/brasilia?o=2"
        """
        prop_type = task['prop_type']
        transaction_type = task['transaction_type']
        location = task.get('location', '')  # Location can be empty string for state-level

        # Build base URL following OLX pattern
        if location:
            base_url = f"https://www.olx.com.br/imoveis/{transaction_type}/{prop_type}/estado-df{location}"
        else:
            base_url = f"https://www.olx.com.br/imoveis/{transaction_type}/{prop_type}/estado-df"

        # Add page parameter if not first page
        if page_num > 1:
            base_url += f"?o={page_num}"

        return base_url


# ============================================================================
# PRODUCTION WORKFLOW - Task-Based Scraping with Retry
# ============================================================================

def scrape_task_with_retry(task: dict, redis_clients: dict, max_pages: int = 100):
    """
    Production workflow: scrape task with retry logic and Redis integration.

    Args:
        task: Task dict with 'prop_type', 'transaction_type', 'location', 'task_id'
        redis_clients: Dict with Redis client instances
        max_pages: Maximum pages to scrape (default 100, respects site limit)

    Returns:
        Dict with stats: {'pages_scraped': int, 'urls_found': int, 'status': str}
    """
    logger.info("="*60)
    logger.info("STARTING SCRAPE TASK (PRODUCTION)")
    logger.info(f"Task: {task.get('task_id', 'unknown')}")
    logger.info(f"Type: {task['transaction_type']} - {task['prop_type']}")
    logger.info(f"Location: {task.get('location', 'state-level')}")
    logger.info("="*60)

    MAX_RETRIES = 3
    MAX_PAGES = max_pages

    # Task stats
    total_stats = {
        'pages_scraped': 0,
        'urls_found': 0,
        'new_urls': 0,
        'price_changes': 0,
        'duplicates': 0,
        'status': 'unknown'
    }

    # Persistent state across retries
    pagination_limit = MAX_PAGES  # Will be updated after first successful page 1
    start_page = 1  # Which page to start from (updated on retry)

    for attempt in range(MAX_RETRIES):
        scraper = None

        try:
            if attempt > 0:
                logger.info("="*60)
                logger.info(f"🔄 RETRY ATTEMPT {attempt + 1}/{MAX_RETRIES}")
                logger.info(f"Resuming from page {start_page}")
                logger.info("="*60)
                time.sleep(2)  # Brief delay before retry

            # Initialize scraper with Redis clients
            scraper = OlxScraper(redis_clients=redis_clients)
            logger.info("✓ Scraper instance created")

            # Initialize browser
            scraper.init_browser()
            logger.info("✓ Browser initialized")

            # Construct URL for starting page
            start_url = scraper.get_page_url(task, start_page)

            # Navigate and get page state
            state = scraper.goto_url(start_url)
            logger.info(f"Page state: {state}")

            # Handle initial navigation result
            if state == PageState.NO_RESULTS:
                # Graceful stop - don't retry
                logger.info("="*60)
                logger.info("⚠️  NO RESULTS - No listings found")
                logger.info("Stopping task gracefully")
                logger.info("="*60)
                total_stats['status'] = 'no_results'
                return total_stats

            elif state in [PageState.CAPTCHA, PageState.TIMEOUT, PageState.UNKNOWN]:
                # Error - will retry
                logger.error(f"❌ Initial navigation failed: {state}")
                scraper.save_debug_snapshot(f"initial_{state}_attempt{attempt + 1}")
                continue  # Next retry attempt

            elif state != PageState.SUCCESS:
                # Unexpected state
                logger.error(f"❌ Unexpected initial state: {state}")
                continue  # Next retry attempt

            # SUCCESS - proceed with scraping
            logger.info("="*60)
            logger.info("✓ SUCCESS - Ready to scrape")
            logger.info("="*60)

            # Extract data from starting page
            page_num = scraper.get_page_number()
            page_data = scraper.get_page_data()
            logger.info(f"📄 Page {page_num}: Extracted {len(page_data)} listings")

            # Store to Redis
            batch_stats = scraper.store_urls_batch(page_data)
            total_stats['new_urls'] += batch_stats['new']
            total_stats['price_changes'] += batch_stats['price_changes']
            total_stats['duplicates'] += batch_stats['duplicates']
            total_stats['urls_found'] += len(page_data)
            total_stats['pages_scraped'] += 1

            # Detect total pages (only on first attempt from page 1)
            if attempt == 0 and start_page == 1:
                total_pages = scraper.get_total_pages()
                pagination_limit = min(total_pages, MAX_PAGES) if total_pages > 0 else MAX_PAGES
                logger.info(f"Pagination limit set to {pagination_limit} pages")

            current_page = start_page

            logger.info(f"Starting pagination (limit: {pagination_limit} pages)")

            # Track if we should retry (error occurred)
            should_retry = False

            while current_page < pagination_limit:
                logger.info(f"On page {current_page}, navigating to next page...")

                # Navigate to next page
                next_state = scraper.goto_next_page()

                if next_state == PageState.SUCCESS:
                    # Extract actual page number from URL
                    page_num = scraper.get_page_number()
                    expected_page = current_page + 1

                    # Verify we landed on the expected page
                    if page_num != expected_page:
                        logger.warning(f"⚠️  Page mismatch! Expected page {expected_page}, but landed on page {page_num}")
                        logger.info(f"Attempting to navigate directly to page {expected_page}...")

                        # Construct URL for expected page
                        target_url = scraper.get_page_url(task, expected_page)

                        # Try to navigate directly to the correct page
                        recovery_state = scraper.goto_url(target_url)

                        if recovery_state != PageState.SUCCESS:
                            logger.error(f"❌ Failed to recover: could not navigate to page {expected_page}")
                            scraper.save_debug_snapshot(f"recovery_failed_page{expected_page}_attempt{attempt + 1}")
                            # Set retry from this page
                            start_page = expected_page
                            should_retry = True
                            break

                        logger.info(f"✓ Recovered: successfully navigated to page {expected_page}")
                        page_num = expected_page  # Update to expected page

                    # Update current page and extract data
                    current_page = page_num
                    page_data = scraper.get_page_data()
                    logger.info(f"📄 Page {page_num}: Extracted {len(page_data)} listings")

                    # Store to Redis
                    batch_stats = scraper.store_urls_batch(page_data)
                    total_stats['new_urls'] += batch_stats['new']
                    total_stats['price_changes'] += batch_stats['price_changes']
                    total_stats['duplicates'] += batch_stats['duplicates']
                    total_stats['urls_found'] += len(page_data)
                    total_stats['pages_scraped'] += 1

                elif next_state == PageState.NO_RESULTS:
                    # Graceful stop - don't retry
                    logger.info(f"Reached end of listings at page {current_page}")
                    break

                elif next_state in [PageState.TIMEOUT, PageState.UNKNOWN]:
                    # Error - will retry from this page
                    logger.warning(f"Navigation failed on page {current_page}: {next_state}")
                    scraper.save_debug_snapshot(f"pagination_{next_state}_page{current_page}_attempt{attempt + 1}")
                    start_page = current_page + 1  # Retry from next page
                    should_retry = True
                    break

                elif next_state == PageState.CAPTCHA:
                    # Error - will retry from this page
                    logger.error(f"Captcha on page {current_page}")
                    scraper.save_debug_snapshot(f"pagination_captcha_page{current_page}_attempt{attempt + 1}")
                    start_page = current_page + 1  # Retry from next page
                    should_retry = True
                    break

            # Check if we should retry or if pagination completed
            if should_retry:
                logger.warning(f"Error occurred, will retry from page {start_page}")
                continue  # Next retry attempt

            # Pagination completed successfully
            logger.info(f"✅ Pagination complete: scraped {total_stats['pages_scraped']} pages")
            logger.info(f"📊 Stats: {total_stats['new_urls']} new, "
                       f"{total_stats['price_changes']} price changes, "
                       f"{total_stats['duplicates']} duplicates")
            total_stats['status'] = 'success'
            return total_stats  # Success - exit retry loop

        except Exception as e:
            # Exception during workflow - will retry
            logger.error(f"❌ Exception during scraping: {str(e)[:200]}")
            if scraper:
                scraper.save_debug_snapshot(f"exception_attempt{attempt + 1}")

            # Set retry from current page if known
            if 'current_page' in locals():
                start_page = current_page
            logger.warning(f"Will retry from page {start_page}")
            continue  # Next retry attempt

        finally:
            # Always close browser after each attempt
            if scraper:
                scraper.close_browser()
                logger.debug("Browser closed")

    # All retries exhausted
    logger.error("="*60)
    logger.error(f"❌ TASK FAILED after {MAX_RETRIES} attempts")
    logger.error(f"Last attempted page: {start_page}")
    logger.error(f"Pages scraped before failure: {total_stats['pages_scraped']}")
    logger.error("="*60)
    total_stats['status'] = 'failed'
    return total_stats
